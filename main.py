import os
import time
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL", "")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"

# --- ЛОГИРОВАНИЕ ---
def send_log(msg):
    if not GAS_WEBAPP_URL: return
    try:
        requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "LOG", "msg": str(msg)[:5000]}, timeout=5)
    except: pass

def send_data(sheet, rows):
    if not GAS_WEBAPP_URL: return
    try:
        requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "DATA", "sheetName": sheet, "rows": rows}, timeout=30)
    except: pass

# --- ЗАГОЛОВКИ ---
def get_headers(cid, key):
    # Жесткая очистка от пробелов и переносов
    c_clean = str(cid).strip().replace("\n", "").replace("\r", "")
    k_clean = str(key).strip().replace("\n", "").replace("\r", "")
    return {
        "Client-Id": c_clean, 
        "Api-Key": k_clean,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; OzonLoader/1.0)",
        "Accept": "application/json"
    }

# --- ПОИСК БРЕНДА ---
def get_brand(attrs):
    for a in attrs:
        if a.get("attribute_id") in [85, 31]: # 85 - Brand
            vals = a.get("values", [])
            if vals: return vals[0].get("value", "")
    return ""

# --- OZON CARDS (Товары) ---
def fetch_cards(cid, key):
    items = []
    # Используем v2 product list
    url_list = "https://api-seller.ozon.ru/v2/product/list"
    url_info = "https://api-seller.ozon.ru/v2/product/info/list"
    last_id = ""
    
    headers = get_headers(cid, key)

    while True:
        try:
            # Упрощенный payload, иногда visibility:ALL вызывает проблемы
            payload = { 
                "filter": {}, 
                "limit": 100 
            }
            if last_id: payload["last_id"] = last_id
            
            # ЗАПРОС СПИСКА
            r = requests.post(url_list, headers=headers, json=payload)
            
            # --- ДИАГНОСТИКА ОШИБКИ ---
            if r.status_code != 200:
                # Отправляем в лог ТЕКСТ ошибки, чтобы понять причину
                send_log(f"OZ List ERR {r.status_code}: {r.text}")
                break
            
            data = r.json().get("result", {}).get("items", [])
            if not data: 
                send_log("OZ List: No items returned (Empty)")
                break
            
            # ЗАПРОС ДЕТАЛЕЙ
            ids = [i.get("product_id") for i in data]
            r_info = requests.post(url_info, headers=headers, json={"product_id": ids})
            
            info_map = {}
            if r_info.status_code == 200:
                for i in r_info.json().get("result", {}).get("items", []):
                    info_map[i.get("id")] = i
            else:
                send_log(f"OZ Info ERR {r_info.status_code}: {r_info.text}")

            # СБОРКА
            for item_base in data:
                pid = item_base.get("product_id")
                full = info_map.get(pid, {})
                
                # Поля
                ozon_id = str(pid)
                offer_id = full.get("offer_id") or item_base.get("offer_id") or "-"
                name = full.get("name") or "Товар"
                cat = str(full.get("category_id", ""))
                brand = get_brand(full.get("attributes", []))
                
                # Фото
                primary = full.get("primary_image") or ""
                if not primary and full.get("images"): primary = full["images"][0]

                # Цены
                def gp(d, k): return float(d.get(k) or 0)
                po = full.get("price", {})
                
                p_old = gp(full, "old_price") or gp(po, "old_price")
                p_sell = gp(full, "price") or gp(po, "price") # Цена продавца
                p_mkt = gp(full, "marketing_price") or gp(po, "marketing_price") # Цена для покупателя
                
                if p_old == 0: p_old = p_sell
                if p_mkt == 0: p_mkt = p_sell
                
                # [Фото, Арт.OZ, Арт.Наш, Бренд, Категория, Название, Ц.База, Ц.Прод, Ц.Покуп, Ц.Карта]
                items.append([primary, ozon_id, offer_id, brand, cat, name, p_old, p_sell, p_mkt, p_mkt])
            
            last_id = data[-1].get("product_id")
            if len(data) < 100: break
            
        except Exception as e:
            send_log(f"OZ Cards Exception: {str(e)}")
            break
            
    return items

# --- OZON STOCK (Остатки) ---
def fetch_stocks(cid, key):
    items = []
    # v2 Stock (FBO)
    url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    
    try:
        r = requests.post(url, headers=get_headers(cid, key), json={"limit": 1000, "offset": 0})
        if r.status_code != 200:
            send_log(f"OZ Stock ERR {r.status_code}: {r.text}")
            return []
            
        rows = r.json().get("result", {}).get("rows", [])
        for row in rows:
            sku = str(row.get("sku", ""))
            offer_id = row.get("item_code") or sku
            
            for wh in row.get("warehouses", []):
                qty = wh.get("item_cnt", 0)
                wh_name = wh.get("warehouse_name", "Склад")
                if qty > 0:
                    # [Склад, Артикул, Остаток]
                    items.append([wh_name, offer_id, qty])
    except Exception as e:
        send_log(f"OZ Stock Exception: {str(e)}")
    return items

# --- OZON SALES (Продажи) ---
def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
    
    headers = get_headers(cid, key)
    
    while True:
        try:
            payload = {
                "filter": {
                    "since": f"{d_from}T00:00:00Z",
                    "to": f"{d_to}T23:59:59Z"
                },
                "limit": 1000,
                "page": page,
                "with": {"analytics_data": True, "financial_data": True}
            }
            
            r = requests.post(url, headers=headers, json=payload)
            if r.status_code != 200:
                send_log(f"OZ Sales ERR {r.status_code}: {r.text}")
                break
                
            res = r.json().get("result", [])
            if not res: break
            
            for p in res:
                created = p.get("created_at", "")[:10]
                status = str(p.get("status", "")).lower()
                typ = "Отмена" if "cancelled" in status else "Продажа"
                
                an = p.get("analytics_data") or {}
                wh_from = an.get("warehouse_name", "FBO")
                wh_to = an.get("region", "RU")
                
                fin = p.get("financial_data") or {}
                fin_prods = {x.get('product_id'): x for x in fin.get('products', [])}
                
                for prod in p.get("products", []):
                    sku = prod.get("sku")
                    oid = prod.get("offer_id")
                    
                    # Цена продажи (сколько заплатил клиент)
                    fp = fin_prods.get(sku, {})
                    price = float(fp.get('client_price') or prod.get('price') or 0)
                    
                    # [Дата, Тип, Арт.Наш, Арт.OZ, Кол, Цена, Склад Отгр., Склад Дост.]
                    items.append([created, typ, oid, str(sku), 1, price, wh_from, wh_to])
            
            if len(res) < 1000: break
            page += 1
            time.sleep(0.2)
            
        except Exception as e:
            send_log(f"OZ Sales Exception: {str(e)}")
            break
    return items

@app.route("/")
def health(): return "Ozon v106 Diag OK", 200

@app.route("/sync", methods=['POST'])
def sync():
    try:
        data = request.json
        cid = data.get("clientId")
        key = data.get("apiKey")
        mode = data.get("mode")
        
        rows = []
        target = ""
        
        if mode == "CARDS":
            rows = fetch_cards(cid, key)
            target = "OZ_CARDS_PY"
        elif mode == "STOCK":
            rows = fetch_stocks(cid, key)
            target = "OZ_STOCK_PY"
        elif mode == "SALES":
            rows = fetch_sales(cid, key, data.get("dateFrom"), data.get("dateTo"))
            target = "OZ_SALES_PY"
        elif mode == "FUNNEL":
             return jsonify({"status": "empty"}), 200
            
        if rows:
            send_data(target, rows)
            return jsonify({"status": "ok", "count": len(rows)}), 200
        else:
            return jsonify({"status": "empty"}), 200
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)