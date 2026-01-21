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
        requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "LOG", "msg": str(msg)[:2000]}, timeout=5)
    except: pass

def send_data(sheet, rows):
    if not GAS_WEBAPP_URL: return
    try:
        requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "DATA", "sheetName": sheet, "rows": rows}, timeout=45)
    except: pass

# --- ЗАГОЛОВКИ ---
def get_headers(cid, key):
    return {
        "Client-Id": str(cid).strip(), 
        "Api-Key": str(key).strip(),
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; Google-Apps-Script)",
        "Accept": "application/json"
    }

# --- ПОИСК БРЕНДА ---
def extract_brand(attributes):
    # Ищем атрибут 85 (Бренд)
    for a in attributes:
        if a.get("attribute_id") == 85:
            vals = a.get("values", [])
            if vals: return vals[0].get("value", "")
    return ""

# --- OZON CARDS (ТОВАРЫ) v3/v4 ---
def fetch_cards(cid, key):
    items = []
    headers = get_headers(cid, key)
    
    # 1. Получаем список ID (v3)
    url_list = "https://api-seller.ozon.ru/v3/product/list"
    # 2. Получаем Атрибуты (Имя, Бренд, Фото) (v3)
    url_attr = "https://api-seller.ozon.ru/v3/products/info/attributes"
    # 3. Получаем Цены (v4)
    url_price = "https://api-seller.ozon.ru/v4/product/info/prices"
    
    last_id = ""

    while True:
        try:
            # --- ШАГ 1: Список ID ---
            payload_list = { 
                "filter": { "visibility": "ALL" }, 
                "limit": 100,
                "last_id": last_id if last_id else ""
            }
            r = requests.post(url_list, headers=headers, json=payload_list)
            if r.status_code != 200:
                send_log(f"OZ List ERR {r.status_code}: {r.text[:100]}")
                break
            
            list_data = r.json().get("result", {}).get("items", [])
            if not list_data: break
            
            # Собираем ID текущей пачки
            ids = [i.get("product_id") for i in list_data]
            
            # --- ШАГ 2: Атрибуты (Attributes) ---
            attr_map = {}
            try:
                r_attr = requests.post(url_attr, headers=headers, json={
                    "filter": {"product_id": ids, "visibility": "ALL"},
                    "limit": 100
                })
                if r_attr.status_code == 200:
                    for p in r_attr.json().get("result", []):
                        attr_map[p.get("id")] = p
                else:
                    send_log(f"OZ Attr ERR {r_attr.status_code}")
            except Exception as e:
                send_log(f"OZ Attr Ex: {e}")

            # --- ШАГ 3: Цены (Prices) ---
            price_map = {}
            try:
                r_price = requests.post(url_price, headers=headers, json={
                    "filter": {"product_id": ids, "visibility": "ALL"},
                    "limit": 100
                })
                if r_price.status_code == 200:
                    for p in r_price.json().get("result", {}).get("items", []):
                        price_map[p.get("product_id")] = p
                else:
                    send_log(f"OZ Price ERR {r_price.status_code}")
            except Exception as e:
                send_log(f"OZ Price Ex: {e}")

            # --- СБОРКА ИТОГОВОЙ СТРОКИ ---
            for basic in list_data:
                pid = basic.get("product_id")
                
                # Данные из List
                offer_id = basic.get("offer_id")
                ozon_id = str(pid)
                
                # Данные из Attributes
                att_data = attr_map.get(pid, {})
                name = att_data.get("name") or "Товар"
                cat_id = str(att_data.get("category_id", ""))
                
                images = att_data.get("images", [])
                primary = images[0].get("file_name", "") if images else ""
                
                brand = extract_brand(att_data.get("attributes", []))
                
                # Данные из Prices
                pr_data = price_map.get(pid, {}).get("price", {})
                
                def f(v): return float(v) if v else 0.0
                
                p_old = f(pr_data.get("old_price"))
                p_sell = f(pr_data.get("price"))
                p_mkt = f(pr_data.get("marketing_price"))
                
                if p_old == 0: p_old = p_sell
                if p_mkt == 0: p_mkt = p_sell
                
                # Структура для Google Sheet:
                # [Фото, Арт.OZ, Арт.Наш, Бренд, Категория, Название, Ц.База, Ц.Прод, Ц.Покуп, Ц.Карта]
                items.append([
                    primary,
                    ozon_id,
                    offer_id,
                    brand,
                    cat_id,
                    name,
                    p_old,
                    p_sell,
                    p_mkt,
                    p_mkt
                ])

            last_id = list_data[-1].get("last_id")
            if not last_id: break
            if len(list_data) < 100: break
            
        except Exception as e:
            send_log(f"OZ Loop Ex: {str(e)}")
            break
            
    return items

# --- OZON STOCK (Остатки v2) ---
def fetch_stocks(cid, key):
    items = []
    url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    try:
        r = requests.post(url, headers=get_headers(cid, key), json={"limit": 1000, "offset": 0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                sku = str(row.get("sku", ""))
                offer_id = row.get("item_code") or sku
                for wh in row.get("warehouses", []):
                    qty = wh.get("item_cnt", 0)
                    wh_name = wh.get("warehouse_name", "Склад")
                    if qty > 0:
                        items.append([wh_name, offer_id, qty])
    except Exception as e:
        send_log(f"OZ Stock Ex: {str(e)}")
    return items

# --- OZON SALES (Продажи FBO v2) ---
def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
    headers = get_headers(cid, key)
    
    while True:
        try:
            payload = {
                "filter": { "since": f"{d_from}T00:00:00Z", "to": f"{d_to}T23:59:59Z" },
                "limit": 1000,
                "page": page,
                "with": {"analytics_data": True, "financial_data": True}
            }
            r = requests.post(url, headers=headers, json=payload)
            if r.status_code != 200:
                send_log(f"OZ Sales ERR {r.status_code}")
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
                    fp = fin_prods.get(sku, {})
                    price = float(fp.get('client_price') or prod.get('price') or 0)
                    
                    items.append([created, typ, oid, str(sku), 1, price, wh_from, wh_to])
            
            if len(res) < 1000: break
            page += 1
            time.sleep(0.2)
        except Exception as e:
            send_log(f"OZ Sales Ex: {str(e)}")
            break
    return items

@app.route("/")
def health(): return "Ozon v108 Modern OK", 200

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