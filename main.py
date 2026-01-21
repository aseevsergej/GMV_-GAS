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
    # Максимальная очистка
    c_clean = str(cid).strip().replace('"', '').replace("'", "")
    k_clean = str(key).strip().replace('"', '').replace("'", "")
    return {
        "Client-Id": c_clean, 
        "Api-Key": k_clean,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

# --- ПОИСК БРЕНДА ---
def extract_brand(attributes):
    for a in attributes:
        if a.get("attribute_id") in [85, 31]:
            vals = a.get("values", [])
            if vals: return vals[0].get("value", "")
    return ""

# --- OZON CARDS (ТОВАРЫ) - STRICT V2 ---
def fetch_cards(cid, key):
    items = []
    headers = get_headers(cid, key)
    
    # СТРОГО V2
    url_list = "https://api-seller.ozon.ru/v2/product/list"
    url_info = "https://api-seller.ozon.ru/v2/product/info/list"
    
    last_id = 0 # В v2 это число!

    while True:
        try:
            # 1. СПИСОК (List)
            payload_list = { 
                "filter": { "visibility": "ALL" }, 
                "limit": 100
            }
            if last_id > 0: 
                payload_list["last_id"] = last_id
            
            r = requests.post(url_list, headers=headers, json=payload_list)
            
            if r.status_code != 200:
                send_log(f"OZ List ERR {r.status_code}: {r.text[:200]}")
                break
            
            data_list = r.json().get("result", {}).get("items", [])
            if not data_list: break
            
            # 2. ПОДГОТОВКА ID (Info)
            # Принудительно конвертируем в INT (Ozon не любит строки здесь)
            ids = []
            for i in data_list:
                try: ids.append(int(i.get("product_id")))
                except: pass
            
            if not ids: break

            # 3. ДЕТАЛИ (Info)
            info_map = {}
            payload_info = {"product_id": ids}
            
            r_info = requests.post(url_info, headers=headers, json=payload_info)
            
            if r_info.status_code == 200:
                for item in r_info.json().get("result", {}).get("items", []):
                    info_map[item.get("id")] = item
            else:
                # Логируем, что именно мы отправили, если упала ошибка
                send_log(f"OZ Info ERR {r_info.status_code}. Pay: {json.dumps(payload_info)}")
            
            # 4. СБОРКА
            for basic in data_list:
                pid = int(basic.get("product_id"))
                full = info_map.get(pid, {})
                
                offer_id = full.get("offer_id") or basic.get("offer_id") or ""
                ozon_id = str(pid)
                name = full.get("name") or "Товар"
                cat_id = str(full.get("category_id", ""))
                
                # Фото
                primary = full.get("primary_image") or ""
                if not primary and full.get("images"): 
                    imgs = full.get("images")
                    # Обработка разных форматов ответа
                    if imgs and isinstance(imgs[0], str): primary = imgs[0]
                    elif imgs and isinstance(imgs[0], dict): primary = imgs[0].get("file_name", "")
                
                brand = extract_brand(full.get("attributes", []))
                
                # Цены
                def get_p(k): return float(full.get(k) or full.get("price", {}).get(k) or 0)
                
                p_old = get_p("old_price")
                p_sell = get_p("price")
                p_mkt = get_p("marketing_price")
                
                if p_old == 0: p_old = p_sell
                if p_mkt == 0: p_mkt = p_sell
                
                items.append([
                    primary, ozon_id, offer_id, brand, cat_id, name, p_old, p_sell, p_mkt, p_mkt
                ])
            
            # Пагинация для v2 - берем последний PID как число
            last_item = data_list[-1]
            last_id = int(last_item.get("product_id"))
            
            if len(data_list) < 100: break
            
        except Exception as e:
            send_log(f"OZ Loop Error: {str(e)}")
            break
            
    return items

# --- OZON STOCK (Остатки v2) ---
def fetch_stocks(cid, key):
    items = []
    headers = get_headers(cid, key)
    url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    try:
        r = requests.post(url, headers=headers, json={"limit": 1000, "offset": 0})
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
        else:
            send_log(f"OZ Stock ERR {r.status_code}")
    except Exception as e:
        send_log(f"OZ Stock Ex: {str(e)}")
    return items

# --- OZON SALES (Продажи FBO v2) ---
def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    headers = get_headers(cid, key)
    url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
    
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
                send_log(f"OZ Sales ERR {r.status_code}: {r.text[:50]}")
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
def health(): return "Ozon v111 Strict OK", 200

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