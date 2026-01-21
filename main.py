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
    c_clean = str(cid).strip().replace('"', '').replace("'", "")
    k_clean = str(key).strip().replace('"', '').replace("'", "")
    return {
        "Client-Id": c_clean, 
        "Api-Key": k_clean,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

# --- УНИВЕРСАЛЬНЫЙ POST ---
def post_req(url, headers, payload, label):
    try:
        r = requests.post(url, headers=headers, json=payload)
        if r.status_code == 200:
            return r.json()
        else:
            # Логируем ошибку, но коротко
            send_log(f"OZ {label} ERR {r.status_code}. Msg: {r.text[:200]}")
            return None
    except Exception as e:
        send_log(f"OZ {label} Ex: {str(e)}")
        return None

# --- ПОИСК БРЕНДА ---
def extract_brand(attributes):
    for a in attributes:
        if a.get("attribute_id") in [85, 31]:
            vals = a.get("values", [])
            if vals: return vals[0].get("value", "")
    return ""

# --- OZON CARDS (v3 List + v3 Attr + v4 Price) ---
def fetch_cards(cid, key):
    items = []
    headers = get_headers(cid, key)
    
    # URLS
    url_list = "https://api-seller.ozon.ru/v3/product/list"
    url_attr = "https://api-seller.ozon.ru/v3/products/info/attributes"
    url_price = "https://api-seller.ozon.ru/v4/product/info/prices"
    
    last_id = "" # В v3 это строковый токен

    while True:
        # 1. СПИСОК (List v3)
        payload_list = { 
            "filter": { "visibility": "ALL" }, 
            "limit": 100
        }
        if last_id: payload_list["last_id"] = last_id
        
        resp_list = post_req(url_list, headers, payload_list, "ListV3")
        if not resp_list: break
        
        # Получаем items и новый last_id из корня result
        result = resp_list.get("result", {})
        data_list = result.get("items", [])
        new_last_id = result.get("last_id", "")
        
        if not data_list: break
        
        # Собираем ID (строго int)
        ids = [int(i.get("product_id")) for i in data_list]
        
        info_map = {} # Сюда сложим данные из атрибутов и цен
        
        # 2. АТРИБУТЫ (v3)
        # Убрали visibility из фильтра, оставили только product_id
        payload_attr = {
            "filter": { "product_id": ids },
            "limit": 100
        }
        resp_attr = post_req(url_attr, headers, payload_attr, "AttrV3")
        if resp_attr:
            for item in resp_attr.get("result", []):
                info_map[item.get("id")] = item

        # 3. ЦЕНЫ (v4)
        payload_price = {
            "filter": { "product_id": ids },
            "limit": 100
        }
        resp_price = post_req(url_price, headers, payload_price, "PriceV4")
        if resp_price:
            for item in resp_price.get("result", {}).get("items", []):
                pid = item.get("product_id")
                if pid not in info_map: info_map[pid] = {}
                info_map[pid]["price_info"] = item.get("price", {})

        # 4. СБОРКА
        for basic in data_list:
            pid = int(basic.get("product_id"))
            
            # Данные из списка
            offer_id = basic.get("offer_id")
            ozon_id = str(pid)
            
            # Данные из InfoMap
            details = info_map.get(pid, {})
            
            name = details.get("name") or "Товар"
            cat_id = str(details.get("category_id", ""))
            
            # Фото
            primary = ""
            imgs = details.get("images", [])
            if imgs:
                # В v3 images - это список объектов {file_name, ...}
                if isinstance(imgs[0], dict): primary = imgs[0].get("file_name", "")
                elif isinstance(imgs[0], str): primary = imgs[0]
            
            brand = extract_brand(details.get("attributes", []))
            
            # Цены
            prices = details.get("price_info", {})
            def get_p(k): return float(prices.get(k) or 0)
            
            p_old = get_p("old_price")
            p_sell = get_p("price")
            p_mkt = get_p("marketing_price")
            
            if p_old == 0: p_old = p_sell
            if p_mkt == 0: p_mkt = p_sell
            
            items.append([
                primary, ozon_id, offer_id, brand, cat_id, name, p_old, p_sell, p_mkt, p_mkt
            ])
        
        # Пагинация
        last_id = new_last_id
        if not last_id: break # Если токена нет, значит конец
        if len(data_list) < 100: break # Или если пришло меньше лимита
            
    return items

# --- OZON STOCK (v2) ---
def fetch_stocks(cid, key):
    items = []
    headers = get_headers(cid, key)
    url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    
    resp = post_req(url, headers, {"limit": 1000, "offset": 0}, "Stock")
    if resp:
        rows = resp.get("result", {}).get("rows", [])
        for row in rows:
            sku = str(row.get("sku", ""))
            offer_id = row.get("item_code") or sku
            for wh in row.get("warehouses", []):
                qty = wh.get("item_cnt", 0)
                wh_name = wh.get("warehouse_name", "Склад")
                if qty > 0:
                    items.append([wh_name, offer_id, qty])
    return items

# --- OZON SALES (v2 FBO) ---
def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    headers = get_headers(cid, key)
    url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
    
    while True:
        payload = {
            "filter": { "since": f"{d_from}T00:00:00Z", "to": f"{d_to}T23:59:59Z" },
            "limit": 1000,
            "page": page,
            "with": {"analytics_data": True, "financial_data": True}
        }
        resp = post_req(url, headers, payload, "Sales")
        if not resp: break
        
        res = resp.get("result", [])
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

    return items

@app.route("/")
def health(): return "Ozon v112 Clean OK", 200

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