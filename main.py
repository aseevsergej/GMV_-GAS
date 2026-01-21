import os
import time
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL", "")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"
START_TIME = 0

# --- ЛОГИРОВАНИЕ ---
def send_log(msg):
    if not GAS_WEBAPP_URL: return
    try:
        requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "LOG", "msg": str(msg)[:1000]}, timeout=3)
    except: pass

def send_data(sheet, rows):
    if not GAS_WEBAPP_URL: return
    try:
        requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "DATA", "sheetName": sheet, "rows": rows}, timeout=30)
    except: pass

# --- ЗАГОЛОВКИ ---
def get_headers(cid, key):
    return {
        "Client-Id": str(cid).strip().replace('"', '').replace("'", ""),
        "Api-Key": str(key).strip().replace('"', '').replace("'", ""),
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

# --- OZON CARDS (Safe Mode) ---
def fetch_cards(cid, key):
    items = []
    headers = get_headers(cid, key)
    
    # URLS
    URL_LIST = "https://api-seller.ozon.ru/v2/product/list"
    URL_INFO_LIST = "https://api-seller.ozon.ru/v2/product/info/list"
    URL_INFO_ONE = "https://api-seller.ozon.ru/v2/product/info"
    
    last_id = "" 
    
    global START_TIME
    
    while True:
        # ЗАЩИТА ОТ ТАЙМАУТА: Если прошло > 25 сек, выходим
        if (time.time() - START_TIME) > 25:
            send_log("Time Limit Reached. Returning partial data.")
            break

        # 1. СПИСОК (List v2)
        payload_list = { "filter": { "visibility": "ALL" }, "limit": 50 } # Снизил лимит до 50
        if last_id: payload_list["last_id"] = str(last_id)
        
        try:
            r = requests.post(URL_LIST, headers=headers, json=payload_list)
            if r.status_code != 200:
                send_log(f"List ERR {r.status_code}")
                break
            
            data_list = r.json().get("result", {}).get("items", [])
            if not data_list: break
        except Exception as e:
            send_log(f"List Crash: {e}")
            break

        # ID товаров
        ids = [int(i.get("product_id")) for i in data_list]
        
        # 2. ДЕТАЛИ (Попытка получить данные)
        info_map = {}
        details_ok = False
        
        # Попытка А: Массовая (Info List)
        try:
            r_info = requests.post(URL_INFO_LIST, headers=headers, json={"product_id": ids})
            if r_info.status_code == 200:
                details_ok = True
                for i in r_info.json().get("result", {}).get("items", []):
                    info_map[i.get("id")] = i
            elif r_info.status_code == 404:
                 # Попытка Б: Если массовая не работает, попробуем ОДИН товар (для проверки)
                 # Это поможет понять, есть ли доступ вообще
                 try:
                     r_one = requests.post(URL_INFO_ONE, headers=headers, json={"product_id": ids[0]})
                     if r_one.status_code == 200:
                         # Если одиночный работает - значит массовый сломан. 
                         # В будущем можно переписать на цикл, но пока просто логируем.
                         send_log("Mass Info 404, but Single Info OK! API Limitation detected.")
                         one_data = r_one.json().get("result", {})
                         info_map[one_data.get("id")] = one_data
                 except: pass
        except: pass

        # 3. СБОРКА (Даже если details_ok = False)
        for basic in data_list:
            pid = int(basic.get("product_id"))
            
            # Если деталей нет, берем базовые данные из Списка
            # Basic содержит: product_id, offer_id
            
            details = info_map.get(pid, {})
            
            offer_id = details.get("offer_id") or basic.get("offer_id") or "NoOfferID"
            ozon_id = str(pid)
            
            # Если деталей нет - ставим заглушки
            if details:
                name = details.get("name") or "Товар"
                cat_id = str(details.get("category_id", ""))
                primary = details.get("primary_image") or ""
                if not primary and details.get("images"):
                     imgs = details.get("images")
                     primary = imgs[0] if isinstance(imgs[0], str) else imgs[0].get("file_name", "")
                brand = extract_brand(details.get("attributes", []))
                
                def gp(k): return float(details.get(k) or details.get("price", {}).get(k) or 0)
                p_old = gp("old_price")
                p_sell = gp("price")
                p_mkt = gp("marketing_price")
            else:
                # ЗАГЛУШКИ (чтобы таблица заполнилась)
                name = "Детали недоступны (404)"
                cat_id = "-"
                primary = ""
                brand = "-"
                p_old = 0
                p_sell = 0
                p_mkt = 0

            if p_old == 0: p_old = p_sell
            if p_mkt == 0: p_mkt = p_sell
            
            items.append([
                primary, ozon_id, offer_id, brand, cat_id, name, p_old, p_sell, p_mkt, p_mkt
            ])
        
        # Пагинация
        last_item = data_list[-1]
        last_id = str(last_item.get("product_id"))
        
        if len(data_list) < 50: break
            
    return items

# --- STOCK & SALES (Standard) ---
def fetch_stocks(cid, key):
    items = []
    headers = get_headers(cid, key)
    url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    try:
        r = requests.post(url, headers=headers, json={"limit": 1000, "offset": 0})
        if r.status_code == 200:
            for row in r.json().get("result", {}).get("rows", []):
                sku = str(row.get("sku", ""))
                oid = row.get("item_code") or sku
                for wh in row.get("warehouses", []):
                    if wh.get("item_cnt", 0) > 0:
                        items.append([wh.get("warehouse_name"), oid, wh.get("item_cnt")])
    except Exception as e:
        send_log(f"Stock Err: {e}")
    return items

def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    headers = get_headers(cid, key)
    url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
    
    global START_TIME
    
    while True:
        if (time.time() - START_TIME) > 25: break

        payload = {
            "filter": { "since": f"{d_from}T00:00:00Z", "to": f"{d_to}T23:59:59Z" },
            "limit": 1000, "page": page,
            "with": {"analytics_data": True, "financial_data": True}
        }
        try:
            r = requests.post(url, headers=headers, json=payload)
            if r.status_code != 200: break
            res = r.json().get("result", [])
            if not res: break
            
            for p in res:
                created = p.get("created_at", "")[:10]
                typ = "Отмена" if "cancelled" in str(p.get("status", "")).lower() else "Продажа"
                an = p.get("analytics_data") or {}
                fin = p.get("financial_data") or {}
                fin_prods = {x.get('product_id'): x for x in fin.get('products', [])}
                for prod in p.get("products", []):
                    sku = prod.get("sku")
                    fp = fin_prods.get(sku, {})
                    price = float(fp.get('client_price') or prod.get('price') or 0)
                    items.append([created, typ, prod.get("offer_id"), str(sku), 1, price, an.get("warehouse_name"), an.get("region")])
            
            if len(res) < 1000: break
            page += 1
            time.sleep(0.2)
        except: break
    return items

@app.route("/")
def health(): return "Ozon v114 Protected OK", 200

@app.route("/sync", methods=['POST'])
def sync():
    global START_TIME
    START_TIME = time.time()
    
    try:
        data = request.json
        rows = []
        target = ""
        mode = data.get("mode")
        
        cid = data.get("clientId")
        key = data.get("apiKey")
        
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
            
        # Всегда возвращаем успех, если rows пустой или полный, чтобы GAS не зависал
        if rows:
            send_data(target, rows)
            return jsonify({"status": "ok", "count": len(rows)}), 200
        else:
            return jsonify({"status": "empty"}), 200
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)