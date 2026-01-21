import os
import time
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL", "")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"

# --- LOGGING ---
def send_log(msg):
    if not GAS_WEBAPP_URL: return
    try:
        requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "LOG", "msg": str(msg)[:1000]}, timeout=3)
    except: pass

def send_data(sheet, rows):
    if not GAS_WEBAPP_URL: return
    try:
        requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "DATA", "sheetName": sheet, "rows": rows}, timeout=45)
    except: pass

# --- HEADERS ---
def get_headers(cid, key):
    return {
        "Client-Id": str(cid).strip().replace('"', '').replace("'", ""),
        "Api-Key": str(key).strip().replace('"', '').replace("'", ""),
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

# --- OZON CARDS (Skeleton) ---
def fetch_cards(cid, key):
    items = []
    headers = get_headers(cid, key)
    
    URL_LIST = "https://api-seller.ozon.ru/v2/product/list"
    URL_INFO_LIST = "https://api-seller.ozon.ru/v2/product/info/list"
    
    last_id = "" 
    details_blocked = False # Флаг блокировки деталей
    
    while True:
        # 1. СПИСОК (List v2)
        payload_list = { "filter": { "visibility": "ALL" }, "limit": 100 }
        if last_id: payload_list["last_id"] = str(last_id)
        
        try:
            r = requests.post(URL_LIST, headers=headers, json=payload_list)
            if r.status_code != 200:
                send_log(f"List Fetch Error: {r.status_code}")
                break
            
            data_list = r.json().get("result", {}).get("items", [])
            if not data_list: break
        except Exception as e:
            send_log(f"List Crash: {e}")
            break

        # 2. ПОПЫТКА ПОЛУЧИТЬ ДЕТАЛИ (Только если не заблокировано)
        info_map = {}
        ids = [int(i.get("product_id")) for i in data_list]
        
        if not details_blocked:
            try:
                # Пробуем получить детали
                r_info = requests.post(URL_INFO_LIST, headers=headers, json={"product_id": ids})
                
                if r_info.status_code == 200:
                    for i in r_info.json().get("result", {}).get("items", []):
                        info_map[i.get("id")] = i
                elif r_info.status_code == 404:
                    # Если 404 - блокируем попытки для всех следующих страниц
                    details_blocked = True
                    send_log("Details API returned 404. Switching to SKELETON mode (IDs only).")
                else:
                    send_log(f"Details Err: {r_info.status_code}")
            except:
                details_blocked = True

        # 3. ЗАПОЛНЕНИЕ (Даже если details_blocked=True)
        for basic in data_list:
            pid = int(basic.get("product_id"))
            
            # Данные из Списка (всегда есть)
            offer_id = basic.get("offer_id") or "Нет Артикула"
            ozon_id = str(pid)
            
            # Данные из Деталей (могут отсутствовать)
            details = info_map.get(pid, {})
            
            if details:
                name = details.get("name") or "Товар"
                cat = str(details.get("category_id", ""))
                
                primary = details.get("primary_image") or ""
                if not primary and details.get("images"):
                    imgs = details.get("images")
                    primary = imgs[0] if isinstance(imgs[0], str) else imgs[0].get("file_name", "")
                
                # Бренд
                brand = ""
                for a in details.get("attributes", []):
                    if a.get("attribute_id") in [85, 31]:
                        vals = a.get("values", [])
                        if vals: brand = vals[0].get("value", "")
                        break

                def gp(k): return float(details.get(k) or details.get("price", {}).get(k) or 0)
                p_old, p_sell, p_mkt = gp("old_price"), gp("price"), gp("marketing_price")
                
            else:
                # ЗАГЛУШКИ (Скелет)
                name = "Доступ ограничен (404)"
                cat = "-"
                primary = ""
                brand = "OZON"
                p_old, p_sell, p_mkt = 0, 0, 0
            
            if p_old == 0: p_old = p_sell
            if p_mkt == 0: p_mkt = p_sell

            # Структура колонок строго по ТЗ
            items.append([
                primary, ozon_id, offer_id, brand, cat, name, p_old, p_sell, p_mkt, p_mkt
            ])

        # Пагинация
        last_item = data_list[-1]
        last_id = str(last_item.get("product_id"))
        
        if len(data_list) < 100: break

    return items

# --- STOCK & SALES ---
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
    except Exception as e: send_log(f"Stock Err: {e}")
    return items

def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    headers = get_headers(cid, key)
    url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
    
    while True:
        try:
            payload = {
                "filter": { "since": f"{d_from}T00:00:00Z", "to": f"{d_to}T23:59:59Z" },
                "limit": 1000, "page": page,
                "with": {"analytics_data": True, "financial_data": True}
            }
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
def health(): return "Ozon v115 Skeleton OK", 200

@app.route("/sync", methods=['POST'])
def sync():
    try:
        data = request.json
        mode = data.get("mode")
        rows = []
        target = ""
        
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
        
        # Всегда возвращаем JSON, даже если список пуст
        if rows: send_data(target, rows)
        return jsonify({"status": "ok", "count": len(rows)}), 200
            
    except Exception as e:
        return jsonify({"error": str(e)}), 200 # Возвращаем 200 даже при ошибке, чтобы GAS не падал

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)