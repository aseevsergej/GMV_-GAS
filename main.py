import os
import time
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL", "")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"

def get_headers(cid, key):
    return {
        "Client-Id": str(cid).strip(), 
        "Api-Key": str(key).strip(),
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

def send_to_gas(payload):
    if not GAS_WEBAPP_URL: return
    payload["secret"] = SECRET_KEY
    try: requests.post(GAS_WEBAPP_URL, json=payload, timeout=10)
    except: pass

# --- OZON CARDS (МЕТОД v3) ---
def fetch_cards(cid, key):
    items = []
    print(f"📦 Start Cards {cid}...")
    
    # Сначала пробуем v3 (актуальный в 2026)
    # Если не выйдет - v2
    endpoints = [
        "https://api-seller.ozon.ru/v3/product/list",
        "https://api-seller.ozon.ru/v2/product/list"
    ]
    
    last_id = ""
    
    for url in endpoints:
        has_error = False
        while True:
            # Для v3/v2 payload одинаковый
            payload = { 
                "filter": { "visibility": "ALL" }, 
                "limit": 1000 
            }
            if last_id: payload["last_id"] = last_id
            
            try:
                r = requests.post(url, headers=get_headers(cid, key), json=payload)
                
                if r.status_code != 200:
                    # Если этот урл не сработал, прерываем цикл пагинации и идем к следующему урлу
                    if page_idx == 0: # Только если это первая страница
                         send_to_gas({"type": "LOG", "msg": f"Try {url.split('/')[-3]}: {r.status_code}"})
                    has_error = True
                    break
                
                data = r.json().get("result", {}).get("items", [])
                if not data: break # Данные кончились или пусто
                
                # Получаем детали (Info)
                # Для v3 product/list возвращает product_id, но для Info v2 все еще актуален
                ids = [i["product_id"] for i in data]
                
                r_info = requests.post("https://api-seller.ozon.ru/v2/product/info/list", headers=get_headers(cid, key), json={"product_id": ids})
                info_map = {}
                if r_info.status_code == 200:
                    for i in r_info.json().get("result", {}).get("items", []):
                        info_map[i["id"]] = i
                
                for item_base in data:
                    pid = item_base["product_id"]
                    full = info_map.get(pid, {})
                    offer_id = full.get("offer_id") or item_base.get("offer_id") or ""
                    
                    p = float(full.get("price", {}).get("price", 0))
                    mp = float(full.get("price", {}).get("marketing_price", 0) or p)
                    st = full.get("stocks", {}).get("present", 0)
                    
                    items.append([
                        full.get("primary_image", ""), 
                        str(pid), str(pid), offer_id,
                        "OZON", str(full.get("category_id", "")), full.get("name", f"ID {pid}"), 
                        p, mp, mp, st
                    ])
                
                last_id = data[-1]["product_id"]
                if len(data) < 1000: 
                    return items # Успех, возвращаем все что есть
                
            except Exception as e:
                has_error = True
                break
        
        # Если первый урл сработал и вернул данные (или успешно прошел), выходим
        if items and not has_error: return items
        # Если была ошибка на v3, цикл перейдет к v2
    
    # Если ничего не помогло
    if not items:
        send_to_gas({"type": "LOG", "msg": f"OZ {cid}: Cards Failed (All Methods)"})
            
    return items

# --- OZON STOCK ---
def fetch_stocks(cid, key):
    items = []
    try:
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(cid, key), json={"limit": 1000, "offset":0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                whs = row.get("warehouses") or []
                total = sum(w.get("item_cnt",0) for w in whs)
                items.append(["FBO Ozon", row.get("item_code"), total, 0, str(row.get("sku"))])
    except: pass
    return items

# --- OZON SALES ---
def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    since_dt = f"{d_from}T00:00:00Z"
    to_dt = f"{d_to}T23:59:59Z"

    while True:
        try:
            payload = { "filter": { "since": since_dt, "to": to_dt }, "limit": 1000, "page": page }
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: break
            
            res = r.json().get("result")
            if not res or not isinstance(res, list): break
            
            for p in res:
                created = p.get("created_at") or "2000-01-01T00:00:00Z"
                status = "Отмена" if "cancelled" in (p.get("status") or "").lower() else "Заказ"
                wh = (p.get("analytics_data") or {}).get("warehouse_name", "Ozon")
                
                products = p.get("products") or []
                for prod in products:
                    price = float(prod.get("price", 0))
                    items.append([created[:10], created[11:16], status, prod.get("offer_id"), str(prod.get("sku")), 1, price, price, price, 0, wh, "RU", p.get("posting_number")])
            
            if len(res) < 1000: break
            page += 1
            time.sleep(0.2)
        except: break
    return items

@app.route("/")
def health(): return "Ozon v91 OK", 200

@app.route("/sync", methods=['POST'])
def sync():
    try:
        data = request.json
        if not data: return jsonify({"error": "No JSON"}), 400
        
        cid = data.get("clientId")
        key = data.get("apiKey")
        mode = data.get("mode")
        d_from = data.get("dateFrom")
        d_to = data.get("dateTo")
        
        if not cid or not key:
            send_to_gas({"type": "LOG", "msg": "Py: Keys Missing"})
            return jsonify({"error": "Keys missing"}), 400

        # send_to_gas({"type": "LOG", "msg": f"Py: {mode} {cid}..."})

        rows = []
        target = ""
        
        if mode == "CARDS":
            rows = fetch_cards(cid, key)
            target = "OZ_CARDS_PY"
        elif mode == "STOCK":
            rows = fetch_stocks(cid, key)
            target = "OZ_STOCK_PY"
        elif mode == "SALES":
            rows = fetch_sales(cid, key, d_from, d_to)
            target = "OZ_SALES_PY"
        elif mode == "FUNNEL":
            return jsonify({"status": "empty"}), 200
            
        if rows:
            send_to_gas({"type": "DATA", "sheetName": target, "rows": rows})
            return jsonify({"status": "ok", "count": len(rows)}), 200
        else:
            return jsonify({"status": "empty"}), 200
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)