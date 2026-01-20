import os
import time
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- КОНФИГУРАЦИЯ ---
OZON_CLIENT_ID = os.environ.get("OZON_CLIENT_ID")
OZON_API_KEY = os.environ.get("OZON_API_KEY")
GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"

def get_headers():
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }

def send_to_gas(payload):
    if not GAS_WEBAPP_URL: return
    payload["secret"] = SECRET_KEY
    try:
        requests.post(GAS_WEBAPP_URL, json=payload, timeout=10)
    except: pass

def fetch_cards():
    items = []
    last_id = ""
    print("📦 Start Cards...")
    
    while True:
        payload = { "filter": { "visibility": "ALL" }, "limit": 100 }
        if last_id: payload["last_id"] = last_id
        
        try:
            # 1. Получаем ID
            r = requests.post("https://api-seller.ozon.ru/v2/product/list", headers=get_headers(), json=payload)
            if r.status_code != 200:
                send_to_gas({"type": "LOG", "msg": f"Py Err List: {r.status_code} {r.text[:50]}"})
                break
                
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            # 2. Получаем Инфо
            ids = [i["product_id"] for i in data]
            r_info = requests.post("https://api-seller.ozon.ru/v2/product/info/list", headers=get_headers(), json={"product_id": ids})
            
            info_map = {}
            if r_info.status_code == 200:
                info_list = r_info.json().get("result", {}).get("items", [])
                for i in info_list: info_map[i["id"]] = i
            else:
                 send_to_gas({"type": "LOG", "msg": f"Py Err Info: {r_info.status_code}"})

            # 3. Собираем
            for item_base in data:
                pid = item_base["product_id"]
                full = info_map.get(pid, {})
                
                # Если full пустой, берем хотя бы ID из item_base
                offer_id = full.get("offer_id") or item_base.get("offer_id") or ""
                
                p = float(full.get("price", {}).get("price", 0))
                mp = float(full.get("price", {}).get("marketing_price", 0) or p)
                st = full.get("stocks", {}).get("present", 0)
                
                items.append([
                    full.get("primary_image", ""), 
                    str(pid), str(pid), 
                    offer_id,
                    "OZON", 
                    str(full.get("category_id", "")), 
                    full.get("name", "Товар " + str(pid)), 
                    p, mp, mp, st
                ])
            
            last_id = data[-1]["product_id"]
            if len(data) < 100: break
            time.sleep(0.2)
            
        except Exception as e:
            send_to_gas({"type": "LOG", "msg": f"Py Crash Cards: {str(e)}"})
            break
            
    return items

def fetch_stocks():
    # Заглушка для Stocks (чтобы не дублировать код, если пока не работает)
    # Реализуем простейший FBO
    items = []
    try:
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(), json={"limit": 100, "offset":0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                total = sum(w.get("item_cnt",0) for w in row.get("warehouses", []))
                items.append(["FBO", row.get("item_code"), total, 0, str(row.get("sku"))])
    except: pass
    return items

def fetch_sales(d_from, d_to):
    items = []
    # Реализация запроса продаж (упрощенная)
    try:
        payload = { "filter": { "since": f"{d_from}T00:00:00Z", "to": f"{d_to}T23:59:59Z" }, "limit": 100 }
        r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=get_headers(), json=payload)
        if r.status_code == 200:
            postings = r.json().get("result", [])
            for p in postings:
                # Парсинг заказа... (упростим для краткости, главное структура)
                date_str = p.get("created_at")[:10]
                for prod in p.get("products", []):
                    items.append([date_str, "00:00", "Заказ", prod.get("offer_id"), str(prod.get("sku")), 1, float(prod.get("price",0)), 0, 0, 0, "Ozon", "RU", p.get("posting_number")])
    except Exception as e:
        send_to_gas({"type": "LOG", "msg": f"Py Err Sales: {str(e)}"})
    return items

@app.route("/")
def health(): return "Ozon Service v2 OK", 200

@app.route("/sync", methods=['POST'])
def sync():
    data = request.json
    mode = data.get("mode")
    d_from = data.get("dateFrom")
    d_to = data.get("dateTo")
    
    if not OZON_API_KEY: 
        return jsonify({"error": "No API Key"}), 500

    rows = []
    target = ""
    
    if mode == "CARDS":
        rows = fetch_cards()
        target = "OZ_CARDS_PY"
    elif mode == "STOCK":
        rows = fetch_stocks()
        target = "OZ_STOCK_PY"
    elif mode == "SALES":
        rows = fetch_sales(d_from, d_to)
        target = "OZ_SALES_PY"
    elif mode == "FUNNEL":
        # Заглушка, чтобы не было ошибки 400
        send_to_gas({"type": "LOG", "msg": "Python: Funnel not implemented yet"})
        return jsonify({"status": "empty"}), 200
    else:
        return jsonify({"error": f"Unknown mode {mode}"}), 400
        
    if rows:
        send_to_gas({"type": "DATA", "sheetName": target, "rows": rows})
        return jsonify({"status": "ok", "count": len(rows)}), 200
    else:
        send_to_gas({"type": "LOG", "msg": f"Python: {mode} returned 0 rows"})
        return jsonify({"status": "empty"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)