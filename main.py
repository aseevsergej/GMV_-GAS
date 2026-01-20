import os
import time
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

# Секрет для защиты (должен совпадать в GAS)
# В GAS_WEBAPP_URL все еще нужен для отправки ответа
GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL", "")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"

def get_headers(cid, key):
    return {
        "Client-Id": str(cid).strip(), 
        "Api-Key": str(key).strip(),
        "Content-Type": "application/json"
    }

def send_to_gas(payload):
    if not GAS_WEBAPP_URL: return
    payload["secret"] = SECRET_KEY
    try: requests.post(GAS_WEBAPP_URL, json=payload, timeout=10)
    except: pass

def fetch_cards(cid, key):
    items = []
    print(f"📦 Start Cards for {cid}...")
    
    # Payload
    payload = { "filter": { "visibility": "ALL" }, "limit": 1000 }
    
    try:
        # Пробуем v2
        r = requests.post("https://api-seller.ozon.ru/v2/product/list", headers=get_headers(cid, key), json=payload)
        
        # Если 404, пробуем v1 (редкий кейс)
        if r.status_code == 404:
             r = requests.post("https://api-seller.ozon.ru/v1/product/list", headers=get_headers(cid, key), json=payload)

        if r.status_code != 200:
            send_to_gas({"type": "LOG", "msg": f"OZ Err {cid}: {r.status_code} {r.text[:30]}"})
            return []
            
        data = r.json().get("result", {}).get("items", [])
        if not data:
             send_to_gas({"type": "LOG", "msg": f"OZ {cid}: 0 items"})
             return []
        
        # Info
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
                full.get("primary_image", ""), str(pid), str(pid), offer_id,
                "OZON", str(full.get("category_id", "")), full.get("name", f"ID {pid}"), p, mp, mp, st
            ])
            
    except Exception as e:
        send_to_gas({"type": "LOG", "msg": f"Py Crash {cid}: {e}"})
            
    return items

def fetch_stocks(cid, key):
    items = []
    try:
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(cid, key), json={"limit": 1000, "offset":0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                total = sum(w.get("item_cnt",0) for w in row.get("warehouses", []))
                items.append(["FBO", row.get("item_code"), total, 0, str(row.get("sku"))])
    except: pass
    return items

def fetch_sales(cid, key, d_from, d_to):
    items = []
    # Пагинация
    page = 1
    while True:
        try:
            payload = { 
                "filter": { "since": f"{d_from}T00:00:00Z", "to": f"{d_to}T23:59:59Z" }, 
                "limit": 1000,
                "page": page
            }
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: break
            
            postings = r.json().get("result", [])
            if not postings: break
            
            for p in postings:
                date_str = p.get("created_at")[:10]
                status = "Отмена" if "cancelled" in p.get("status","") else "Заказ"
                wh = p.get("analytics_data", {}).get("warehouse_name", "Ozon")
                for prod in p.get("products", []):
                    items.append([date_str, "00:00", status, prod.get("offer_id"), str(prod.get("sku")), 1, float(prod.get("price",0)), 0, 0, 0, wh, "RU", p.get("posting_number")])
            
            if len(postings) < 1000: break
            page += 1
            time.sleep(0.3)
        except: break
    return items

@app.route("/")
def health(): return "Ozon Universal OK", 200

@app.route("/sync", methods=['POST'])
def sync():
    data = request.json
    # ПОЛУЧАЕМ КЛЮЧИ ОТ GOOGLE SCRIPT
    cid = data.get("clientId")
    key = data.get("apiKey")
    
    if not cid or not key:
        send_to_gas({"type": "LOG", "msg": "Py: Missing Keys from GAS"})
        return jsonify({"error": "No Keys"}), 400

    mode = data.get("mode")
    d_from = data.get("dateFrom")
    d_to = data.get("dateTo")
    
    send_to_gas({"type": "LOG", "msg": f"Py: Start {mode} for {cid}..."})

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
        send_to_gas({"type": "LOG", "msg": "Py: Funnel Skipped"})
        return jsonify({"status": "empty"}), 200
        
    if rows:
        send_to_gas({"type": "DATA", "sheetName": target, "rows": rows})
        return jsonify({"status": "ok", "count": len(rows)}), 200
    else:
        return jsonify({"status": "empty"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)