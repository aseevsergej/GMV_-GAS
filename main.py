import os
import time
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL", "")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"

def get_headers(cid, key):
    # Убра��и "шумные" заголовки браузера. Оставили только то, что требует Ozon API.
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

# --- OZON CARDS ---
def fetch_cards(cid, key):
    items = []
    
    # 1. Проверка доступа через самый простой метод (Склады)
    # Если это не сработает - проблема точно в ключах или IP сервера (Render) забанен Ozon
    try:
        ping = requests.post("https://api-seller.ozon.ru/v1/warehouse/list", headers=get_headers(cid, key), json={})
        if ping.status_code == 401 or ping.status_code == 403:
            send_to_gas({"type": "LOG", "msg": f"OZ Auth: Неверный ID или Key ({ping.status_code})"})
            return []
        if ping.status_code == 404:
            # Если 404 тут - значит IP забанен или URL неверен глобально
            send_to_gas({"type": "LOG", "msg": "OZ Critical: 404 on warehouse/list"})
    except Exception as e:
        send_to_gas({"type": "LOG", "msg": f"OZ Conn Error: {str(e)}"})
        return []

    # 2. Получение списка товаров (v2)
    url_list = "https://api-seller.ozon.ru/v2/product/list"
    url_info = "https://api-seller.ozon.ru/v2/product/info/list"
    last_id = ""
    
    while True:
        try:
            # Минималистичный payload
            payload = { 
                "filter": { "visibility": "ALL" }, 
                "limit": 500 
            }
            if last_id: payload["last_id"] = last_id
            
            r = requests.post(url_list, headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: 
                send_to_gas({"type": "LOG", "msg": f"OZ List Err: {r.status_code}"})
                break
            
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            # Детали
            ids = [i.get("product_id") for i in data]
            r_info = requests.post(url_info, headers=get_headers(cid, key), json={"product_id": ids})
            
            info_map = {}
            if r_info.status_code == 200:
                for i in r_info.json().get("result", {}).get("items", []):
                    info_map[i.get("id")] = i
            
            for item_base in data:
                pid = item_base.get("product_id")
                full = info_map.get(pid, {})
                
                offer_id = full.get("offer_id") or item_base.get("offer_id") or ""
                ozon_id = str(pid)
                name = full.get("name") or ""
                cat = str(full.get("category_id", ""))
                
                primary = full.get("primary_image") or ""
                if not primary and full.get("images"): primary = full["images"][0]
                
                brand = "OZON" 
                
                def gp(d, k): return float(d.get(k) or 0)
                price_obj = full.get("price") or {}
                
                p_old = gp(full, "old_price") or gp(price_obj, "old_price")
                p_sell = gp(full, "price") or gp(price_obj, "price")
                p_mkt = gp(full, "marketing_price") or gp(price_obj, "marketing_price")
                
                if p_mkt == 0: p_mkt = p_sell
                if p_old == 0: p_old = p_sell

                items.append([
                    primary, ozon_id, offer_id, brand, cat, name,
                    p_old, p_sell, p_mkt, p_mkt
                ])
            
            last_id = data[-1].get("product_id")
            if len(data) < 500: break
            
        except: break
            
    return items

# --- OZON STOCK ---
def fetch_stocks(cid, key):
    items = []
    try:
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(cid, key), json={"limit": 1000, "offset":0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                sku = str(row.get("sku", ""))
                offer_id = row.get("item_code") or sku
                
                for wh in row.get("warehouses", []):
                    name = wh.get("warehouse_name", "Склад")
                    qty = wh.get("item_cnt", 0) 
                    if qty > 0:
                        items.append([name, offer_id, qty])
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
            payload = { 
                "filter": { "since": since_dt, "to": to_dt }, 
                "limit": 1000, 
                "page": page,
                "with": { "analytics_data": True, "financial_data": True }
            }
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: 
                send_to_gas({"type": "LOG", "msg": f"OZ Sales Err: {r.status_code}"})
                break
            
            res = r.json().get("result", [])
            if not res: break
            
            for p in res:
                created = p.get("created_at")
                if not created: continue
                
                status_raw = str(p.get("status", "")).lower()
                typ = "Отмена" if "cancelled" in status_raw else "Продажа"
                
                analytics = p.get("analytics_data") or {}
                wh_from = analytics.get("warehouse_name", "FBO")
                wh_to = analytics.get("region", "RU")
                
                financial = p.get("financial_data") or {}
                fin_prods = {x.get('product_id'): x for x in financial.get('products', [])}

                for prod in p.get("products", []):
                    sku = prod.get("sku")
                    fin = fin_prods.get(sku, {})
                    price = float(fin.get('client_price') or prod.get('price') or 0)
                    
                    items.append([
                        created[:10], typ, prod.get("offer_id"), str(sku), 
                        1, price, wh_from, wh_to
                    ])
            if len(res) < 1000: break
            page += 1
            time.sleep(0.3)
        except: break
    return items

@app.route("/")
def health(): return "Ozon v104 OK", 200

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
            send_to_gas({"type": "DATA", "sheetName": target, "rows": rows})
            return jsonify({"status": "ok", "count": len(rows)}), 200
        return jsonify({"status": "empty"}), 200
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)