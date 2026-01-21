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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
    }

def send_to_gas(payload):
    if not GAS_WEBAPP_URL: return
    payload["secret"] = SECRET_KEY
    try: requests.post(GAS_WEBAPP_URL, json=payload, timeout=10)
    except: pass

# --- OZON CARDS (Universal Loader) ---
def fetch_cards(cid, key):
    items = []
    
    # Список методов по приоритету (v3 самый новый)
    endpoints = [
        "https://api-seller.ozon.ru/v3/product/list",
        "https://api-seller.ozon.ru/v2/product/list",
        "https://api-seller.ozon.ru/v1/product/list"
    ]
    
    url_list = ""
    # 1. Ищем рабочий метод
    for ep in endpoints:
        try:
            r = requests.post(ep, headers=get_headers(cid, key), json={"limit": 1})
            if r.status_code == 200:
                url_list = ep
                break
        except: pass
        
    if not url_list:
        send_to_gas({"type": "LOG", "msg": f"OZ {cid}: All API methods failed (404/403)"})
        return []

    # 2. Скачиваем
    last_id = ""
    while True:
        try:
            payload = { "filter": { "visibility": "ALL" }, "limit": 300 }
            if last_id: payload["last_id"] = last_id
            
            r = requests.post(url_list, headers=get_headers(cid, key), json=payload)
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            # Получаем детали (Info всегда v2)
            ids = [i.get("product_id") or i.get("id") for i in data] # v3 returns product_id, v1 returns id
            
            r_info = requests.post("https://api-seller.ozon.ru/v2/product/info/list", headers=get_headers(cid, key), json={"product_id": ids})
            info_map = {}
            if r_info.status_code == 200:
                for i in r_info.json().get("result", {}).get("items", []):
                    info_map[i.get("id")] = i
            
            for item_base in data:
                pid = item_base.get("product_id") or item_base.get("id")
                full = info_map.get(pid, {})
                
                # Маппинг данных
                photo = full.get("primary_image") or ""
                if not photo and full.get("images"): photo = full["images"][0]
                
                # Артикулы
                ozon_id = str(pid)
                offer_id = full.get("offer_id") or item_base.get("offer_id") or ""
                
                # Бренд (из имени или атрибутов, заглушка OZON если пусто)
                name = full.get("name") or ""
                cat = str(full.get("category_id", ""))
                brand = "OZON" 
                
                # Цены
                def to_f(v): 
                    try: return float(v)
                    except: return 0.0
                
                p_old = to_f(full.get("old_price") or full.get("price",{}).get("price"))
                p_sell = to_f(full.get("price")) # Цена продавца
                p_mkt = to_f(full.get("marketing_price")) # Цена для покупателя
                if p_mkt == 0: p_mkt = p_sell
                
                # [Фото, Арт.OZ, Арт.Наш, Бренд, Категория, Название, Ц.ДоСкидки, Ц.Продавца, Ц.Покупателя, Ц.OZКарта]
                items.append([
                    photo, ozon_id, offer_id, brand, cat, name,
                    p_old, p_sell, p_mkt, p_mkt
                ])
            
            last_id = data[-1].get("product_id") or data[-1].get("id")
            if len(data) < 300: break
            
        except Exception as e:
            send_to_gas({"type": "LOG", "msg": f"Cards error: {str(e)[:50]}"})
            break
            
    return items

# --- OZON STOCK ---
def fetch_stocks(cid, key):
    items = []
    try:
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(cid, key), json={"limit": 1000, "offset":0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                offer_id = row.get("item_code")
                for wh in row.get("warehouses", []):
                    name = wh.get("warehouse_name", "OZON")
                    qty = wh.get("item_cnt", 0)
                    if qty > 0:
                        # [Склад, Артикул, Остаток]
                        items.append([name, offer_id, qty])
    except: pass
    return items

# --- OZON SALES ---
def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    # Даты
    since_dt = f"{d_from}T00:00:00Z"
    to_dt = f"{d_to}T23:59:59Z"

    while True:
        try:
            payload = { 
                "filter": { "since": since_dt, "to": to_dt }, 
                "limit": 1000, 
                "page": page,
                "with": { "analytics_data": True }
            }
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: break
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
                
                for prod in p.get("products", []):
                    price = float(prod.get("price", 0))
                    # [Дата, Тип, Арт.Наш, Арт.OZ, Кол, Цена, Склад Отгр., Склад Дост.]
                    items.append([
                        created[:10], typ, prod.get("offer_id"), str(prod.get("sku")), 
                        1, price, wh_from, wh_to
                    ])
            if len(res) < 1000: break
            page += 1
            time.sleep(0.3)
        except: break
    return items

@app.route("/")
def health(): return "Ozon v100 Ready", 200

@app.route("/sync", methods=['POST'])
def sync():
    try:
        data = request.json
        cid = data.get("clientId")
        key = data.get("apiKey")
        mode = data.get("mode")
        
        if not cid or not key: return jsonify({"error": "Keys missing"}), 400

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