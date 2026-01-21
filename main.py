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

# --- OZON CARDS ---
# Формат вывода: [Фото, Арт.OZ, Арт.Наш, Бренд, Категория, Название, Ц.ДоСкидки, Ц.Продавца, Ц.Покупателя, Ц.OZКарта]
def fetch_cards(cid, key):
    items = []
    
    # 1. Получаем список ID
    url_list = "https://api-seller.ozon.ru/v2/product/list"
    url_info = "https://api-seller.ozon.ru/v2/product/info/list"
    last_id = ""
    
    while True:
        try:
            payload = { "filter": { "visibility": "ALL" }, "limit": 200 }
            if last_id: payload["last_id"] = last_id
            
            r = requests.post(url_list, headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: break
            
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            # 2. Получаем детали
            ids = [i["product_id"] for i in data]
            r_info = requests.post(url_info, headers=get_headers(cid, key), json={"product_id": ids})
            
            info_map = {}
            if r_info.status_code == 200:
                for i in r_info.json().get("result", {}).get("items", []):
                    info_map[i.get("id")] = i
            
            for item_base in data:
                pid = item_base["product_id"]
                full = info_map.get(pid, {})
                
                # Данные
                offer_id = full.get("offer_id") or item_base.get("offer_id") or ""
                ozon_id = str(pid)
                name = full.get("name") or ""
                cat = str(full.get("category_id", ""))
                
                # Фото
                primary = full.get("primary_image") or ""
                if not primary and full.get("images"): primary = full["images"][0]
                
                # Бренд (из атрибутов сложно, берем заглушку если нет явного поля)
                brand = "OZON" # API v2 product/info часто не отдает бренд в корне
                
                # Цены (безопасное приведение)
                def get_p(val):
                    try: return float(val) if val else 0.0
                    except: return 0.0

                p_old = get_p(full.get("old_price"))
                p_sell = get_p(full.get("price"))
                p_mkt = get_p(full.get("marketing_price"))
                if p_mkt == 0: p_mkt = p_sell
                
                items.append([
                    primary, ozon_id, offer_id, brand, cat, name,
                    p_old, p_sell, p_mkt, p_mkt
                ])
            
            last_id = data[-1]["product_id"]
            if len(data) < 200: break
            
        except Exception as e:
            send_to_gas({"type": "LOG", "msg": f"Cards Err: {str(e)[:50]}"})
            break
            
    return items

# --- OZON STOCK ---
# Формат вывода: [Склад, Артикул, Остаток]
def fetch_stocks(cid, key):
    items = []
    try:
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(cid, key), json={"limit": 1000, "offset":0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                offer_id = row.get("item_code") # Наш артикул
                warehouses = row.get("warehouses", [])
                
                for wh in warehouses:
                    name = wh.get("warehouse_name", "Склад")
                    qty = wh.get("item_cnt", 0) # Доступно к продаже
                    if qty > 0:
                        items.append([name, offer_id, qty])
    except: pass
    return items

# --- OZON SALES ---
# Формат вывода: [Дата, Тип, Арт.Наш, ��рт.OZ, Кол, Цена, Склад Отгр., Склад Дост.]
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
                "with": { "analytics_data": True }
            }
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: break
            
            res = r.json().get("result", [])
            if not res: break
            
            for p in res:
                created = p.get("created_at")
                if not created: continue
                
                # Статус
                status_raw = str(p.get("status", "")).lower()
                typ = "Отмена" if "cancelled" in status_raw else "Продажа"
                
                # Склады
                analytics = p.get("analytics_data") or {}
                wh_from = analytics.get("warehouse_name", "FBO")
                wh_to = analytics.get("region", "RU")
                
                for prod in p.get("products", []):
                    price = float(prod.get("price", 0))
                    items.append([
                        created[:10], # Дата YYYY-MM-DD
                        typ,          # Тип
                        prod.get("offer_id"), # Арт.Наш
                        str(prod.get("sku")), # Арт.OZ
                        1,            # Кол
                        price,        # Цена
                        wh_from,      # Склад Отгр
                        wh_to         # Склад Дост
                    ])
            
            if len(res) < 1000: break
            page += 1
            time.sleep(0.3)
        except: break
    return items

@app.route("/")
def health(): return "Ozon v99 OK", 200

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