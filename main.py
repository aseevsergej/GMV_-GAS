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

# --- OZON CARDS (v3 -> v2) ---
def fetch_cards(cid, key):
    items = []
    # Сначала v3 (новый), если нет - v2
    endpoints = ["https://api-seller.ozon.ru/v3/product/list", "https://api-seller.ozon.ru/v2/product/list"]
    
    last_id = ""
    for url in endpoints:
        has_error = False
        while True:
            payload = { "filter": { "visibility": "ALL" }, "limit": 1000 }
            if last_id: payload["last_id"] = last_id
            
            try:
                r = requests.post(url, headers=get_headers(cid, key), json=payload)
                if r.status_code != 200:
                    has_error = True
                    break
                
                data = r.json().get("result", {}).get("items", [])
                if not data: break
                
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
                    
                    # [Фото, nmID, АртOZ, АртПрод, Бренд, Кат, Назв, Ц.База, Ц.Прод, Ц.СПП, Ост]
                    items.append([
                        full.get("primary_image", ""), str(pid), str(pid), offer_id,
                        "OZON", str(full.get("category_id", "")), full.get("name", f"ID {pid}"), 
                        p, mp, mp, st
                    ])
                
                last_id = data[-1]["product_id"]
                if len(data) < 1000: return items 
                
            except: has_error = True; break
        if items and not has_error: return items
            
    return items

# --- OZON STOCK (Real Warehouses) ---
def fetch_stocks(cid, key):
    items = []
    try:
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(cid, key), json={"limit": 1000, "offset":0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                sku = str(row.get("sku"))
                item_code = row.get("item_code")
                # Разбиваем по складам
                warehouses = row.get("warehouses", [])
                if not warehouses:
                    # Если складов нет, но есть товар (редко)
                    items.append(["FBO Ozon", item_code, 0, 0, sku])
                else:
                    for wh in warehouses:
                        wh_name = wh.get("warehouse_name", "Ozon Wh")
                        # Берем "present" или "item_cnt" (доступное к продаже)
                        qty = wh.get("item_cnt", 0) 
                        if qty > 0:
                            # [Склад, Арт, Ост, Путь, nmID]
                            items.append([wh_name, item_code, qty, 0, sku])
    except: pass
    return items

# --- OZON SALES ---
def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    while True:
        try:
            payload = { "filter": { "since": f"{d_from}T00:00:00Z", "to": f"{d_to}T23:59:59Z" }, "limit": 1000, "page": page }
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: break
            res = r.json().get("result", [])
            if not res or not isinstance(res, list): break
            
            for p in res:
                created = p.get("created_at") or "2000-01-01T00:00:00Z"
                status = "Отмена" if "cancelled" in (p.get("status") or "").lower() else "Заказ"
                
                # Получаем реальное имя склада
                analytics = p.get("analytics_data") or {}
                wh = analytics.get("warehouse_name", "Ozon")
                if not wh: wh = "Ozon"
                
                reg = analytics.get("region", "RU")
                num = p.get("posting_number", "")

                for prod in p.get("products", []):
                    price = float(prod.get("price", 0))
                    # [Дата, Время, Тип, Арт, nmID, Кол, Ц.Розн, Ц.Прод, Ц.Факт, СПП, Склад, Регион, №]
                    items.append([
                        created[:10], created[11:16], status, 
                        prod.get("offer_id"), str(prod.get("sku")), 
                        1, price, price, price, 0, wh, reg, num
                    ])
            if len(res) < 1000: break
            page += 1
            time.sleep(0.2)
        except: break
    return items

@app.route("/")
def health(): return "Ozon v92 OK", 200

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