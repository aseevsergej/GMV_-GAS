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
        "Content-Type": "application/json"
    }

def send_to_gas(payload):
    if not GAS_WEBAPP_URL: return
    payload["secret"] = SECRET_KEY
    try: requests.post(GAS_WEBAPP_URL, json=payload, timeout=10)
    except: pass

# --- OZON CARDS (FIXED MAPPING) ---
def fetch_cards(cid, key):
    items = []
    # Используем v2, он самый стабильный для получения списка ID
    url_list = "https://api-seller.ozon.ru/v2/product/list"
    url_info = "https://api-seller.ozon.ru/v2/product/info/list"
    
    last_id = ""
    
    while True:
        try:
            # 1. Получаем список ID
            payload = { "filter": { "visibility": "ALL" }, "limit": 500 } # Уменьшил лимит для стабильности
            if last_id: payload["last_id"] = last_id
            
            r = requests.post(url_list, headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: break
            
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            # 2. Получаем детали (Info)
            ids = [i["product_id"] for i in data]
            r_info = requests.post(url_info, headers=get_headers(cid, key), json={"product_id": ids})
            
            info_map = {}
            if r_info.status_code == 200:
                info_items = r_info.json().get("result", {}).get("items", [])
                for i in info_items:
                    # Сохраняем по product_id (число)
                    info_map[i.get("id")] = i
            
            # 3. Собираем строку
            for item_base in data:
                pid = item_base["product_id"]
                full = info_map.get(pid, {})
                
                # Данные
                offer_id = full.get("offer_id") or item_base.get("offer_id") or ""
                name = full.get("name") or f"Товар {offer_id}"
                cat = full.get("category_id") or ""
                
                # Фото
                images = full.get("images", [])
                primary = full.get("primary_image") or (images[0] if images else "")
                
                # Цены (глубокий поиск)
                price_obj = full.get("price") or {}
                # Ozon часто меняет структуру цены
                p = float(price_obj.get("price") or full.get("old_price") or 0)
                mp = float(price_obj.get("marketing_price") or full.get("price") or p)
                
                # Остатки (FBS + FBO summary)
                stocks_obj = full.get("stocks") or {}
                st = stocks_obj.get("present", 0)
                
                # Бренд (ищем в атрибутах, так как прямого поля brand может не быть)
                brand = "No Brand"
                # Ozon не всегда отдает бренд в info/list, берем заглушку или парсим name
                
                # [Фото, nmID, АртOZ, АртПрод, Бренд, Кат, Назв, Ц.База, Ц.Прод, Ц.СПП, Ост]
                items.append([
                    primary, 
                    str(pid), str(pid), offer_id,
                    brand, str(cat), name, 
                    p, mp, mp, st
                ])
            
            last_id = data[-1]["product_id"]
            if len(data) < 500: break
            
        except Exception as e:
            send_to_gas({"type": "LOG", "msg": f"Cards Crash: {e}"})
            break
            
    return items

# --- OZON STOCK (FIXED WAREHOUSES) ---
def fetch_stocks(cid, key):
    items = []
    try:
        # Лимит 1000, offset 0
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(cid, key), json={"limit": 1000, "offset":0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                sku = str(row.get("sku"))
                item_code = row.get("item_code")
                item_name = row.get("item_name", "")
                
                warehouses = row.get("warehouses", [])
                if not warehouses:
                    continue
                
                for wh in warehouses:
                    wh_name = wh.get("warehouse_name", "Склад Ozon")
                    # item_cnt = Доступно к продаже, promised = Зарезервировано
                    qty = wh.get("item_cnt", 0)
                    
                    if qty > 0:
                        # [Склад, Арт, Ост, Путь, nmID]
                        items.append([wh_name, item_code, qty, 0, sku])
    except: pass
    return items

# --- OZON SALES (FIXED WAREHOUSES) ---
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
                "with": { "analytics_data": True } # Важно! Запрашиваем аналитику
            }
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=get_headers(cid, key), json=payload)
            
            if r.status_code != 200: break
            res = r.json().get("result", [])
            if not res: break
            
            for p in res:
                created = p.get("created_at")
                if not created: continue
                
                status = "Отмена" if "cancelled" in (p.get("status") or "").lower() else "Заказ"
                
                # Данные склада
                analytics = p.get("analytics_data") or {}
                wh = analytics.get("warehouse_name") or analytics.get("warehouse") or "Склад Ozon"
                reg = analytics.get("region", "RU")
                num = p.get("posting_number", "")

                products = p.get("products") or []
                for prod in products:
                    price = float(prod.get("price", 0))
                    # [Дата, Время, Тип, Арт, nmID, Кол, Ц.Розн, Ц.Прод, Ц.Факт, СПП, Склад, Регион, №]
                    items.append([
                        created[:10], created[11:16], status, 
                        prod.get("offer_id"), str(prod.get("sku")), 
                        1, price, price, price, 0, wh, reg, num
                    ])
            
            if len(res) < 1000: break
            page += 1
            time.sleep(0.3)
        except: break
    return items

@app.route("/")
def health(): return "Ozon v94 OK", 200

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