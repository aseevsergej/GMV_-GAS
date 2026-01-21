import os
import time
import requests
import json
from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL", "")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"
START_TIME = time.time() # Время старта сервера

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

# --- OZON CARDS (Товары) ---
def fetch_cards(cid, key):
    items = []
    url_list = "https://api-seller.ozon.ru/v2/product/list"
    url_info = "https://api-seller.ozon.ru/v2/product/info/list"
    last_id = ""
    
    while True:
        try:
            # 1. Список ID
            payload = { "filter": { "visibility": "ALL" }, "limit": 200 }
            if last_id: payload["last_id"] = last_id
            
            r = requests.post(url_list, headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: break
            
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            # 2. Детали
            ids = [i["product_id"] for i in data]
            r_info = requests.post(url_info, headers=get_headers(cid, key), json={"product_id": ids})
            
            info_map = {}
            if r_info.status_code == 200:
                for i in r_info.json().get("result", {}).get("items", []):
                    info_map[i.get("id")] = i
            
            for item_base in data:
                pid = item_base["product_id"]
                full = info_map.get(pid, {})
                
                offer_id = full.get("offer_id") or item_base.get("offer_id") or ""
                name = full.get("name") or "Без названия"
                cat_id = full.get("category_id") or ""
                
                # Фото
                images = full.get("images", [])
                primary_img = full.get("primary_image") or (images[0] if images else "")
                
                # Бренд (Пытаемся найти в атрибутах)
                # Ozon передает атрибуты сложно, но часто "brand" это просто текст
                # Если нет, ставим Ozon, чтобы не ломать логику
                brand = "No Brand" 
                # (Тут можно добавить сложный парсинг атрибутов, но пока берем заглушку если нет явного поля)
                
                # Цены
                # Цена до скидки (old_price)
                p_old = float(full.get("old_price") or full.get("price") or 0)
                # Цена с учетом скидок продавца (marketing_price или price)
                p_marketing = float(full.get("marketing_price") or full.get("price") or 0)
                # Цена для покупателя (часто совпадает с marketing, если нет Ozon карты)
                p_final = p_marketing
                
                stocks = full.get("stocks", {}).get("present", 0)
                
                # Порядок для GAS:
                # [Фото, Арт.OZ (nmID), Арт.OZ, Арт.Наш, Бренд, Категория, Название, Цена До, Цена ЛК, Цена Клиент, Остаток]
                items.append([
                    primary_img, 
                    str(pid), 
                    str(pid), 
                    offer_id,
                    brand, 
                    str(cat_id), 
                    name, 
                    p_old, 
                    p_marketing, 
                    p_final, 
                    stocks
                ])
            
            last_id = data[-1]["product_id"]
            if len(data) < 200: break
            
        except Exception as e:
            send_to_gas({"type": "LOG", "msg": f"Cards err: {e}"})
            break
            
    return items

# --- OZON STOCK (Остатки) ---
def fetch_stocks(cid, key):
    items = []
    try:
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(cid, key), json={"limit": 1000, "offset":0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                sku = str(row.get("sku"))
                # item_code = row.get("item_code") # Наш артикул
                
                warehouses = row.get("warehouses", [])
                for wh in warehouses:
                    wh_name = wh.get("warehouse_name", "Склад Ozon")
                    qty = wh.get("item_cnt", 0) # Фактический остаток доступный к продаже
                    
                    if qty > 0:
                        # [Склад, Арт.OZ, Остаток, Путь(0), nmID(Арт.OZ)] - формат как у WB для унификации
                        items.append([wh_name, sku, qty, 0, sku])
    except: pass
    return items

# --- OZON SALES (Продажи) ---
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
            
            if r.status_code != 200: break
            res = r.json().get("result", [])
            if not res: break
            
            for p in res:
                created = p.get("created_at")
                if not created: continue
                
                # Тип: Заказ или Отмена
                status_raw = (p.get("status") or "").lower()
                op_type = "Отмена" if "cancelled" in status_raw else "Заказ"
                
                # Склады
                analytics = p.get("analytics_data") or {}
                wh_from = analytics.get("warehouse_name") or analytics.get("warehouse") or "Склад Ozon"
                wh_to = analytics.get("region") or analytics.get("city") or "RU" # Склад доставки/Регион
                
                posting_number = p.get("posting_number", "")

                products = p.get("products") or []
                financial = p.get("financial_data") or {}
                
                for prod in products:
                    # Цена продажи (сколько платит покупатель)
                    price = float(prod.get("price", 0))
                    
                    # [Дата, Время, Тип, Арт.Наш, Арт.OZ, Кол, Ц.Прод, Ц.Прод, Ц.Прод, СПП(0), Склад Отгр, Склад Дост, Номер]
                    items.append([
                        created[:10], created[11:16], 
                        op_type, 
                        prod.get("offer_id"), 
                        str(prod.get("sku")), 
                        1, 
                        price, price, price, 0, 
                        wh_from, wh_to, 
                        posting_number
                    ])
            
            if len(res) < 1000: break
            page += 1
            time.sleep(0.3)
        except: break
    return items

@app.route("/")
def health(): 
    uptime = time.time() - START_TIME
    return jsonify({"status": "Ozon v95 OK", "uptime_sec": int(uptime)}), 200

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