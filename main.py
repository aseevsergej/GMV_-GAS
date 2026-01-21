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

# --- 1. OZON ТОВАРЫ (Детальные) ---
def fetch_cards(cid, key):
    items = []
    # Сначала получаем список ID
    url_list = "https://api-seller.ozon.ru/v2/product/list"
    url_info = "https://api-seller.ozon.ru/v2/product/info/list"
    
    last_id = ""
    while True:
        try:
            payload = { "filter": { "visibility": "ALL" }, "limit": 500 }
            if last_id: payload["last_id"] = last_id
            
            r = requests.post(url_list, headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: break
            
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            # Получаем детали
            ids = [i["product_id"] for i in data]
            r_info = requests.post(url_info, headers=get_headers(cid, key), json={"product_id": ids})
            
            info_map = {}
            if r_info.status_code == 200:
                for i in r_info.json().get("result", {}).get("items", []):
                    info_map[i["id"]] = i
            
            for base in data:
                pid = base["product_id"]
                full = info_map.get(pid, {})
                
                # Поля
                photo = full.get("primary_image") or ""
                ozon_id = str(full.get("sku") or full.get("fbo_sku") or pid)
                art_seller = full.get("offer_id") or ""
                
                # Цены
                p_obj = full.get("price", {})
                price_before = float(p_obj.get("old_price") or 0)
                price_seller = float(p_obj.get("marketing_price") or 0) 
                if price_seller == 0: price_seller = float(p_obj.get("price") or 0)
                price_client = float(p_obj.get("price") or 0) # Цена продажи текущая
                
                # Бренд и Категория (грязный хак, но работает лучше атрибутов v3)
                brand = "Ozon Brand" # Дефолт
                cat_name = "Category"
                # Попытка найти в name или атрибутах (сложно без v3/attributes, но берем что есть)
                # В v2/product/info бренд часто не приходит явно, берем из на��вания или заглушку,
                # если нужно 100% точность, нужен отдельный запрос v3/products/info/attributes (очень тяжелый).
                # Пока берем категорию ID
                cat_id = full.get("category_id")
                
                name = full.get("name") or art_seller
                
                # Заголовки: [Фото, Арт.OZ, Арт.Наш, Бренд, Категория, Название, Ц.ДоСкидки, Ц.Селлер, Ц.Покуп, Ц.ОзонКарта]
                items.append([
                    photo, ozon_id, art_seller, 
                    brand, str(cat_id), name, 
                    price_before, price_seller, price_client, price_client 
                ])
                
            last_id = data[-1]["product_id"]
            if len(data) < 500: break
        except: break
    return items

# --- 2. OZON ПРОДАЖИ (Детальные) ---
def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    # Формат дат
    since = f"{d_from}T00:00:00Z"
    to_d = f"{d_to}T23:59:59Z"
    
    while True:
        try:
            payload = { 
                "filter": { "since": since, "to": to_d }, 
                "limit": 1000, 
                "page": page,
                "with": { "analytics_data": True, "financial_data": True } 
            }
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: break
            
            res = r.json().get("result", [])
            if not res: break
            
            for p in res:
                created = p.get("created_at")[:10]
                status_raw = (p.get("status") or "").lower()
                
                # Тип: Продажа или Отмена
                typ = "Отмена" if "cancelled" in status_raw else "Продажа"
                
                # Склады
                analytics = p.get("analytics_data") or {}
                wh_from = analytics.get("warehouse_name") or "Ozon Склад"
                wh_to = analytics.get("region") or "RF" # Точный склад доставки Ozon не отдает по FBO, только регион
                
                # Финансы
                fin = p.get("financial_data") or {}
                products_fin = fin.get("products") or []
                prod_fin_map = {x["product_id"]: x for x in products_fin}

                for prod in p.get("products", []):
                    pid = prod.get("sku")
                    art_oz = str(pid)
                    art_sell = prod.get("offer_id")
                    
                    # Цена продажи (сколько оплатил покупатель)
                    # Ищем в финансовых данных, иначе берем из товара
                    f_data = prod_fin_map.get(pid, {})
                    price_sale = float(f_data.get("price") or prod.get("price") or 0)
                    
                    # Заголовки: [Дата, Тип, Арт.Наш, Арт.OZ, Кол-во, ЦенаПродажи, СкладОтпр, СкладДост]
                    items.append([
                        created, typ, art_sell, art_oz, 1, 
                        price_sale, wh_from, wh_to
                    ])
            
            if len(res) < 1000: break
            page += 1
            time.sleep(0.3)
        except: break
    return items

# --- 3. OZON ОСТАТКИ (Склады) ---
def fetch_stocks(cid, key):
    items = []
    try:
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(cid, key), json={"limit": 1000, "offset":0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                art_sell = row.get("item_code")
                # Разбивка по складам
                for wh in row.get("warehouses", []):
                    name = wh.get("warehouse_name", "Склад")
                    qty = wh.get("item_cnt", 0) # Доступный остаток
                    if qty > 0:
                        # [Склад, Арт.Наш, Остаток]
                        items.append([name, art_sell, qty])
    except: pass
    return items

@app.route("/")
def health(): 
    # Эндпоинт для проверки здоровья
    return jsonify({"status": "live", "timestamp": time.time()}), 200

@app.route("/sync", methods=['POST'])
def sync():
    try:
        data = request.json
        cid, key = data.get("clientId"), data.get("apiKey")
        if not cid or not key: return jsonify({"error": "No keys"}), 400
        
        mode = data.get("mode")
        d_from, d_to = data.get("dateFrom"), data.get("dateTo")
        
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
        return jsonify({"status": "empty"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)