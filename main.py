import os
import time
import requests
import json
from datetime import datetime
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

# --- OZON PRODUCTS (ТОВАРЫ) ---
def fetch_cards(cid, key):
    items = []
    print(f"📦 Start Cards {cid}...")
    
    url_list = "https://api-seller.ozon.ru/v2/product/list"
    url_info = "https://api-seller.ozon.ru/v2/product/info/list"
    last_id = ""
    
    while True:
        try:
            # 1. Список ID
            payload = { "filter": { "visibility": "ALL" }, "limit": 500 }
            if last_id: payload["last_id"] = last_id
            
            r = requests.post(url_list, headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: 
                send_to_gas({"type": "LOG", "msg": f"OZ Cards Err: {r.status_code}"})
                break
            
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            ids = [i["product_id"] for i in data]
            
            # 2. Детали (Info)
            r_info = requests.post(url_info, headers=get_headers(cid, key), json={"product_id": ids})
            info_map = {}
            if r_info.status_code == 200:
                for i in r_info.json().get("result", {}).get("items", []):
                    info_map[i.get("id")] = i
            
            for item_base in data:
                pid = item_base["product_id"]
                full = info_map.get(pid, {})
                
                # Поля по ТЗ
                photo = full.get("primary_image") or ""
                if not photo and full.get("images"): photo = full["images"][0]
                
                ozon_id = str(pid) # Артикул Ozon
                vendor_code = full.get("offer_id") or "" # Наш Артикул
                
                # Бренд - Ozon часто не отдает его явно, пробуем найти
                brand = "Не указан"
                # Обычно бренд лежит в attributes, но это сложный запрос. Пока берем заглушку или из названия.
                
                cat = full.get("category_id") or ""
                name = full.get("name") or ""
                
                # Цены
                # Цена до скидки (old_price)
                price_old = float(full.get("old_price") or full.get("price", {}).get("price") or 0)
                # Цена продавца (после скидки селлера)
                price_seller = float(full.get("price") or 0)
                # Цена для покупателя (маркетинговая)
                price_marketing = float(full.get("marketing_price") or price_seller)
                
                # Цена по Ozon карте (обычно ~ маркетинговая, точнее только через отчеты)
                price_ozon_card = price_marketing 

                # Структура для GAS: 
                # [Фото, Арт.OZ, Арт.Наш, Бренд, Категория, Название, Ц.ДоСкидки, Ц.Селлера, Ц.Покупателя, Ц.OzonКарта]
                items.append([
                    photo, ozon_id, vendor_code, brand, str(cat), name,
                    price_old, price_seller, price_marketing, price_ozon_card
                ])
            
            last_id = data[-1]["product_id"]
            if len(data) < 500: break
            
        except Exception as e:
            send_to_gas({"type": "LOG", "msg": f"Cards Crash: {e}"})
            break
            
    return items

# --- OZON STOCKS (ОСТАТКИ ПО СКЛАДАМ) ---
def fetch_stocks(cid, key):
    items = []
    try:
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(cid, key), json={"limit": 1000, "offset":0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                vendor_code = row.get("item_code") # Наш артикул
                
                warehouses = row.get("warehouses", [])
                if not warehouses: continue
                
                for wh in warehouses:
                    # ТЗ: Склад (где находится), Артикул, Остаток
                    wh_name = wh.get("warehouse_name", "Склад Ozon")
                    qty = wh.get("item_cnt", 0) # Фактический остаток
                    
                    if qty > 0:
                        items.append([wh_name, vendor_code, qty])
    except: pass
    return items

# --- OZON SALES (ПРОДАЖИ) ---
def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    # Формат дат RFC3339
    since_dt = f"{d_from}T00:00:00Z"
    to_dt = f"{d_to}T23:59:59Z"

    while True:
        try:
            payload = { 
                "filter": { "since": since_dt, "to": to_dt }, 
                "limit": 1000, 
                "page": page,
                "with": { "analytics_data": True, "financial_data": True } # Важно для цен и кластеров
            }
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=get_headers(cid, key), json=payload)
            
            if r.status_code != 200: break
            res = r.json().get("result", [])
            if not res: break
            
            for p in res:
                created = p.get("created_at") or "2000-01-01T00:00:00Z"
                status_raw = p.get("status") or ""
                
                # Тип: Продажа или Отмена
                typ = "Отмена" if "cancelled" in status_raw.lower() else "Продажа"
                
                analytics = p.get("analytics_data") or {}
                financial = p.get("financial_data") or {}
                
                # Склады
                wh_ship = analytics.get("warehouse_name") or "Ozon FBO" # Склад отгрузки
                wh_deliv = analytics.get("region") or "RF" # Склад доставки (обычно регион/кластер)
                
                products = p.get("products") or []
                financial_prods = financial.get("products") or []
                
                # Создаем мапу финансовых данных по product_id (sku)
                fin_map = {fp.get("product_id"): fp for fp in financial_prods}

                for prod in products:
                    sku = prod.get("sku")
                    fin_data = fin_map.get(sku) or {}
                    
                    # Цена продажи (сколько оплатил покупатель)
                    # Если есть client_price - берем её, иначе price
                    sale_price = float(fin_data.get("client_price") or prod.get("price") or 0)
                    
                    # ТЗ: Дата, Тип, Арт(Наш), Арт(Ozon), Кол-во, Цена, СкладОтгр, СкладДост
                    items.append([
                        created[:10], # Дата
                        typ,          # Тип
                        prod.get("offer_id"), # Арт Наш
                        str(sku),     # Арт Ozon
                        1,            # Кол-во
                        sale_price,   # Цена продажи
                        wh_ship,      # Склад отгрузки
                        wh_deliv      # Склад доставки (Регион)
                    ])
            
            if len(res) < 1000: break
            page += 1
            time.sleep(0.3)
        except: break
    return items

@app.route("/")
def health(): return "Ozon v95 OK", 200

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