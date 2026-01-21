import os
import time
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL", "")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"

# --- ЗАГОЛОВКИ ---
def get_headers(cid, key):
    return {
        "Client-Id": str(cid).strip(), 
        "Api-Key": str(key).strip(),
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Connection": "keep-alive"
    }

def send_to_gas(payload):
    if not GAS_WEBAPP_URL: return
    payload["secret"] = SECRET_KEY
    try: requests.post(GAS_WEBAPP_URL, json=payload, timeout=10)
    except: pass

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def get_brand_from_attributes(attributes):
    # Ищем атрибут с id 85 (Бренд) или 31 (иногда встречается)
    for attr in attributes:
        if attr.get("attribute_id") == 85 or attr.get("attribute_id") == 31:
            vals = attr.get("values", [])
            if vals:
                return vals[0].get("value", "")
    return "Не указан"

# --- OZON CARDS (ТОВАРЫ) ---
def fetch_cards(cid, key):
    items = []
    url_list = "https://api-seller.ozon.ru/v2/product/list"
    url_info = "https://api-seller.ozon.ru/v2/product/info/list"
    last_id = ""
    
    while True:
        try:
            # 1. Получаем список ID товаров
            payload = { 
                "filter": { "visibility": "ALL" }, 
                "limit": 100 
            }
            if last_id: payload["last_id"] = last_id
            
            r = requests.post(url_list, headers=get_headers(cid, key), json=payload)
            
            if r.status_code != 200: 
                send_to_gas({"type": "LOG", "msg": f"OZ Cards List Error: {r.status_code} {r.text[:100]}"})
                break
            
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            # 2. Получаем детали (цены, названия, бренды)
            ids = [i.get("product_id") for i in data]
            r_info = requests.post(url_info, headers=get_headers(cid, key), json={"product_id": ids})
            
            info_map = {}
            if r_info.status_code == 200:
                for i in r_info.json().get("result", {}).get("items", []):
                    info_map[i.get("id")] = i
            else:
                 send_to_gas({"type": "LOG", "msg": f"OZ Info Error: {r_info.status_code}"})
            
            # 3. Собираем строку
            for item_base in data:
                pid = item_base.get("product_id")
                full = info_map.get(pid, {})
                
                # Поля
                offer_id = full.get("offer_id") or item_base.get("offer_id") or ""
                ozon_id = str(pid)
                name = full.get("name") or "Товар"
                
                # Категория (пытаемся взять имя, если есть, иначе ID)
                cat_obj = full.get("category_id") or ""
                cat = str(cat_obj) # В API v2 это часто просто ID, название получить сложно без огромного справочника
                
                # Фото
                primary = full.get("primary_image") or ""
                if not primary and full.get("images"): primary = full["images"][0]
                
                # Бренд (парсим атрибуты)
                brand = get_brand_from_attributes(full.get("attributes", []))
                
                # Цены
                def get_p(d, k): return float(d.get(k) or 0)
                price_obj = full.get("price") or {}
                
                # Цена до скидки (old_price)
                p_old = get_p(full, "old_price") or get_p(price_obj, "old_price")
                # Цена продавца (price - ваша цена)
                p_sell = get_p(full, "price") or get_p(price_obj, "price")
                # Цена для покупателя (marketing_price или price)
                p_mkt = get_p(full, "marketing_price") or get_p(price_obj, "marketing_price")
                
                if p_old == 0: p_old = p_sell
                if p_mkt == 0: p_mkt = p_sell
                
                # Цена Ozon Карта (обычно равна marketing_price, если она ниже обычной)
                p_card = p_mkt 

                # СТРУКТУРА: [Фото, Арт.OZ, Арт.Наш, Бренд, Категория, Название, Ц.База, Ц.Прод, Ц.Покуп, Ц.Карта]
                items.append([
                    primary, 
                    ozon_id, 
                    offer_id, 
                    brand, 
                    cat, 
                    name,
                    p_old,   # Ц.База (до скидок)
                    p_sell,  # Ц.Продавца (ваша)
                    p_mkt,   # Ц.Покупателя (фактическая)
                    p_card   # Ц.OZКарта
                ])
            
            last_id = data[-1].get("product_id")
            if len(data) < 100: break
            
        except Exception as e:
            send_to_gas({"type": "LOG", "msg": f"OZ Cards Exception: {str(e)}"})
            break
            
    return items

# --- OZON STOCK (ОСТАТКИ) ---
def fetch_stocks(cid, key):
    items = []
    # Используем v2, он наиболее стабилен для FBO
    url_stock = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    
    try:
        r = requests.post(url_stock, headers=get_headers(cid, key), json={"limit": 1000, "offset":0})
        if r.status_code != 200:
             send_to_gas({"type": "LOG", "msg": f"OZ Stock Err: {r.status_code}"})
             return []

        rows = r.json().get("result", {}).get("rows", [])
        for row in rows:
            sku = str(row.get("sku", ""))
            offer_id = row.get("item_code") # Артикул селлера
            if not offer_id: offer_id = sku

            warehouses = row.get("warehouses", [])
            for wh in warehouses:
                name = wh.get("warehouse_name", "Склад")
                qty = wh.get("item_cnt", 0) # FBO доступно
                
                if qty > 0:
                    # СТРУКТУРА: [Склад, Артикул, Остаток]
                    items.append([name, offer_id, qty])
    except Exception as e:
         send_to_gas({"type": "LOG", "msg": f"OZ Stock Exception: {str(e)}"})
    return items

# --- OZON SALES (ПРОДАЖИ FBO) ---
def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    # Формат дат для Ozon
    since_dt = f"{d_from}T00:00:00Z"
    to_dt = f"{d_to}T23:59:59Z"
    
    url_sales = "https://api-seller.ozon.ru/v2/posting/fbo/list"

    while True:
        try:
            payload = { 
                "filter": { "since": since_dt, "to": to_dt }, 
                "limit": 1000, 
                "page": page,
                "with": { "analytics_data": True, "financial_data": True }
            }
            r = requests.post(url_sales, headers=get_headers(cid, key), json=payload)
            if r.status_code != 200: 
                send_to_gas({"type": "LOG", "msg": f"OZ Sales {r.status_code} {r.text[:50]}"})
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
                
                # Финансы
                financial = p.get("financial_data") or {}
                fin_prods = {x.get('product_id'): x for x in financial.get('products', [])}

                for prod in p.get("products", []):
                    sku = prod.get("sku")
                    offer_id = prod.get("offer_id")
                    
                    # Пытаемся найти фактическую цену оплаты клиентом (client_price)
                    fin_item = fin_prods.get(sku, {})
                    price = float(fin_item.get('client_price') or prod.get('price') or 0)
                    
                    # СТРУКТУРА: [Дата, Тип, Арт.Наш, Арт.OZ, Кол, Цена, Склад Отгр., Склад Дост.]
                    # Дата обрезается до YYYY-MM-DD
                    items.append([
                        created[:10], 
                        typ, 
                        offer_id, 
                        str(sku), 
                        1, 
                        price, 
                        wh_from, 
                        wh_to
                    ])
            
            if len(res) < 1000: break
            page += 1
            time.sleep(0.3) # Анти-спам
        except Exception as e:
             send_to_gas({"type": "LOG", "msg": f"OZ Sales Exception: {str(e)}"})
             break
    return items

@app.route("/")
def health(): return "Ozon v105 Fixed OK", 200

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
            
        if rows:
            send_to_gas({"type": "DATA", "sheetName": target, "rows": rows})
            return jsonify({"status": "ok", "count": len(rows)}), 200
        else:
            # Если пустой список, все равно шлем пустой ответ, чтобы GAS знал, что задача завершена
            return jsonify({"status": "empty"}), 200
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)