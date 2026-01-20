import os
import time
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- КОНФИГУРАЦИЯ ---
# Переменные окружения берутся из настроек Render
OZON_CLIENT_ID = os.environ.get("OZON_CLIENT_ID")
OZON_API_KEY = os.environ.get("OZON_API_KEY")
GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"

def get_headers():
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }

def send_to_gas(payload):
    """Отправляет данные или лог в Google Script"""
    if not GAS_WEBAPP_URL: return
    payload["secret"] = SECRET_KEY
    try:
        requests.post(GAS_WEBAPP_URL, json=payload, timeout=10)
    except Exception as e:
        print(f"❌ GAS Error: {e}")

# --- МОДУЛЬ: ТОВАРЫ (CARDS) ---
def fetch_cards():
    items = []
    last_id = ""
    print("📦 Start Cards...")
    
    while True:
        payload = { "filter": { "visibility": "ALL" }, "limit": 100 }
        if last_id: payload["last_id"] = last_id
        
        try:
            r = requests.post("https://api-seller.ozon.ru/v2/product/list", headers=get_headers(), json=payload)
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            ids = [i["product_id"] for i in data]
            r_info = requests.post("https://api-seller.ozon.ru/v2/product/info/list", headers=get_headers(), json={"product_id": ids})
            info = r_info.json().get("result", {}).get("items", [])
            
            for i in info:
                p = float(i.get("price", {}).get("price", 0))
                mp = float(i.get("price", {}).get("marketing_price", 0) or p)
                st = i.get("stocks", {}).get("present", 0)
                # Структура как в GAS: [Фото, id, id, offer, Brand, Cat, Name, Price, Disc, Disc, Stock]
                items.append([
                    i.get("primary_image", ""), str(i.get("id")), str(i.get("id")), i.get("offer_id", ""),
                    "OZON", i.get("category_id", ""), i.get("name", ""), p, mp, mp, st
                ])
            
            last_id = data[-1]["product_id"]
            if len(data) < 100: break
            
        except Exception as e:
            print(f"Err Cards: {e}")
            break
            
    return items

# --- МОДУЛЬ: ОСТАТКИ (STOCKS) ---
def fetch_stocks():
    items = []
    page = 1
    print("📦 Start Stocks...")
    
    # 1. FBO (Со склада Ozon)
    while True:
        payload = { "limit": 100, "offset": (page-1)*100 }
        try:
            r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(), json=payload)
            rows = r.json().get("result", {}).get("rows", [])
            if not rows: break
            
            for r in rows:
                sku = r.get("sku")
                name = r.get("item_name")
                # FBO возвращает остатки по складам, суммируем
                total = 0
                for wh in r.get("warehouses", []):
                    # Зарезервировано + Доступно
                    total += wh.get("item_cnt", 0)
                
                # Структура: [Склад, Артикул, Остаток, В пути(0), nmID(sku)]
                items.append(["FBO Ozon", r.get("item_code"), total, 0, str(sku)])
            
            if len(rows) < 100: break
            page += 1
        except Exception as e: 
            print(f"Err Stock FBO: {e}"); break

    # 2. FBS (С нашего склада) - метод v3/product/info/stocks
    # Для простоты пока оставим только FBO, так как FBS требует перебора всех ID.
    
    return items

# --- МОДУЛЬ: ПРОДАЖИ (SALES - FBO) ---
def fetch_sales(d_from, d_to):
    items = []
    page = 1
    print(f"💰 Start Sales {d_from}-{d_to}...")
    
    while True:
        payload = {
            "filter": { "since": f"{d_from}T00:00:00Z", "to": f"{d_to}T23:59:59Z" },
            "limit": 100,
            "page": page
        }
        try:
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=get_headers(), json=payload)
            postings = r.json().get("result", [])
            if not postings: break
            
            for p in postings:
                order_num = p.get("posting_number")
                date_str = p.get("created_at")[:10] # YYYY-MM-DD
                time_str = p.get("created_at")[11:19]
                wh = p.get("analytics_data", {}).get("warehouse_name", "Ozon")
                region = p.get("analytics_data", {}).get("region", "RU")
                status = p.get("status", "")
                is_cancel = "cancelled" in status
                
                for prod in p.get("products", []):
                    price = float(prod.get("price", 0))
                    # Структура Sales: [Date, Time, Type, Art, nmID, Qty, Price, PriceDisc, FinPrice, SPP, Wh, Reg, OrdNum]
                    row = [
                        date_str, time_str, 
                        "Отмена заказа" if is_cancel else "Заказ",
                        prod.get("offer_id"), str(prod.get("sku")), 1,
                        price, price, price, 0, wh, region, order_num
                    ]
                    items.append(row)
            
            if len(postings) < 100: break
            page += 1
        except Exception as e:
            print(f"Err Sales: {e}"); break
            
    return items

@app.route("/")
def health(): return "Ozon Service OK", 200

@app.route("/sync", methods=['POST'])
def sync():
    data = request.json
    mode = data.get("mode") # CARDS, STOCK, SALES
    d_from = data.get("dateFrom")
    d_to = data.get("dateTo")
    
    if not OZON_API_KEY: return jsonify({"error": "No API Key"}), 500
    
    # 1. Сообщаем в GAS, что начали
    send_to_gas({"type": "LOG", "msg": f"Python: Start {mode}..."})
    
    rows = []
    target_sheet = ""
    
    if mode == "CARDS":
        rows = fetch_cards()
        target_sheet = "OZ_CARDS_PY"
    elif mode == "STOCK":
        rows = fetch_stocks()
        target_sheet = "OZ_STOCK_PY"
    elif mode == "SALES":
        rows = fetch_sales(d_from, d_to)
        target_sheet = "OZ_SALES_PY"
    else:
        return jsonify({"error": "Unknown mode"}), 400
        
    if rows:
        # 2. Отправляем данные
        send_to_gas({
            "type": "DATA",
            "sheetName": target_sheet,
            "rows": rows
        })
        return jsonify({"status": "ok", "count": len(rows)}), 200
    else:
        send_to_gas({"type": "LOG", "msg": f"Python: {mode} Empty"})
        return jsonify({"status": "empty"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)