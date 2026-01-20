import os
import time
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

# --- ВАЖНО: Читаем переменные окружения ---
# Если переменные не приходят, впишите их сюда ЖЕСТКО для теста
OZON_CLIENT_ID = os.environ.get("OZON_CLIENT_ID", "")
OZON_API_KEY = os.environ.get("OZON_API_KEY", "")
GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL", "")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"

def get_headers():
    return {
        "Client-Id": str(OZON_CLIENT_ID), # Ozon принимает и так, и так, но лучше строку
        "Api-Key": str(OZON_API_KEY),
        "Content-Type": "application/json"
    }

def send_to_gas(payload):
    if not GAS_WEBAPP_URL: return
    payload["secret"] = SECRET_KEY
    try:
        requests.post(GAS_WEBAPP_URL, json=payload, timeout=10)
    except: pass

def fetch_cards():
    items = []
    last_id = ""
    print("📦 Start Cards...")
    
    if not OZON_CLIENT_ID or not OZON_API_KEY:
        send_to_gas({"type": "LOG", "msg": "CRITICAL: NO KEYS IN PYTHON"})
        return []

    while True:
        # Упрощенный payload, который работает у всех
        payload = { 
            "filter": { "visibility": "ALL" }, 
            "limit": 1000 # Берем по максимуму
        }
        if last_id: payload["last_id"] = last_id
        
        try:
            r = requests.post("https://api-seller.ozon.ru/v2/product/list", headers=get_headers(), json=payload)
            
            if r.status_code == 404:
                # Fallback: пробуем v1 (иногда v2 глючит на старых аккаунтах)
                r = requests.post("https://api-seller.ozon.ru/v1/product/list", headers=get_headers(), json=payload)
            
            if r.status_code != 200:
                send_to_gas({"type": "LOG", "msg": f"OZON List Err: {r.status_code} {r.text[:50]}"})
                break
                
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            # Получаем инфо
            ids = [i["product_id"] for i in data]
            r_info = requests.post("https://api-seller.ozon.ru/v2/product/info/list", headers=get_headers(), json={"product_id": ids})
            
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
                
                # Структура [Фото, nmID, АртWB, АртПрод, Бренд, Кат, Назв, Ц.База, Ц.Прод, Ц.СПП, Ост]
                items.append([
                    full.get("primary_image", ""), 
                    str(pid), 
                    str(pid), 
                    offer_id,
                    "OZON", 
                    str(full.get("category_id", "")), 
                    full.get("name", f"Товар {pid}"), 
                    p, mp, mp, st
                ])
            
            last_id = data[-1]["product_id"]
            if len(data) < 1000: break
            time.sleep(0.2)
            
        except Exception as e:
            send_to_gas({"type": "LOG", "msg": f"Py Crash Cards: {str(e)}"})
            break
            
    return items

def fetch_stocks():
    items = []
    try:
        # FBO
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(), json={"limit": 1000, "offset":0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                total = sum(w.get("item_cnt",0) for w in row.get("warehouses", []))
                # Структура [Склад, Арт, Ост, Путь, nmID]
                items.append(["FBO Ozon", row.get("item_code"), total, 0, str(row.get("sku"))])
    except Exception as e:
        send_to_gas({"type": "LOG", "msg": f"Stock Crash: {e}"})
    return items

def fetch_sales(d_from, d_to):
    items = []
    page = 1
    # Приводим даты к формату RFC3339
    date_since = f"{d_from}T00:00:00Z"
    date_to = f"{d_to}T23:59:59Z"

    while True:
        try:
            payload = { 
                "filter": { "since": date_since, "to": date_to }, 
                "limit": 1000,
                "page": page
            }
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=get_headers(), json=payload)
            
            if r.status_code != 200:
                send_to_gas({"type": "LOG", "msg": f"Sales Err {r.status_code}: {r.text[:50]}"})
                break

            postings = r.json().get("result", [])
            if not postings: break
            
            for p in postings:
                # Дата заказа
                created = p.get("created_at", "2000-01-01T00:00:00Z")
                d_str = created[:10]
                t_str = created[11:16]
                
                stat_raw = p.get("status", "")
                status = "Отмена" if "cancelled" in stat_raw else "Заказ"
                
                wh = p.get("analytics_data", {}).get("warehouse_name", "Ozon")
                reg = p.get("analytics_data", {}).get("region", "RU")
                num = p.get("posting_number", "")

                for prod in p.get("products", []):
                    price = float(prod.get("price", 0))
                    # Структура [Дата, Время, Тип, Арт, nmID, Кол, Ц.Розн, Ц.Прод, Ц.Факт, СПП, Склад, Регион, №]
                    items.append([
                        d_str, t_str, status, 
                        prod.get("offer_id"), 
                        str(prod.get("sku")), 
                        1, 
                        price, price, price, 0, 
                        wh, reg, num
                    ])
            
            if len(postings) < 1000: break
            page += 1
            time.sleep(0.3)
            
        except Exception as e:
            send_to_gas({"type": "LOG", "msg": f"Sales Crash: {e}"})
            break
            
    return items

@app.route("/")
def health(): return "Ozon Service v4 OK", 200

@app.route("/sync", methods=['POST'])
def sync():
    data = request.json
    mode = data.get("mode")
    d_from = data.get("dateFrom")
    d_to = data.get("dateTo")
    
    send_to_gas({"type": "LOG", "msg": f"Python: Start {mode}..."})

    rows = []
    target = ""
    
    if mode == "CARDS":
        rows = fetch_cards()
        target = "OZ_CARDS_PY"
    elif mode == "STOCK":
        rows = fetch_stocks()
        target = "OZ_STOCK_PY"
    elif mode == "SALES":
        rows = fetch_sales(d_from, d_to)
        target = "OZ_SALES_PY"
    elif mode == "FUNNEL":
        send_to_gas({"type": "LOG", "msg": "Python: Funnel Skipped"})
        return jsonify({"status": "empty"}), 200
    else:
        return jsonify({"error": f"Unknown mode"}), 400
        
    if rows:
        send_to_gas({"type": "DATA", "sheetName": target, "rows": rows})
        return jsonify({"status": "ok", "count": len(rows)}), 200
    else:
        # Не спамим Empty, если это реально пусто
        # send_to_gas({"type": "LOG", "msg": f"Python: {mode} Empty"})
        return jsonify({"status": "empty"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)