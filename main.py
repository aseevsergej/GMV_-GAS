import os
import time
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

# Fallback secrets (not used if passed from GAS)
GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL", "")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"

def get_headers(cid, key):
    # Очистка ключей от пробелов и приведение к строке
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

# --- OZON CARDS (Актуальный метод 2026: v3 или v2 с фильтром) ---
def fetch_cards(cid, key):
    items = []
    print(f"📦 Start Cards {cid}...")
    
    # Используем v2, так как он возвращает product_id. 
    # Если v2 404, пробуем v1. v3 обычно для создания.
    endpoints = [
        "https://api-seller.ozon.ru/v2/product/list",
        "https://api-seller.ozon.ru/v1/product/list"
    ]
    
    last_id = ""
    used_endpoint = ""
    
    # 1. Определяем рабочий эндпоинт
    for ep in endpoints:
        try:
            r = requests.post(ep, headers=get_headers(cid, key), json={"limit": 1})
            if r.status_code == 200:
                used_endpoint = ep
                break
        except: pass
        
    if not used_endpoint:
        send_to_gas({"type": "LOG", "msg": f"OZ {cid}: All Card Endpoints 404/Error"})
        return []

    # 2. Скачиваем
    while True:
        payload = { "filter": { "visibility": "ALL" }, "limit": 1000 }
        if last_id: payload["last_id"] = last_id
        
        try:
            r = requests.post(used_endpoint, headers=get_headers(cid, key), json=payload)
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            ids = [i["product_id"] for i in data]
            
            # Получаем детали (Info)
            r_info = requests.post("https://api-seller.ozon.ru/v2/product/info/list", headers=get_headers(cid, key), json={"product_id": ids})
            info_map = {}
            if r_info.status_code == 200:
                for i in r_info.json().get("result", {}).get("items", []):
                    info_map[i["id"]] = i
            
            for item_base in data:
                pid = item_base["product_id"]
                full = info_map.get(pid, {})
                offer_id = full.get("offer_id") or item_base.get("offer_id") or ""
                
                # Защита от отсутствия цен
                price_obj = full.get("price") or {}
                p = float(price_obj.get("price", 0))
                mp = float(price_obj.get("marketing_price", 0) or p)
                
                stocks_obj = full.get("stocks") or {}
                st = stocks_obj.get("present", 0)
                
                # [Фото, nmID, АртWB, АртПрод, Бренд, Кат, Назв, Ц.База, Ц.Прод, Ц.СПП, Ост]
                items.append([
                    full.get("primary_image", ""), 
                    str(pid), str(pid), offer_id,
                    "OZON", str(full.get("category_id", "")), full.get("name", f"ID {pid}"), 
                    p, mp, mp, st
                ])
            
            last_id = data[-1]["product_id"]
            if len(data) < 1000: break
            
        except Exception as e:
            send_to_gas({"type": "LOG", "msg": f"Py Crash Cards: {str(e)}"})
            break
            
    return items

# --- OZON STOCK (FBO) ---
def fetch_stocks(cid, key):
    items = []
    try:
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=get_headers(cid, key), json={"limit": 1000, "offset":0})
        if r.status_code == 200:
            rows = r.json().get("result", {}).get("rows", [])
            for row in rows:
                # FBO остатки суммируем
                whs = row.get("warehouses") or []
                total = sum(w.get("item_cnt",0) for w in whs)
                # [Склад, Арт, Ост, Путь, nmID]
                items.append(["FBO Ozon", row.get("item_code"), total, 0, str(row.get("sku"))])
        else:
            send_to_gas({"type": "LOG", "msg": f"Stock Err {cid}: {r.status_code}"})
    except Exception as e:
        send_to_gas({"type": "LOG", "msg": f"Stock Crash: {e}"})
    return items

# --- OZON SALES (FBO) ---
def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    
    # Формат дат для Ozon: 2024-01-01T00:00:00Z
    since_dt = f"{d_from}T00:00:00Z"
    to_dt = f"{d_to}T23:59:59Z"

    while True:
        try:
            payload = { 
                "filter": { "since": since_dt, "to": to_dt }, 
                "limit": 1000, 
                "page": page 
            }
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=get_headers(cid, key), json=payload)
            
            if r.status_code != 200: break
            
            # Защита от NoneType
            res = r.json().get("result")
            if not res: break
            
            # В v2/posting/fbo/list иногда приходит список, иногда объект?
            # По документации: result: []
            if not isinstance(res, list): break
            if len(res) == 0: break
            
            for p in res:
                created = p.get("created_at") or "2000-01-01T00:00:00Z"
                d_str = created[:10]
                t_str = created[11:16]
                
                status_raw = p.get("status") or ""
                status = "Отмена" if "cancelled" in status_raw.lower() else "Заказ"
                
                # Безопасное получение вложенных объектов
                analytics = p.get("analytics_data") or {}
                wh = analytics.get("warehouse_name", "Ozon")
                reg = analytics.get("region", "RU")
                num = p.get("posting_number", "")

                products = p.get("products") or []
                for prod in products:
                    price = float(prod.get("price", 0))
                    # [Дата, Время, Тип, Арт, nmID, Кол, Ц.Розн, Ц.Прод, Ц.Факт, СПП, Склад, Регион, №]
                    items.append([
                        d_str, t_str, status, 
                        prod.get("offer_id"), str(prod.get("sku")), 
                        1, price, price, price, 0, wh, reg, num
                    ])
            
            if len(res) < 1000: break
            page += 1
            time.sleep(0.2)
            
        except Exception as e:
            send_to_gas({"type": "LOG", "msg": f"Sales Crash: {str(e)}"})
            break
            
    return items

# --- OZON FUNNEL (ANALYTICS) ---
def fetch_funnel(cid, key, d_from, d_to):
    # Метод v1/analytics/data (тяжелый, берем только общие цифры)
    items = []
    try:
        payload = {
            "date_from": d_from,
            "date_to": d_to,
            "metrics": ["ordered_units", "revenue", "hits_view_search"],
            "dimension": ["sku", "day"],
            "limit": 1000
        }
        r = requests.post("https://api-seller.ozon.ru/v1/analytics/data", headers=get_headers(cid, key), json=payload)
        # Это сложный метод, пока вернем заглушку или базовый список
        # Чтобы не крашилось
    except: pass
    
    # Возвращаем пустой список, чтобы GAS не ругался, или реализуем позже
    return items

@app.route("/")
def health(): return "Ozon Final OK", 200

@app.route("/sync", methods=['POST'])
def sync():
    try:
        data = request.json
        if not data: return jsonify({"error": "No JSON"}), 400
        
        cid = data.get("clientId")
        key = data.get("apiKey")
        mode = data.get("mode")
        d_from = data.get("dateFrom")
        d_to = data.get("dateTo")
        
        if not cid or not key:
            send_to_gas({"type": "LOG", "msg": "Py: No Keys provided"})
            return jsonify({"error": "Keys missing"}), 400

        send_to_gas({"type": "LOG", "msg": f"Py: {mode} start for {cid}..."})

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
            # Пока заглушка
            return jsonify({"status": "empty"}), 200
        else:
            return jsonify({"error": "Unknown mode"}), 400
            
        if rows:
            send_to_gas({"type": "DATA", "sheetName": target, "rows": rows})
            return jsonify({"status": "ok", "count": len(rows)}), 200
        else:
            return jsonify({"status": "empty"}), 200
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)