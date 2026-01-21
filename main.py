import os
import time
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL", "")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"

# --- ЛОГИРОВАНИЕ ---
def send_log(msg):
    if not GAS_WEBAPP_URL: return
    try:
        requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "LOG", "msg": str(msg)[:2000]}, timeout=5)
    except: pass

def send_data(sheet, rows):
    if not GAS_WEBAPP_URL: return
    try:
        requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "DATA", "sheetName": sheet, "rows": rows}, timeout=45)
    except: pass

# --- ЗАГОЛОВКИ ---
def get_headers(cid, key):
    # Удаляем пробелы, переносы и возможные кавычки
    c_clean = str(cid).strip().replace('"', '').replace("'", "")
    k_clean = str(key).strip().replace('"', '').replace("'", "")
    return {
        "Client-Id": c_clean, 
        "Api-Key": k_clean,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; Google-Apps-Script)",
        "Accept": "application/json"
    }

# --- УНИВЕРСАЛЬНЫЙ FETCH ---
def post_request(url, headers, payload, label=""):
    try:
        r = requests.post(url, headers=headers, json=payload)
        if r.status_code == 200:
            return r.json()
        else:
            send_log(f"OZ {label} ERR {r.status_code} on {url}: {r.text[:100]}")
            return None
    except Exception as e:
        send_log(f"OZ {label} Ex on {url}: {e}")
        return None

# --- OZON CARDS (ТОВАРЫ) v109 ---
def fetch_cards(cid, key):
    items = []
    headers = get_headers(cid, key)
    
    # URLS
    url_list = "https://api-seller.ozon.ru/v3/product/list"
    
    # Strategies
    url_v2_info = "https://api-seller.ozon.ru/v2/product/info/list"
    url_v3_attr = "https://api-seller.ozon.ru/v3/products/info/attributes"
    url_v4_price = "https://api-seller.ozon.ru/v4/product/info/prices"
    
    last_id = ""

    while True:
        # 1. Получаем СПИСОК ID
        payload_list = { 
            "filter": { "visibility": "ALL" }, 
            "limit": 100,
            "last_id": last_id if last_id else ""
        }
        resp = post_request(url_list, headers, payload_list, "List")
        if not resp: break
        
        list_data = resp.get("result", {}).get("items", [])
        if not list_data: break
        
        # Гарантируем, что ID - это int
        ids = [int(i.get("product_id")) for i in list_data]
        
        info_map = {} # Словарь для данных (цены, имена...)
        
        # 2. ПОПЫТКА А: v2/product/info/list (Всё в одном)
        success_v2 = False
        resp_v2 = post_request(url_v2_info, headers, {"product_id": ids}, "InfoV2")
        
        if resp_v2:
            success_v2 = True
            for i in resp_v2.get("result", {}).get("items", []):
                info_map[i.get("id")] = i
        else:
            # 3. ПОПЫТКА Б: v3 Attributes + v4 Prices (Если v2 упал)
            # send_log("Fallback to v3/v4 methods...")
            
            # Attributes
            resp_attr = post_request(url_v3_attr, headers, {"filter": {"product_id": ids, "visibility": "ALL"}, "limit": 100}, "AttrV3")
            if resp_attr:
                for i in resp_attr.get("result", []):
                    if i.get("id") not in info_map: info_map[i.get("id")] = {}
                    info_map[i.get("id")].update(i) # Merging attributes
            
            # Prices
            resp_price = post_request(url_v4_price, headers, {"filter": {"product_id": ids, "visibility": "ALL"}, "limit": 100}, "PriceV4")
            if resp_price:
                for i in resp_price.get("result", {}).get("items", []):
                    pid = i.get("product_id")
                    if pid not in info_map: info_map[pid] = {}
                    info_map[pid]["price"] = i.get("price", {}) # Merging prices

        # --- СБОРКА ---
        for basic in list_data:
            pid = int(basic.get("product_id"))
            
            # Данные из List (базовые)
            offer_id = basic.get("offer_id")
            ozon_id = str(pid)
            
            # Данные из Info (Details)
            details = info_map.get(pid, {})
            
            # Имя, Категория
            name = details.get("name") or "Товар"
            cat_id = str(details.get("category_id", ""))
            
            # Фото
            primary = details.get("primary_image") or ""
            if not primary and details.get("images"):
                imgs = details.get("images")
                # В v2 images - это строки, в v3 - объекты
                if isinstance(imgs[0], str): primary = imgs[0]
                elif isinstance(imgs[0], dict): primary = imgs[0].get("file_name", "")
            
            # Бренд
            brand = ""
            # Способ 1: Прямое поле (v2)
            # Способ 2: Атрибуты
            attrs = details.get("attributes", [])
            for a in attrs:
                if a.get("attribute_id") in [85, 31]:
                    vals = a.get("values", [])
                    if vals: brand = vals[0].get("value", "")
                    break
            
            # Цены
            price_obj = details.get("price") or {}
            def get_p(k): return float(price_obj.get(k) or details.get(k) or 0)
            
            p_old = get_p("old_price")
            p_sell = get_p("price")
            p_mkt = get_p("marketing_price")
            
            if p_old == 0: p_old = p_sell
            if p_mkt == 0: p_mkt = p_sell
            
            # [Фото, Арт.OZ, Арт.Наш, Бренд, Категория, Название, Ц.База, Ц.Прод, Ц.Покуп, Ц.Карта]
            items.append([
                primary,
                ozon_id,
                offer_id,
                brand,
                cat_id,
                name,
                p_old,
                p_sell,
                p_mkt,
                p_mkt
            ])

        last_id = list_data[-1].get("last_id")
        if not last_id:
             # Fallback для v2 пагинации
             last_id = list_data[-1].get("product_id")
             
        if len(list_data) < 100: break
            
    return items

# --- OZON STOCK (Остатки v2) ---
def fetch_stocks(cid, key):
    items = []
    headers = get_headers(cid, key)
    url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    
    resp = post_request(url, headers, {"limit": 1000, "offset": 0}, "Stock")
    if resp:
        rows = resp.get("result", {}).get("rows", [])
        for row in rows:
            sku = str(row.get("sku", ""))
            offer_id = row.get("item_code") or sku
            for wh in row.get("warehouses", []):
                qty = wh.get("item_cnt", 0)
                wh_name = wh.get("warehouse_name", "Склад")
                if qty > 0:
                    items.append([wh_name, offer_id, qty])
    return items

# --- OZON SALES (Продажи FBO v2) ---
def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    headers = get_headers(cid, key)
    url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
    
    while True:
        payload = {
            "filter": { "since": f"{d_from}T00:00:00Z", "to": f"{d_to}T23:59:59Z" },
            "limit": 1000,
            "page": page,
            "with": {"analytics_data": True, "financial_data": True}
        }
        resp = post_request(url, headers, payload, "Sales")
        if not resp: break
        
        res = resp.get("result", [])
        if not res: break
        
        for p in res:
            created = p.get("created_at", "")[:10]
            status = str(p.get("status", "")).lower()
            typ = "Отмена" if "cancelled" in status else "Продажа"
            
            an = p.get("analytics_data") or {}
            wh_from = an.get("warehouse_name", "FBO")
            wh_to = an.get("region", "RU")
            
            fin = p.get("financial_data") or {}
            fin_prods = {x.get('product_id'): x for x in fin.get('products', [])}
            
            for prod in p.get("products", []):
                sku = prod.get("sku")
                oid = prod.get("offer_id")
                fp = fin_prods.get(sku, {})
                price = float(fp.get('client_price') or prod.get('price') or 0)
                
                items.append([created, typ, oid, str(sku), 1, price, wh_from, wh_to])
        
        if len(res) < 1000: break
        page += 1
        time.sleep(0.2)

    return items

@app.route("/")
def health(): return "Ozon v109 Fallback OK", 200

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
        elif mode == "FUNNEL":
             return jsonify({"status": "empty"}), 200
            
        if rows:
            send_data(target, rows)
            return jsonify({"status": "ok", "count": len(rows)}), 200
        else:
            return jsonify({"status": "empty"}), 200
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)