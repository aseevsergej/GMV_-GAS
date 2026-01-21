import os
import time
import requests
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL", "")
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"

# --- LOGGING ---
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

# --- HEADERS ---
def get_headers(cid, key):
    return {
        "Client-Id": str(cid).strip().replace('"', '').replace("'", ""),
        "Api-Key": str(key).strip().replace('"', '').replace("'", ""),
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

# --- HELPER: POST WITH FAIL-SAFE ---
def safe_post(url, headers, payload, label):
    try:
        r = requests.post(url, headers=headers, json=payload)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 404:
            send_log(f"OZ {label} 404 (Not Found). URL: {url}")
            return None
        else:
            send_log(f"OZ {label} ERR {r.status_code}: {r.text[:200]}")
            return None
    except Exception as e:
        send_log(f"OZ {label} Ex: {e}")
        return None

# --- BRAND EXTRACTOR ---
def get_brand(attrs):
    for a in attrs:
        if a.get("attribute_id") in [85, 31]:
            vals = a.get("values", [])
            if vals: return vals[0].get("value", "")
    return ""

# --- MAIN: CARDS (Robust) ---
def fetch_cards(cid, key):
    items = []
    headers = get_headers(cid, key)
    
    # Endpoints
    URL_LIST_V3 = "https://api-seller.ozon.ru/v3/product/list"
    URL_INFO_V2 = "https://api-seller.ozon.ru/v2/product/info/list"
    
    last_id = "" # V3 token (string)

    while True:
        # 1. GET LIST (V3)
        payload = { "filter": { "visibility": "ALL" }, "limit": 100 }
        if last_id: payload["last_id"] = last_id
        
        resp = safe_post(URL_LIST_V3, headers, payload, "ListV3")
        if not resp: break
        
        result = resp.get("result", {})
        data_list = result.get("items", [])
        new_last_id = result.get("last_id", "")
        
        if not data_list: break
        
        # Collect IDs (int)
        ids = [int(x.get("product_id")) for x in data_list]
        
        # 2. GET DETAILS (V2 Mass)
        info_map = {}
        if ids:
            # Try V2 Info List
            resp_info = safe_post(URL_INFO_V2, headers, {"product_id": ids}, "InfoV2")
            
            if resp_info:
                for i in resp_info.get("result", {}).get("items", []):
                    info_map[i.get("id")] = i
            else:
                send_log("Warning: Details failed. Loading basic data only.")
                # Fallback: Try fetching just ONE item to verify access (debug)
                try:
                    one_url = "https://api-seller.ozon.ru/v2/product/info"
                    one_resp = requests.post(one_url, headers=headers, json={"product_id": ids[0]})
                    if one_resp.status_code == 200:
                        send_log(f"DEBUG: Single Item Access OK! Mass info is broken.")
                    else:
                        send_log(f"DEBUG: Single Item Access Also Failed: {one_resp.status_code}")
                except: pass

        # 3. BUILD ROWS (Even if info missing)
        for basic in data_list:
            pid = int(basic.get("product_id"))
            
            # If info failed, we use what we have from 'basic' (Offer ID)
            # 'basic' structure in V3 list: { "product_id": 123, "offer_id": "art-001" }
            
            # Merge with info if available
            details = info_map.get(pid, {})
            
            # Fields
            offer_id = details.get("offer_id") or basic.get("offer_id") or "NoOfferID"
            ozon_id = str(pid)
            
            name = details.get("name") or "Товар (нет деталей)"
            cat = str(details.get("category_id", ""))
            brand = get_brand(details.get("attributes", []))
            
            # Photos
            primary = details.get("primary_image") or ""
            if not primary and details.get("images"):
                imgs = details.get("images")
                primary = imgs[0] if isinstance(imgs[0], str) else imgs[0].get("file_name","")

            # Prices
            def gp(k): return float(details.get(k) or details.get("price", {}).get(k) or 0)
            p_old = gp("old_price")
            p_sell = gp("price")
            p_mkt = gp("marketing_price")
            
            if p_old == 0: p_old = p_sell
            if p_mkt == 0: p_mkt = p_sell
            
            # [Photo, ArtOZ, ArtMy, Brand, Cat, Name, PriceOld, PriceSell, PriceMkt, PriceCard]
            items.append([primary, ozon_id, offer_id, brand, cat, name, p_old, p_sell, p_mkt, p_mkt])

        # Pagination
        last_id = new_last_id
        if not last_id: break
        if len(data_list) < 100: break

    return items

# --- STOCK & SALES (No Changes) ---
def fetch_stocks(cid, key):
    items = []
    headers = get_headers(cid, key)
    url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    resp = safe_post(url, headers, {"limit": 1000, "offset": 0}, "Stock")
    if resp:
        for r in resp.get("result", {}).get("rows", []):
            sku = str(r.get("sku", ""))
            oid = r.get("item_code") or sku
            for wh in r.get("warehouses", []):
                if wh.get("item_cnt", 0) > 0:
                    items.append([wh.get("warehouse_name"), oid, wh.get("item_cnt")])
    return items

def fetch_sales(cid, key, d_from, d_to):
    items = []
    page = 1
    headers = get_headers(cid, key)
    url = "https://api-seller.ozon.ru/v2/posting/fbo/list"
    while True:
        payload = {
            "filter": { "since": f"{d_from}T00:00:00Z", "to": f"{d_to}T23:59:59Z" },
            "limit": 1000, "page": page,
            "with": {"analytics_data": True, "financial_data": True}
        }
        resp = safe_post(url, headers, payload, "Sales")
        if not resp: break
        res = resp.get("result", [])
        if not res: break
        for p in res:
            created = p.get("created_at", "")[:10]
            typ = "Отмена" if "cancelled" in str(p.get("status", "")).lower() else "Продажа"
            an = p.get("analytics_data") or {}
            fin = p.get("financial_data") or {}
            fin_prods = {x.get('product_id'): x for x in fin.get('products', [])}
            for prod in p.get("products", []):
                sku = prod.get("sku")
                price = float(fin_prods.get(sku, {}).get('client_price') or prod.get('price') or 0)
                items.append([created, typ, prod.get("offer_id"), str(sku), 1, price, an.get("warehouse_name"), an.get("region")])
        if len(res) < 1000: break
        page += 1
        time.sleep(0.2)
    return items

@app.route("/")
def health(): return "Ozon v113 Survival OK", 200

@app.route("/sync", methods=['POST'])
def sync():
    try:
        data = request.json
        # Check mode
        mode = data.get("mode")
        rows = []
        target = ""
        
        if mode == "CARDS":
            rows = fetch_cards(data.get("clientId"), data.get("apiKey"))
            target = "OZ_CARDS_PY"
        elif mode == "STOCK":
            rows = fetch_stocks(data.get("clientId"), data.get("apiKey"))
            target = "OZ_STOCK_PY"
        elif mode == "SALES":
            rows = fetch_sales(data.get("clientId"), data.get("apiKey"), data.get("dateFrom"), data.get("dateTo"))
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