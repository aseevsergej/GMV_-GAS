import requests
import time
import json
import datetime
import os
import sys
from dotenv import load_dotenv

try:
    import psutil
    PSUTIL_OK = True
except ImportError:
    PSUTIL_OK = False

load_dotenv()

GAS_WEBAPP_URL = os.getenv("GAS_WEBAPP_URL")
SECRET_KEY = os.getenv("SECRET_KEY", "MY_SUPER_SECRET_PASSWORD_123")
UPDATE_INTERVAL_HOURS = int(os.getenv("UPDATE_INTERVAL_HOURS", 1))

# === ГЛОБАЛЬНАЯ СЕССИЯ ===
session = requests.Session()

def get_server_load():
    if not PSUTIL_OK: return {"cpu": 0, "ram": 0}
    try: return {"cpu": psutil.cpu_percent(), "ram": psutil.virtual_memory().percent}
    except: return {"cpu": 0, "ram": 0}

def log(msg, type="LOG_VPS"):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")
    if GAS_WEBAPP_URL:
        try:
            payload = {"secret": SECRET_KEY, "type": type, "msg": f"{msg}", "server_info": get_server_load()}
            # Для логов используем отдельный простой запрос
            requests.post(GAS_WEBAPP_URL, json=payload, timeout=5)
        except: pass

def get_config_from_gas():
    log("Запрос конфига...")
    if not GAS_WEBAPP_URL: return None
    try:
        r = requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "GET_CONFIG"}, timeout=30)
        if r.status_code == 200: return r.json()
        log(f"GAS Error {r.status_code}", "ERR")
    except Exception as e: log(f"GAS Connect Fail: {e}", "ERR")
    return None

def send_to_gas(sheet_name, rows):
    if not rows: return
    log(f"Отправка {len(rows)} строк в {sheet_name}...")
    try:
        chunk_size = 1000
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            payload = {"secret": SECRET_KEY, "type": "DATA", "sheetName": sheet_name, "rows": chunk, "server_info": get_server_load()}
            requests.post(GAS_WEBAPP_URL, json=payload, timeout=90)
            time.sleep(1.2)
        log("Отправка завершена.")
    except Exception as e: log(f"Send Fail: {e}", "ERR")

# === OZON API (SESSION MODE) ===

def update_headers(cid, key):
    # Обновляем заголовки сессии
    session.headers.update({
        "Client-Id": str(cid).strip(),
        "Api-Key": str(key).strip(),
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Host": "api-seller.ozon.ru" # Явное указание хоста
    })

def fetch_cards(cid, key, acc_name):
    update_headers(cid, key)
    URL_LIST = "https://api-seller.ozon.ru/v2/product/list"
    URL_INFO = "https://api-seller.ozon.ru/v2/product/info/list"
    
    log(f"Товары: {URL_LIST}")
    items = []
    last_id = ""
    
    while True:
        # Минималистичный payload
        payload = {"limit": 100}
        if last_id: payload["last_id"] = str(last_id)
        
        try:
            # Используем session.post
            r = session.post(URL_LIST, json=payload)
            
            if r.status_code != 200:
                log(f"Товары Fail {r.status_code}: {r.text[:100]}", "ERR")
                break
                
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            ids = [int(x.get("product_id")) for x in data]
            info_map = {}
            try:
                r_info = session.post(URL_INFO, json={"product_id": ids})
                for i in r_info.json().get("result", {}).get("items", []): info_map[i.get("id")] = i
            except: pass

            for basic in data:
                try:
                    pid = int(basic.get("product_id"))
                    full = info_map.get(pid, {})
                    offer_id = str(full.get("offer_id") or basic.get("offer_id") or "NO_ID")
                    ozon_id = str(pid)
                    name = str(full.get("name") or "Товар")
                    cat = str(full.get("category_id", ""))
                    
                    primary = ""
                    if full.get("primary_image"): primary = full.get("primary_image")
                    elif full.get("images"): primary = full.get("images")[0]
                    if isinstance(primary, dict): primary = primary.get("file_name", "")
                    
                    items.append([acc_name, primary, ozon_id, offer_id, "No Brand", cat, name, 0, 0, 0, 0]) # Цены пока 0 для теста
                except: continue

            last_item = data[-1]
            last_id = str(last_item.get("product_id"))
            if len(data) < 100: break
        except Exception as e:
            log(f"Loop Err: {e}", "ERR")
            break
            
    log(f"Найдено товаров: {len(items)}")
    return items

def fetch_stocks(cid, key, acc_name):
    update_headers(cid, key)
    URL = "https://api-seller.ozon.ru/v3/product/info/stocks"
    log(f"Остатки: {URL}")
    items = []
    last_id = ""
    
    while True:
        payload = {"filter": {}, "limit": 100}
        if last_id: payload["last_id"] = str(last_id)

        try:
            r = session.post(URL, json=payload)
            if r.status_code != 200:
                log(f"Остатки Fail {r.status_code}: {r.text[:50]}", "ERR")
                break
            
            res = r.json().get("result", {})
            data = res.get("items", [])
            if not data: break
            
            for prod in data:
                try:
                    offer_id = prod.get("offer_id", "")
                    for stock in prod.get("stocks", []):
                        if stock.get("present", 0) > 0:
                            items.append([acc_name, f"Ozon {stock.get('type')}", offer_id, stock.get("present")])
                except: continue
            
            last_id = res.get("last_id", "")
            if not last_id or len(data) < 100: break
        except: break
            
    log(f"Найдено остатков: {len(items)}")
    return items

def fetch_sales(cid, key, date_from, date_to, acc_name):
    update_headers(cid, key)
    log(f"Продажи ({date_from})...")
    items = []
    page = 1
    
    while True:
        try:
            r = session.post("https://api-seller.ozon.ru/v2/posting/fbo/list", json={
                "filter": { "since": f"{date_from}T00:00:00Z", "to": f"{date_to}T23:59:59Z" },
                "limit": 1000, "page": page,
                "with": {"analytics_data": True, "financial_data": True}
            })
            if r.status_code != 200: break
            res = r.json().get("result", [])
            if not res: break
            for p in res:
                try:
                    prod = p.get("products", [])[0]
                    price = float(prod.get('price') or 0)
                    items.append([acc_name, p.get("created_at")[:10], "Продажа", prod.get("offer_id"), str(prod.get("sku")), 1, price, "", ""])
                except: continue
            if len(res) < 1000: break
            page += 1
            time.sleep(0.1)
        except: break
    log(f"Найдено продаж: {len(items)}")
    return items

if __name__ == "__main__":
    log("=== ЗАПУСК v133 (SESSION) ===")
    if GAS_WEBAPP_URL:
        try: requests.post(GAS_WEBAPP_URL, json={"secret":SECRET_KEY, "type":"INIT_VPS"}, timeout=5)
        except: pass

    while True:
        config = get_config_from_gas()
        
        now = datetime.datetime.now()
        d_f = (now - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        d_t = now.strftime("%Y-%m-%d")
        
        ACCOUNTS = []
        SETTINGS = {"oz_cards":True, "oz_stock":True, "oz_sales":True}

        if config:
            ACCOUNTS = config.get("accounts", [])
            p = config.get("period", {})
            if p and p.get("dateFrom"): d_f, d_t = p.get("dateFrom"), p.get("dateTo")
            s = config.get("settings", {})
            if s: SETTINGS = s
        else:
            time.sleep(60); continue

        try: requests.post(GAS_WEBAPP_URL, json={"secret":SECRET_KEY, "type":"CLEAR_BUFFERS"}, timeout=10)
        except: pass
        time.sleep(1)

        for acc in ACCOUNTS:
            name = acc.get('name')
            try:
                log(f"--> {name}")
                if SETTINGS.get("oz_cards", True):
                    data = fetch_cards(acc['client_id'], acc['api_key'], name)
                    if data: send_to_gas("OZ_CARDS_PY", data)
                if SETTINGS.get("oz_stock", True):
                    data = fetch_stocks(acc['client_id'], acc['api_key'], name)
                    if data: send_to_gas("OZ_STOCK_PY", data)
                if SETTINGS.get("oz_sales", True):
                    data = fetch_sales(acc['client_id'], acc['api_key'], d_f, d_t, name)
                    if data: send_to_gas("OZ_SALES_PY", data)
            except Exception as e: log(f"Err {name}: {e}", "ERR")
        
        log(f"Сон {UPDATE_INTERVAL_HOURS} ч...")
        time.sleep(UPDATE_INTERVAL_HOURS * 3600)