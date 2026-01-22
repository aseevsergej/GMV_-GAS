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

def get_server_load():
    if not PSUTIL_OK: return {"cpu": 0, "ram": 0}
    try:
        return {"cpu": psutil.cpu_percent(), "ram": psutil.virtual_memory().percent}
    except: return {"cpu": 0, "ram": 0}

def log(msg, type="LOG_VPS"):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")
    if GAS_WEBAPP_URL:
        try:
            payload = {"secret": SECRET_KEY, "type": type, "msg": f"{msg}", "server_info": get_server_load()}
            requests.post(GAS_WEBAPP_URL, json=payload, timeout=5)
        except: pass

def get_config_from_gas():
    log("Запрос настроек...")
    if not GAS_WEBAPP_URL: return None
    try:
        r = requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "GET_CONFIG"}, timeout=30)
        if r.status_code == 200: return r.json()
    except Exception as e: log(f"Err Config: {e}", "ERR")
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
    except Exception as e: log(f"Err Send: {e}", "ERR")

# === ВАЖНО: Добавили User-Agent ===
def get_headers(cid, key):
    return {
        "Client-Id": str(cid).strip(), 
        "Api-Key": str(key).strip(), 
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; OzonMfactorBot/1.0)"
    }

def fetch_cards(cid, key, acc_name):
    log("Загрузка товаров (v2/product/list)...")
    items = []
    headers = get_headers(cid, key)
    last_id = ""
    
    URL_LIST = "https://api-seller.ozon.ru/v2/product/list"
    URL_INFO = "https://api-seller.ozon.ru/v2/product/info/list"

    while True:
        payload = {"filter": {"visibility": "ALL"}, "limit": 100}
        if last_id: payload["last_id"] = str(last_id)
        
        try:
            r = requests.post(URL_LIST, headers=headers, json=payload)
            if r.status_code != 200:
                log(f"Товары 404/Err? Код: {r.status_code}. Тело: {r.text[:50]}", "ERR")
                break
                
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            ids = [int(x.get("product_id")) for x in data]
            info_map = {}
            try:
                r_info = requests.post(URL_INFO, headers=headers, json={"product_id": ids})
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
                    
                    # Цены
                    old_p = float(full.get("old_price") or 0)
                    mkt_p = float(full.get("marketing_price") or 0)
                    buy_p = float(full.get("price") or 0)

                    items.append([acc_name, primary, ozon_id, offer_id, "No Brand", cat, name, old_p, mkt_p, buy_p, buy_p])
                except: continue

            last_item = data[-1]
            last_id = str(last_item.get("product_id"))
            if len(data) < 100: break
        except: break
            
    log(f"Найдено товаров: {len(items)}")
    return items

def fetch_stocks(cid, key, acc_name):
    log("Загрузка остатков (v3/product/info/stocks)...")
    items = []
    headers = get_headers(cid, key)
    last_id = ""
    URL_STOCK = "https://api-seller.ozon.ru/v3/product/info/stocks"
    
    while True:
        # ИСПРАВЛЕНО: для v3 filter должен быть пустым, а не visibility:ALL
        payload = {"filter": {}, "limit": 100}
        if last_id: payload["last_id"] = str(last_id)

        try:
            r = requests.post(URL_STOCK, headers=headers, json=payload)
            if r.status_code != 200:
                log(f"Остатки Err {r.status_code}: {r.text[:50]}", "ERR")
                break
                
            res = r.json().get("result", {})
            data = res.get("items", [])
            if not data: break
            
            for prod in data:
                try:
                    offer_id = prod.get("offer_id", "")
                    for stock in prod.get("stocks", []):
                        st_type = stock.get("type", "")
                        cnt = stock.get("present", 0)
                        if cnt > 0:
                            items.append([acc_name, f"Ozon {st_type.upper()}", offer_id, cnt])
                except: continue
            
            last_id = res.get("last_id", "")
            if not last_id or len(data) < 100: break
        except: break
            
    log(f"Найдено остатков: {len(items)}")
    return items

def fetch_sales(cid, key, date_from, date_to, acc_name):
    log(f"Загрузка продаж ({date_from})...")
    items = []
    headers = get_headers(cid, key)
    page = 1
    
    while True:
        try:
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=headers, json={
                "filter": { "since": f"{date_from}T00:00:00Z", "to": f"{date_to}T23:59:59Z" },
                "limit": 1000, "page": page,
                "with": {"analytics_data": True, "financial_data": True}
            })
            if r.status_code != 200: break
            res = r.json().get("result", [])
            if not res: break
            for p in res:
                try:
                    created = p.get("created_at", "")[:10]
                    status = str(p.get("status", "")).lower()
                    typ = "Отмена" if "cancelled" in status else "Продажа"
                    fin = p.get("financial_data") or {}
                    an = p.get("analytics_data") or {}
                    fin_prods = {x.get('product_id'): x for x in fin.get('products', [])}
                    wh_from = an.get("warehouse_name") or "Неизвестно"
                    wh_to = an.get("region") or "Неизвестно"
                    
                    for prod in p.get("products", []):
                        sku = prod.get("sku")
                        fp = fin_prods.get(sku, {})
                        price = float(fp.get('client_price') or prod.get('price') or 0)
                        items.append([acc_name, created, typ, prod.get("offer_id"), str(sku), 1, price, wh_from, wh_to])
                except: continue
            if len(res) < 1000: break
            page += 1
            time.sleep(0.2)
        except: break
    log(f"Найдено продаж: {len(items)}")
    return items

if __name__ == "__main__":
    log("=== ЗАПУСК v130 (HEADERS FIX) ===")
    if GAS_WEBAPP_URL:
        try: requests.post(GAS_WEBAPP_URL, json={"secret":SECRET_KEY, "type":"INIT_VPS"}, timeout=10)
        except: pass

    while True:
        config = get_config_from_gas()
        
        # Умный дефолт
        now = datetime.datetime.now()
        d_f = (now - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        d_t = now.strftime("%Y-%m-%d")
        source_msg = "АВАРИЙНЫЙ (Нет связи)"
        
        ACCOUNTS = []
        SETTINGS = {"oz_cards":True, "oz_stock":True, "oz_sales":True}

        if config:
            ACCOUNTS = config.get("accounts", [])
            p = config.get("period", {})
            if p and p.get("dateFrom"):
                d_f, d_t = p.get("dateFrom"), p.get("dateTo")
                source_msg = "ТАБЛИЦА"
            SETTINGS = config.get("settings") or SETTINGS
        else:
            log("Жду конфиг...", "WARN")
            time.sleep(60)
            continue

        log(f"Период: {d_f} - {d_t} | Источник: {source_msg}")
        
        try: requests.post(GAS_WEBAPP_URL, json={"secret":SECRET_KEY, "type":"CLEAR_BUFFERS"}, timeout=10)
        except: pass
        time.sleep(1)

        for acc in ACCOUNTS:
            name = acc.get('name')
            cid, key = acc.get('client_id'), acc.get('api_key')
            
            try:
                log(f"--> {name}")
                if SETTINGS.get("oz_cards", True):
                    data = fetch_cards(cid, key, name)
                    if data: send_to_gas("OZ_CARDS_PY", data)
                
                if SETTINGS.get("oz_stock", True):
                    data = fetch_stocks(cid, key, name)
                    if data: send_to_gas("OZ_STOCK_PY", data)
                
                if SETTINGS.get("oz_sales", True):
                    data = fetch_sales(cid, key, d_f, d_t, name)
                    if data: send_to_gas("OZ_SALES_PY", data)
            except Exception as e: log(f"Err {name}: {e}", "ERR")
        
        log(f"Сон {UPDATE_INTERVAL_HOURS} ч...")
        time.sleep(UPDATE_INTERVAL_HOURS * 3600)