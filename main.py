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
            requests.post(GAS_WEBAPP_URL, json=payload, timeout=3)
        except: pass

def get_config_from_gas():
    log("Соединение с Таблицей...")
    if not GAS_WEBAPP_URL: return None
    try:
        # Увеличили таймаут
        r = requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "GET_CONFIG"}, timeout=30)
        
        if r.status_code != 200:
            log(f"Таблица ответила кодом {r.status_code}. Это ошибка.", "ERR")
            return None
            
        # ПРОВЕРКА: Что внутри?
        try:
            data = r.json()
            # Если статус не ок - выводим почему
            if data.get("status") != "ok":
                log(f"Таблица отклонила запрос: {data}", "ERR")
                return None
            return data
        except:
            # Если это HTML (например страница входа) - мы увидим это
            log(f"Таблица прислала не JSON! Начало ответа: {r.text[:200]}", "ERR")
            return None
            
    except Exception as e: 
        log(f"Ошибка сети с Таблицей: {e}", "ERR")
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
            time.sleep(1)
        log("Отправка завершена.")
    except Exception as e: log(f"Сбой отправки: {e}", "ERR")

# === МАСКИРОВКА ПОД POSTMAN ===
def get_headers(cid, key):
    return {
        "Client-Id": str(cid).strip(), 
        "Api-Key": str(key).strip(), 
        "Content-Type": "application/json",
        "User-Agent": "PostmanRuntime/7.32.3", 
        "Accept": "*/*",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive"
    }

def fetch_cards(cid, key, acc_name):
    URL = "https://api-seller.ozon.ru/v2/product/list"
    URL_INFO = "https://api-seller.ozon.ru/v2/product/info/list"
    log(f"Товары: {URL}")
    
    items = []
    headers = get_headers(cid, key)
    last_id = ""
    
    while True:
        # Вернули visibility: ALL, так как это стандарт
        payload = {"filter": {"visibility": "ALL"}, "limit": 100}
        if last_id: payload["last_id"] = str(last_id)
        
        try:
            r = requests.post(URL, headers=headers, json=payload)
            if r.status_code != 200:
                log(f"Сбой Товаров {r.status_code}: {r.text[:100]}", "ERR")
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
                    elif full.get("images") and len(full.get("images")) > 0: primary = full.get("images")[0]
                    if isinstance(primary, dict): primary = primary.get("file_name", "")

                    brand = "No Brand"
                    for a in full.get("attributes", []):
                        if a.get("attribute_id") in [85, 31]:
                            if a.get("values"): brand = a.get("values")[0].get("value", "")
                            break
                    
                    old_p = float(full.get("old_price") or 0)
                    mkt_p = float(full.get("marketing_price") or 0)
                    buy_p = float(full.get("price") or 0)

                    items.append([acc_name, primary, ozon_id, offer_id, brand, cat, name, old_p, mkt_p, buy_p, buy_p])
                except: continue

            last_item = data[-1]
            last_id = str(last_item.get("product_id"))
            if len(data) < 100: break
        except Exception as e:
            log(f"Err Loop Cards: {e}", "ERR")
            break
            
    log(f"Найдено товаров: {len(items)}")
    return items

def fetch_stocks(cid, key, acc_name):
    URL = "https://api-seller.ozon.ru/v3/product/info/stocks"
    log(f"Остатки: {URL}")
    items = []
    headers = get_headers(cid, key)
    last_id = ""
    
    while True:
        payload = {"filter": {}, "limit": 100}
        if last_id: payload["last_id"] = str(last_id)

        try:
            r = requests.post(URL, headers=headers, json=payload)
            if r.status_code != 200:
                log(f"Сбой Остатков {r.status_code}: {r.text[:100]}", "ERR")
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
                    wh_from = an.get("warehouse_name") or "Неизвестно"
                    wh_to = an.get("region") or "Неизвестно"
                    
                    for prod in p.get("products", []):
                        sku = prod.get("sku")
                        price = float(prod.get('price') or 0) # Fallback price
                        items.append([acc_name, created, typ, prod.get("offer_id"), str(sku), 1, price, wh_from, wh_to])
                except: continue
            if len(res) < 1000: break
            page += 1
            time.sleep(0.2)
        except: break
    log(f"Найдено продаж: {len(items)}")
    return items

if __name__ == "__main__":
    log("=== ЗАПУСК v132 (MIMIC BROWSER) ===")
    
    # 1. ТЕСТ СВЯЗИ С ГУГЛОМ
    if GAS_WEBAPP_URL:
        try: requests.post(GAS_WEBAPP_URL, json={"secret":SECRET_KEY, "type":"INIT_VPS"}, timeout=5)
        except: pass

    while True:
        # 2. ПОЛУЧЕНИЕ НАСТРОЕК
        config = get_config_from_gas()
        
        now = datetime.datetime.now()
        # Дефолтные даты (7 дней)
        d_f = (now - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        d_t = now.strftime("%Y-%m-%d")
        source = "АВАРИЙНЫЙ (Гугл молчит)"
        
        ACCOUNTS = []
        SETTINGS = {"oz_cards":True, "oz_stock":True, "oz_sales":True}

        if config:
            ACCOUNTS = config.get("accounts", [])
            p = config.get("period", {})
            # Проверяем, пришли ли даты
            if p and p.get("dateFrom"):
                d_f, d_t = p.get("dateFrom"), p.get("dateTo")
                source = "ТАБЛИЦА"
            else:
                source = "ТАБЛИЦА (Но даты пусты)"
            
            s = config.get("settings", {})
            if s: SETTINGS = s
        else:
            log("Жду конфиг (повтор через 60с)...", "WARN")
            time.sleep(60)
            continue

        log(f"Период: {d_f} - {d_t} | Источник: {source}")
        
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