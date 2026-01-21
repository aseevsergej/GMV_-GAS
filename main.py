import requests
import time
import json
import datetime
import os
import sys

# ================= ГЛАВНЫЕ НАСТРОЙКИ =================
# Скрипт будет стучаться сюда за конфигом (кабинеты + даты)
GAS_WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwxQf0sos-ed3EABhyExUEidByp7Fkhn1oNud1m0SHE2M21BcndKhkqCFjv-nHRp_7M2g/exec" 
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123"

# Как часто проверять обновления (в часах)
UPDATE_INTERVAL_HOURS = 1 
# =====================================================

def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")
    try:
        requests.post(GAS_WEBAPP_URL, json={"secret":SECRET_KEY, "type":"LOG", "msg": f"VPS: {str(msg)[:500]}"}, timeout=5)
    except: pass

# --- МОЗГ: ЗАПРОС НАСТРОЕК ИЗ ТАБЛИЦЫ ---
def get_config_from_gas():
    log("Запрос настроек и периода из GAS...")
    try:
        payload = {"secret": SECRET_KEY, "type": "GET_CONFIG"}
        r = requests.post(GAS_WEBAPP_URL, json=payload, timeout=30)
        data = r.json()
        if data.get("status") == "ok":
            return data # Возвращает {accounts: [...], period: {dateFrom:..., dateTo:...}}
        else:
            log(f"GAS вернул ошибку: {data}")
            return None
    except Exception as e:
        log(f"Не удалось получить конфиг: {e}")
        return None

def clear_sheet(sheet_name):
    try:
        requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "CLEAR", "sheetName": sheet_name}, timeout=10)
    except: pass

def send_to_gas(sheet_name, rows):
    if not rows: return
    log(f"Отправка {len(rows)} строк в {sheet_name}...")
    try:
        chunk_size = 3000
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            payload = {"secret": SECRET_KEY, "type": "DATA", "sheetName": sheet_name, "rows": chunk}
            requests.post(GAS_WEBAPP_URL, json=payload, timeout=90)
            time.sleep(1)
        log("Отправка завершена.")
    except Exception as e:
        log(f"Сбой отправки: {e}")

def get_headers(cid, key):
    return {
        "Client-Id": str(cid).strip(), 
        "Api-Key": str(key).strip(),
        "Content-Type": "application/json", 
        "User-Agent": "Mozilla/5.0 (compatible; OzonVPS/1.0)"
    }

# --- ФУНКЦИИ API OZON ---
def fetch_cards(cid, key):
    log("Загрузка карточек...")
    items = []
    headers = get_headers(cid, key)
    last_id = ""
    while True:
        payload = {"filter": {"visibility": "ALL"}, "limit": 100}
        if last_id: payload["last_id"] = str(last_id)
        try:
            r = requests.post("https://api-seller.ozon.ru/v2/product/list", headers=headers, json=payload)
            if r.status_code != 200: break
            data = r.json().get("result", {}).get("items", [])
            if not data: break
            
            ids = [int(x.get("product_id")) for x in data]
            info_map = {}
            # Детали
            try:
                r_info = requests.post("https://api-seller.ozon.ru/v2/product/info/list", headers=headers, json={"product_id": ids})
                if r_info.status_code == 200:
                    for i in r_info.json().get("result", {}).get("items", []): info_map[i.get("id")] = i
            except: pass

            for basic in data:
                pid = int(basic.get("product_id"))
                full = info_map.get(pid, {})
                offer_id = full.get("offer_id") or basic.get("offer_id") or ""
                ozon_id = str(pid)
                name = full.get("name") or "Товар"
                cat = str(full.get("category_id", ""))
                primary = full.get("primary_image") or ""
                if not primary and full.get("images"):
                     imgs = full.get("images")
                     primary = imgs[0] if isinstance(imgs[0], str) else imgs[0].get("file_name", "")
                brand = ""
                for a in full.get("attributes", []):
                    if a.get("attribute_id") in [85, 31]:
                        vals = a.get("values", [])
                        if vals: brand = vals[0].get("value", "")
                        break
                def gp(k): return float(full.get(k) or full.get("price", {}).get(k) or 0)
                items.append([primary, ozon_id, offer_id, brand, cat, name, gp("old_price"), gp("price"), gp("marketing_price"), gp("marketing_price")])
            
            last_item = data[-1]
            last_id = str(last_item.get("product_id"))
            if len(data) < 100: break
        except: break
    return items

def fetch_stocks(cid, key):
    log("Загрузка остатков...")
    items = []
    headers = get_headers(cid, key)
    try:
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=headers, json={"limit": 1000, "offset": 0})
        rows = r.json().get("result", {}).get("rows", [])
        for row in rows:
            sku = str(row.get("sku", ""))
            oid = row.get("item_code") or sku
            for wh in row.get("warehouses", []):
                if wh.get("item_cnt", 0) > 0:
                    items.append([wh.get("warehouse_name"), oid, wh.get("item_cnt")])
    except: pass
    return items

def fetch_sales(cid, key, date_from, date_to):
    log(f"Загрузка продаж ({date_from} - {date_to})...")
    items = []
    headers = get_headers(cid, key)
    page = 1
    # Приводим даты к формату Ozon
    dt_from = f"{date_from}T00:00:00Z"
    dt_to = f"{date_to}T23:59:59Z"
    
    while True:
        try:
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=headers, json={
                "filter": { "since": dt_from, "to": dt_to }, "limit": 1000, "page": page,
                "with": {"analytics_data": True, "financial_data": True}
            })
            if r.status_code != 200: break
            res = r.json().get("result", [])
            if not res: break
            for p in res:
                created = p.get("created_at", "")[:10]
                typ = "Отмена" if "cancelled" in str(p.get("status", "")).lower() else "Продажа"
                an = p.get("analytics_data") or {}
                fin = p.get("financial_data") or {}
                fin_prods = {x.get('product_id'): x for x in fin.get('products', [])}
                for prod in p.get("products", []):
                    sku = prod.get("sku")
                    fp = fin_prods.get(sku, {})
                    price = float(fp.get('client_price') or prod.get('price') or 0)
                    items.append([created, typ, prod.get("offer_id"), str(sku), 1, price, an.get("warehouse_name"), an.get("region")])
            if len(res) < 1000: break
            page += 1
            time.sleep(0.3)
        except: break
    return items

# --- ГЛАВНЫЙ ЦИКЛ РОБОТА ---
if __name__ == "__main__":
    log("=== ЗАПУСК БОТА v119 (GAS CONFIG) ===")
    
    while True:
        # 1. СПРАШИВАЕМ У ТАБЛИЦЫ, ЧТО ДЕЛАТЬ
        config = get_config_from_gas()
        
        if not config:
            log("Таблица не ответила или вернула ошибку. Жду 5 минут...")
            time.sleep(300)
            continue
        
        ACCOUNTS = config.get("accounts", [])
        PERIOD = config.get("period", {})
        
        # Получаем даты из ответа таблицы
        d_f = PERIOD.get("dateFrom", "2024-01-01")
        d_t = PERIOD.get("dateTo", datetime.datetime.now().strftime("%Y-%m-%d"))

        log(f"Получено задание: Кабинетов={len(ACCOUNTS)}, Период={d_f}-{d_t}")

        # Очистка листов (можно отключить, если хотите копить историю)
        clear_sheet("OZ_CARDS_PY")
        clear_sheet("OZ_STOCK_PY")
        clear_sheet("OZ_SALES_PY")
        time.sleep(2)

        for acc in ACCOUNTS:
            name = acc.get('name', 'Без имени')
            cid = acc.get('client_id')
            key = acc.get('api_key')

            try:
                log(f"--> Обработка кабинета: {name}")
                
                cards = fetch_cards(cid, key)
                if cards: send_to_gas("OZ_CARDS_PY", cards)
                
                stocks = fetch_stocks(cid, key)
                if stocks: send_to_gas("OZ_STOCK_PY", stocks)
                
                # ВАЖНО: Передаем в функцию продаж даты из таблицы!
                sales = fetch_sales(cid, key, d_f, d_t)
                if sales: send_to_gas("OZ_SALES_PY", sales)
                
            except Exception as e:
                log(f"Ошибка в кабинете {name}: {e}")
        
        log(f"Цикл завершен. Следующий запуск через {UPDATE_INTERVAL_HOURS} ч...")
        time.sleep(UPDATE_INTERVAL_HOURS * 3600)