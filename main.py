import requests
import time
import json
import datetime
import os
import sys
from dotenv import load_dotenv

# Загружаем настройки из .env файла
load_dotenv()

# ================= НАСТРОЙКИ (ТЕПЕРЬ ИЗ ФАЙЛА) =================
GAS_WEBAPP_URL = os.getenv("GAS_WEBAPP_URL")
SECRET_KEY = os.getenv("SECRET_KEY", "MY_SUPER_SECRET_PASSWORD_123")
UPDATE_INTERVAL_HOURS = int(os.getenv("UPDATE_INTERVAL_HOURS", 1))

# Проверка, что настройки загрузились
if not GAS_WEBAPP_URL:
    print("ОШИБКА: Не найден файл .env или переменная GAS_WEBAPP_URL!")
    # Скрипт не остановится, но будет писать ошибку в консоль. 
    # Логирование на GAS работать не будет без URL.
# ===============================================================

def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")
    if GAS_WEBAPP_URL:
        try:
            requests.post(GAS_WEBAPP_URL, json={"secret":SECRET_KEY, "type":"LOG", "msg": f"VPS: {str(msg)[:500]}"}, timeout=5)
        except: pass

def get_config_from_gas():
    log("Запрос настроек из GAS...")
    if not GAS_WEBAPP_URL: return None
    try:
        payload = {"secret": SECRET_KEY, "type": "GET_CONFIG"}
        r = requests.post(GAS_WEBAPP_URL, json=payload, timeout=30)
        return r.json() if r.json().get("status") == "ok" else None
    except Exception as e:
        log(f"Ошибка конфига: {e}")
        return None

def clear_sheet(sheet_name):
    if not GAS_WEBAPP_URL: return
    try:
        requests.post(GAS_WEBAPP_URL, json={"secret": SECRET_KEY, "type": "CLEAR", "sheetName": sheet_name}, timeout=10)
    except: pass

def send_to_gas(sheet_name, rows):
    if not rows or not GAS_WEBAPP_URL: return
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
        "Client-Id": str(cid).strip(), "Api-Key": str(key).strip(),
        "Content-Type": "application/json", "User-Agent": "Mozilla/5.0 (compatible; OzonVPS/1.0)"
    }

# --- ФУНКЦИИ ---
def fetch_cards(cid, key, acc_name):
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
                items.append([acc_name, primary, ozon_id, offer_id, brand, cat, name, gp("old_price"), gp("price"), gp("marketing_price"), gp("marketing_price")])
            
            last_item = data[-1]
            last_id = str(last_item.get("product_id"))
            if len(data) < 100: break
        except: break
    return items

def fetch_stocks(cid, key, acc_name):
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
                    items.append([acc_name, wh.get("warehouse_name"), oid, wh.get("item_cnt")])
    except: pass
    return items

def fetch_sales(cid, key, date_from, date_to, acc_name):
    log(f"Загрузка продаж ({date_from} - {date_to})...")
    items = []
    headers = get_headers(cid, key)
    page = 1
    dt_from, dt_to = f"{date_from}T00:00:00Z", f"{date_to}T23:59:59Z"
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
                    items.append([acc_name, created, typ, prod.get("offer_id"), str(sku), 1, price, an.get("warehouse_name"), an.get("region")])
            if len(res) < 1000: break
            page += 1
            time.sleep(0.3)
        except: break
    return items

if __name__ == "__main__":
    log("=== ЗАПУСК БОТА v121 (ENV) ===")
    while True:
        config = get_config_from_gas()
        if not config:
            log("Жду конфиг (проверьте .env)...")
            time.sleep(300)
            continue
        
        ACCOUNTS = config.get("accounts", [])
        PERIOD = config.get("period", {})
        d_f = PERIOD.get("dateFrom", "2024-01-01")
        d_t = PERIOD.get("dateTo", datetime.datetime.now().strftime("%Y-%m-%d"))

        log(f"Задание: {len(ACCOUNTS)} кабинетов. Период: {d_f} - {d_t}")

        clear_sheet("OZ_CARDS_PY")
        clear_sheet("OZ_STOCK_PY")
        clear_sheet("OZ_SALES_PY")
        time.sleep(2)

        for acc in ACCOUNTS:
            name = acc.get('name', 'Unknown')
            cid, key = acc.get('client_id'), acc.get('api_key')
            try:
                log(f"--> {name}")
                cards = fetch_cards(cid, key, name)
                if cards: send_to_gas("OZ_CARDS_PY", cards)
                stocks = fetch_stocks(cid, key, name)
                if stocks: send_to_gas("OZ_STOCK_PY", stocks)
                sales = fetch_sales(cid, key, d_f, d_t, name)
                if sales: send_to_gas("OZ_SALES_PY", sales)
            except Exception as e: log(f"Ошибка {name}: {e}")
        
        log(f"Сон {UPDATE_INTERVAL_HOURS} ч...")
        time.sleep(UPDATE_INTERVAL_HOURS * 3600)