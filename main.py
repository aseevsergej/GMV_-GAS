import requests
import time
import json
import datetime
import os
import sys
import psutil
from dotenv import load_dotenv

load_dotenv()

GAS_WEBAPP_URL = os.getenv("GAS_WEBAPP_URL")
SECRET_KEY = os.getenv("SECRET_KEY", "MY_SUPER_SECRET_PASSWORD_123")
UPDATE_INTERVAL_HOURS = int(os.getenv("UPDATE_INTERVAL_HOURS", 1))

def log(msg, type="LOG"):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")
    if GAS_WEBAPP_URL:
        try:
            # Добавляем инфо о нагрузке сервера в каждый лог
            cpu = psutil.cpu_percent()
            ram = psutil.virtual_memory().percent
            requests.post(GAS_WEBAPP_URL, json={
                "secret":SECRET_KEY, 
                "type": type, 
                "msg": f"VPS: {str(msg)[:500]}",
                "server_load": {"cpu": cpu, "ram": ram}
            }, timeout=5)
        except: pass

def get_config_from_gas():
    log("Запрос настроек...")
    if not GAS_WEBAPP_URL: return None
    try:
        payload = {"secret": SECRET_KEY, "type": "GET_CONFIG"}
        r = requests.post(GAS_WEBAPP_URL, json=payload, timeout=30)
        return r.json() if r.json().get("status") == "ok" else None
    except: return None

def send_to_gas(sheet_name, rows):
    if not rows or not GAS_WEBAPP_URL: return
    log(f"Отправка {len(rows)} строк в {sheet_name}...")
    try:
        chunk_size = 2000
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            payload = {"secret": SECRET_KEY, "type": "DATA", "sheetName": sheet_name, "rows": chunk}
            requests.post(GAS_WEBAPP_URL, json=payload, timeout=90)
            time.sleep(1)
        log("Отправка завершена.")
    except Exception as e:
        log(f"Сбой отправки: {e}", "ERR")

def get_headers(cid, key):
    return {"Client-Id": str(cid).strip(), "Api-Key": str(key).strip(), "Content-Type": "application/json"}

# --- ФУНКЦИИ ЗАГРУЗКИ (СТРОГО ПО ТЗ) ---

def fetch_cards(cid, key, acc_name):
    log("Загрузка товаров...")
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
                
                offer_id = full.get("offer_id") or basic.get("offer_id") or "" # Арт Наш
                ozon_id = str(pid) # Арт Ozon
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
                
                old_p = float(full.get("old_price") or 0)     # До скидки
                mkt_p = float(full.get("marketing_price") or 0) # ЛК (после скидки)
                buy_p = float(full.get("price") or 0)           # Покупателя
                # Цену Ozon Карты берем равной цене покупателя (в API явно нет поля OzonCardPrice для селлера)
                card_p = buy_p 

                # 1.Фото, 2.АртOZ, 3.АртНаш, 4.Бренд, 5.Кат, 6.Имя, 7.ДоСкидки, 8.ЛК, 9.Покуп, 10.Карта
                items.append([acc_name, primary, ozon_id, offer_id, brand, cat, name, old_p, mkt_p, buy_p, card_p])
            
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
            oid = row.get("item_code") or sku # Артикул (Наш)
            for wh in row.get("warehouses", []):
                if wh.get("item_cnt", 0) > 0:
                    # 1.Склад, 2.Артикул, 3.Остаток
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
                status = str(p.get("status", "")).lower()
                typ = "Отмена" if "cancelled" in status else "Продажа"
                
                an = p.get("analytics_data") or {}
                fin = p.get("financial_data") or {}
                fin_prods = {x.get('product_id'): x for x in fin.get('products', [])}
                
                wh_from = an.get("warehouse_name") or "Неизвестно"
                wh_to = an.get("region") or "Неизвестно"

                for prod in p.get("products", []):
                    sku = prod.get("sku")
                    fp = fin_prods.get(sku, {})
                    price = float(fp.get('client_price') or prod.get('price') or 0)
                    
                    # 1.Дата, 2.Тип, 3.АртНаш, 4.АртOZ, 5.Кол, 6.Цена, 7.СкладОт, 8.СкладДо
                    items.append([acc_name, created, typ, prod.get("offer_id"), str(sku), 1, price, wh_from, wh_to])
            if len(res) < 1000: break
            page += 1
            time.sleep(0.3)
        except: break
    return items

if __name__ == "__main__":
    log("=== ЗАПУСК v123 (Server Monitor) ===")
    while True:
        config = get_config_from_gas()
        if not config:
            log("Жду конфиг (проверьте .env)...")
            time.sleep(300)
            continue
        
        ACCOUNTS = config.get("accounts", [])
        PERIOD = config.get("period", {})
        SETTINGS = config.get("settings", {}) # Получаем настройки галочек
        
        d_f = PERIOD.get("dateFrom", "2024-01-01")
        d_t = PERIOD.get("dateTo", datetime.datetime.now().strftime("%Y-%m-%d"))

        log(f"Задание: {len(ACCOUNTS)} каб. Период: {d_f}-{d_t}")

        # Предварительная очистка
        try:
            requests.post(GAS_WEBAPP_URL, json={"secret":SECRET_KEY, "type":"CLEAR_BUFFERS"}, timeout=10)
        except: pass
        time.sleep(2)

        for acc in ACCOUNTS:
            name = acc.get('name', 'Unknown')
            cid, key = acc.get('client_id'), acc.get('api_key')
            try:
                log(f"--> {name}")
                
                # Проверяем настройки: грузить ли Товары Ozon?
                if SETTINGS.get("oz_cards", True):
                    cards = fetch_cards(cid, key, name)
                    if cards: send_to_gas("OZ_CARDS_PY", cards)
                
                if SETTINGS.get("oz_stock", True):
                    stocks = fetch_stocks(cid, key, name)
                    if stocks: send_to_gas("OZ_STOCK_PY", stocks)
                
                if SETTINGS.get("oz_sales", True):
                    sales = fetch_sales(cid, key, d_f, d_t, name)
                    if sales: send_to_gas("OZ_SALES_PY", sales)
                    
            except Exception as e: log(f"Ошибка {name}: {e}", "ERR")
        
        log(f"Сон {UPDATE_INTERVAL_HOURS} ч...")
        time.sleep(UPDATE_INTERVAL_HOURS * 3600)