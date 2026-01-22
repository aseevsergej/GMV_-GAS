import requests
import time
import json
import datetime
import os
import sys
from dotenv import load_dotenv

# Попытка импорта psutil
try:
    import psutil
    PSUTIL_OK = True
except ImportError:
    PSUTIL_OK = False

load_dotenv()

GAS_WEBAPP_URL = os.getenv("GAS_WEBAPP_URL")
SECRET_KEY = os.getenv("SECRET_KEY", "MY_SUPER_SECRET_PASSWORD_123")
# Интервал обновления (по умолчанию 1 час)
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
            payload = {
                "secret": SECRET_KEY, 
                "type": type, 
                "msg": f"{msg}",
                "server_info": get_server_load()
            }
            # Короткий таймаут для логов
            requests.post(GAS_WEBAPP_URL, json=payload, timeout=5)
        except: pass

def get_config_from_gas():
    log("Запрос настроек...")
    if not GAS_WEBAPP_URL: return None
    try:
        payload = {"secret": SECRET_KEY, "type": "GET_CONFIG"}
        r = requests.post(GAS_WEBAPP_URL, json=payload, timeout=30)
        
        if r.status_code != 200:
            log(f"GAS HTTP {r.status_code}", "ERR")
            return None
            
        try:
            return r.json()
        except Exception as e:
            log(f"Ошибка JSON от GAS: {e}. Ответ: {r.text[:100]}", "ERR")
            return None
            
    except Exception as e:
        log(f"Сбой соединения Config: {e}", "ERR")
        return None

def send_to_gas(sheet_name, rows):
    if not rows: return
    log(f"Отправка {len(rows)} строк в {sheet_name}...")
    try:
        chunk_size = 1000
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            payload = {
                "secret": SECRET_KEY, 
                "type": "DATA", 
                "sheetName": sheet_name, 
                "rows": chunk,
                "server_info": get_server_load()
            }
            r = requests.post(GAS_WEBAPP_URL, json=payload, timeout=90)
            if r.status_code != 200:
                log(f"Ошибка отправки пакета: {r.text[:50]}", "ERR")
            time.sleep(1.0)
        log("Отправка завершена.")
    except Exception as e:
        log(f"Сбой отправки: {e}", "ERR")

def get_headers(cid, key):
    return {
        "Client-Id": str(cid).strip(), 
        "Api-Key": str(key).strip(), 
        "Content-Type": "application/json"
    }

# --- OZON API ---

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
            
            # ДИАГНОСТИКА "Extra data"
            try:
                data_json = r.json()
            except Exception as e:
                log(f"CRITICAL: Озон вернул не JSON! Код: {r.status_code}. Текст: {r.text[:200]}", "ERR")
                break

            data = data_json.get("result", {}).get("items", [])
            if not data: break
            
            # Получаем детали
            ids = [int(x.get("product_id")) for x in data]
            info_map = {}
            
            # Запрос деталей
            try:
                r_info = requests.post("https://api-seller.ozon.ru/v2/product/info/list", headers=headers, json={"product_id": ids})
                info_data = r_info.json().get("result", {}).get("items", [])
                for i in info_data: info_map[i.get("id")] = i
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
                    try:
                        primary = full.get("primary_image") or ""
                        if not primary and full.get("images"):
                            imgs = full.get("images")
                            primary = imgs[0] if isinstance(imgs[0], str) else imgs[0].get("file_name", "")
                    except: pass

                    brand = ""
                    try:
                        for a in full.get("attributes", []):
                            if a.get("attribute_id") in [85, 31]:
                                vals = a.get("values", [])
                                if vals: brand = vals[0].get("value", "")
                                break
                    except: pass
                    
                    old_p = float(full.get("old_price") or 0)
                    mkt_p = float(full.get("marketing_price") or 0)
                    buy_p = float(full.get("price") or 0)
                    card_p = buy_p 

                    # [ИмяКабинета, Фото, АртOZ, АртНаш, Бренд, Кат, Имя, Ц.До, Ц.ЛК, Ц.Покуп, Ц.Карта]
                    items.append([acc_name, primary, ozon_id, offer_id, brand, cat, name, old_p, mkt_p, buy_p, card_p])
                except Exception as ex_item:
                    continue # Пропускаем битый товар, идем дальше

            last_item = data[-1]
            last_id = str(last_item.get("product_id"))
            if len(data) < 100: break
            
        except Exception as e:
            log(f"Ошибка цикла товаров: {e}", "ERR")
            break
            
    log(f"Найдено товаров: {len(items)}")
    return items

def fetch_stocks(cid, key, acc_name):
    log("Загрузка остатков...")
    items = []
    headers = get_headers(cid, key)
    try:
        r = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=headers, json={"limit": 1000, "offset": 0})
        
        # Проверка JSON
        try:
            res_json = r.json()
        except:
            log(f"Остатки: Невалидный JSON от Ozon. {r.text[:100]}", "ERR")
            return []

        rows = res_json.get("result", {}).get("rows", [])
        
        for row in rows:
            try:
                sku = str(row.get("sku", ""))
                oid = str(row.get("item_code") or sku)
                for wh in row.get("warehouses", []):
                    cnt = wh.get("item_cnt", 0)
                    if cnt > 0:
                        # [ИмяКабинета, Склад, Артикул, Остаток]
                        items.append([acc_name, wh.get("warehouse_name"), oid, cnt])
            except: continue
            
    except Exception as e: log(f"Ошибка остатков: {e}", "ERR")
    log(f"Найдено остатков: {len(items)}")
    return items

def fetch_sales(cid, key, date_from, date_to, acc_name):
    log(f"Загрузка продаж ({date_from})...")
    items = []
    headers = get_headers(cid, key)
    page = 1
    dt_from = f"{date_from}T00:00:00Z"
    dt_to = f"{date_to}T23:59:59Z"
    
    while True:
        try:
            r = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=headers, json={
                "filter": { "since": dt_from, "to": dt_to }, "limit": 1000, "page": page,
                "with": {"analytics_data": True, "financial_data": True}
            })
            
            try: res_json = r.json()
            except: break
            
            res = res_json.get("result", [])
            if not res: break
            
            for p in res:
                try:
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
                        # [ИмяКабинета, Дата, Тип, АртНаш, АртOZ, Кол, Цена, СкладОт, СкладДо]
                        items.append([acc_name, created, typ, prod.get("offer_id"), str(sku), 1, price, wh_from, wh_to])
                except: continue
                
            if len(res) < 1000: break
            page += 1
            time.sleep(0.3)
        except: break
        
    log(f"Найдено продаж: {len(items)}")
    return items

if __name__ == "__main__":
    log("=== ЗАПУСК v128 (DEBUG MODE) ===")
    
    # Сразу шлем статус
    if GAS_WEBAPP_URL:
        try: requests.post(GAS_WEBAPP_URL, json={"secret":SECRET_KEY, "type":"INIT_VPS"}, timeout=10)
        except: pass

    while True:
        config = get_config_from_gas()
        
        # ДЕФОЛТНЫЕ НАСТРОЙКИ (ЕСЛИ GAS МОЛЧИТ)
        ACCOUNTS = []
        SETTINGS = {"oz_cards":True, "oz_stock":True, "oz_sales":True}
        
        # Рассчитываем "умный дефолт" - последние 7 дней
        now = datetime.datetime.now()
        def_from = (now - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        def_to = now.strftime("%Y-%m-%d")
        
        d_f = def_from
        d_t = def_to

        if config:
            ACCOUNTS = config.get("accounts", [])
            p = config.get("period", {})
            # Если в конфиге есть даты - берем их, иначе оставляем 7 дней
            if p and p.get("dateFrom"):
                d_f = p.get("dateFrom")
                d_t = p.get("dateTo")
            
            s = config.get("settings", {})
            if s: SETTINGS = s
        else:
            log("Нет связи с GAS или конфиг пуст. Жду 60 сек...", "WARN")
            time.sleep(60)
            continue

        log(f"Задание: {len(ACCOUNTS)} каб. Период: {d_f} - {d_t}")
        
        # Очистка буферов
        try: requests.post(GAS_WEBAPP_URL, json={"secret":SECRET_KEY, "type":"CLEAR_BUFFERS"}, timeout=10)
        except: pass
        time.sleep(2)

        for acc in ACCOUNTS:
            name = acc.get('name', 'Unknown')
            cid = acc.get('client_id')
            key = acc.get('api_key')
            
            if not cid or not key:
                log(f"Пропуск {name}: нет ключей", "WARN")
                continue

            try:
                log(f"--> {name}")
                
                if SETTINGS.get("oz_cards", True):
                    cards = fetch_cards(cid, key, name)
                    if cards: send_to_gas("OZ_CARDS_PY", cards)
                
                if SETTINGS.get("oz_stock", True):
                    stocks = fetch_stocks(cid, key, name)
                    if stocks: send_to_gas("OZ_STOCK_PY", stocks)
                
                if SETTINGS.get("oz_sales", True):
                    sales = fetch_sales(cid, key, d_f, d_t, name)
                    if sales: send_to_gas("OZ_SALES_PY", sales)
                    
            except Exception as e: log(f"Ошибка обработки {name}: {e}", "ERR")
        
        log(f"Готово. Сон {UPDATE_INTERVAL_HOURS} ч...")
        time.sleep(UPDATE_INTERVAL_HOURS * 3600)