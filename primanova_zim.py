"""
primanova_zim.py — Генерация ZIM-отчёта для Primanova на основе данных парсинга.

Использование:
    python primanova_zim.py --data <путь_к_файлу.csv или .xlsx>

Скрипт:
1. Читает данные парсинга из CSV или Excel-файла.
2. Получает рыночные данные по категориям из Ozon Analytics API.
3. Формирует ZIM-отчёт (структура аналогичная Primanova).
4. Отправляет данные в Google Apps Script (GAS) по листам:
   - ZIM_CATALOG     — полный каталог товаров
   - ZIM_MARKET      — рыночные данные по категориям
   - ZIM_SUMMARY     — сводка по бренду

Обязательные переменные окружения (.env):
    GAS_WEBAPP_URL   — URL вашего GAS веб-приложения
    SECRET_KEY       — секретный ключ для GAS

Опциональные переменные окружения:
    OZON_CLIENT_ID   — Client-ID Ozon (для получения рыночных данных)
    OZON_API_KEY     — Api-Key Ozon (для получения рыночных данных)
    PROXY_URL        — прокси (http://user:pass@host:port)
"""

import argparse
import csv
import datetime
import os
import sys
import time

import requests
from dotenv import load_dotenv

try:
    import openpyxl
    OPENPYXL_OK = True
except ImportError:
    OPENPYXL_OK = False

try:
    import pandas as pd
    PANDAS_OK = True
except ImportError:
    PANDAS_OK = False

load_dotenv()

GAS_WEBAPP_URL = os.getenv("GAS_WEBAPP_URL")
SECRET_KEY = os.getenv("SECRET_KEY")
OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID", "")
OZON_API_KEY = os.getenv("OZON_API_KEY", "")
PROXY_URL = os.getenv("PROXY_URL")

BRAND_NAME = "Primanova"

# Колонки CSV/Excel входного файла (можно переопределить через --columns)
DEFAULT_COLUMNS = [
    "account",       # Аккаунт / магазин
    "image",         # Ссылка на изображение
    "ozon_id",       # ID товара на Ozon
    "offer_id",      # Артикул продавца
    "brand",         # Бренд
    "category_id",   # ID категории
    "name",          # Название товара
    "old_price",     # Старая цена
    "mkt_price",     # Маркетинговая цена
    "buy_price",     # Цена покупки / текущая цена
    "price",         # Итоговая цена (дублирует buy_price для совместимости)
]

session = requests.Session()
if PROXY_URL:
    session.proxies = {"http": PROXY_URL, "https": PROXY_URL}


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def log(msg):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def send_to_gas(sheet_name, rows):
    """Отправить список строк в GAS по частям."""
    if not rows:
        log(f"Нет данных для листа {sheet_name}, пропускаем.")
        return
    if not GAS_WEBAPP_URL:
        log("GAS_WEBAPP_URL не задан — вывод в консоль:")
        for r in rows[:5]:
            print(r)
        if len(rows) > 5:
            print(f"  ... ещё {len(rows) - 5} строк")
        return

    log(f"Отправка {len(rows)} строк в '{sheet_name}'...")
    chunk_size = 1000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        try:
            resp = session.post(
                GAS_WEBAPP_URL,
                json={
                    "secret": SECRET_KEY,
                    "type": "DATA",
                    "sheetName": sheet_name,
                    "rows": chunk,
                },
                timeout=90,
            )
            if resp.status_code != 200:
                log(f"  Предупреждение: GAS вернул {resp.status_code} — {resp.text[:100]}")
        except Exception as exc:
            log(f"  Ошибка отправки чанка: {exc}")
        time.sleep(0.5)
    log(f"  Отправка '{sheet_name}' завершена.")


def clear_gas_buffers():
    """Очистить буферы GAS перед загрузкой новых данных."""
    if not GAS_WEBAPP_URL:
        return
    try:
        session.post(
            GAS_WEBAPP_URL,
            json={"secret": SECRET_KEY, "type": "CLEAR_BUFFERS"},
            timeout=10,
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Чтение данных парсинга
# ---------------------------------------------------------------------------

def _rows_from_csv(path):
    """Читать строки из CSV-файла."""
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            rows.append(row)
    return rows, header


def _rows_from_xlsx(path):
    """Читать строки из Excel-файла (первый лист)."""
    if PANDAS_OK:
        df = pd.read_excel(path, dtype=str)
        df = df.fillna("")
        header = list(df.columns)
        rows = df.values.tolist()
        return rows, header

    if OPENPYXL_OK:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        all_rows = list(ws.iter_rows(values_only=True))
        wb.close()
        if not all_rows:
            return [], []
        header = [str(c) if c is not None else "" for c in all_rows[0]]
        rows = [
            [str(c) if c is not None else "" for c in row]
            for row in all_rows[1:]
        ]
        return rows, header

    log("ОШИБКА: для чтения .xlsx установите pandas или openpyxl: pip install openpyxl pandas")
    sys.exit(1)


def load_parse_data(path):
    """
    Загрузить данные парсинга из файла (.csv или .xlsx/.xls).
    Возвращает (rows, header) где rows — список списков строк.
    """
    if not os.path.exists(path):
        log(f"ОШИБКА: файл не найден: {path}")
        sys.exit(1)

    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        rows, header = _rows_from_csv(path)
    elif ext in (".xlsx", ".xls", ".xlsm"):
        rows, header = _rows_from_xlsx(path)
    else:
        log(f"ОШИБКА: неподдерживаемый формат файла: {ext}")
        sys.exit(1)

    log(f"Загружено строк: {len(rows)} (файл: {path})")
    return rows, header


# ---------------------------------------------------------------------------
# Нормализация строк к формату ZIM
# ---------------------------------------------------------------------------

def _safe_float(val):
    try:
        return float(str(val).replace(",", ".").strip())
    except (ValueError, TypeError):
        return 0.0


def normalize_rows(rows, header):
    """
    Привести строки входного файла к единому формату ZIM_CATALOG:
    [account, image, ozon_id, offer_id, brand, category_id, name,
     old_price, mkt_price, buy_price, price]

    Если заголовок совпадает с DEFAULT_COLUMNS — маппинг по позиции,
    иначе — пытаемся угадать по названию столбца.
    """
    col_map = {}
    if header:
        header_lower = [str(h).strip().lower() for h in header]
        for i, col in enumerate(header_lower):
            col_map[col] = i

    def _get(row, name, default=""):
        # Прямое имя столбца
        if name in col_map and col_map[name] < len(row):
            return str(row[col_map[name]]).strip()
        # Синонимы
        synonyms = {
            "account": ["аккаунт", "магазин", "seller", "account"],
            "image": ["image", "изображение", "фото", "photo", "img"],
            "ozon_id": ["ozon_id", "ozon id", "product_id", "id товара", "id"],
            "offer_id": ["offer_id", "артикул", "sku", "article"],
            "brand": ["brand", "бренд"],
            "category_id": ["category_id", "категория", "category", "cat_id", "cat"],
            "name": ["name", "название", "наименование", "title"],
            "old_price": ["old_price", "старая цена", "price_old"],
            "mkt_price": ["mkt_price", "маркетинговая цена", "marketing_price", "price_mkt"],
            "buy_price": ["buy_price", "цена покупки"],
            "price": ["price", "итоговая цена", "final_price"],
        }
        for syn in synonyms.get(name, []):
            if syn in col_map and col_map[syn] < len(row):
                return str(row[col_map[syn]]).strip()
        return default

    normalized = []
    for row in rows:
        if not any(str(c).strip() for c in row):
            continue  # пропускаем пустые строки
        account = _get(row, "account", BRAND_NAME)
        image = _get(row, "image")
        ozon_id = _get(row, "ozon_id")
        offer_id = _get(row, "offer_id")
        brand = _get(row, "brand", BRAND_NAME)
        category_id = _get(row, "category_id")
        name = _get(row, "name", "Товар")
        old_price = _safe_float(_get(row, "old_price"))
        mkt_price = _safe_float(_get(row, "mkt_price"))
        buy_price = _safe_float(_get(row, "buy_price"))
        price = _safe_float(_get(row, "price")) or buy_price
        normalized.append([account, image, ozon_id, offer_id, brand,
                            category_id, name, old_price, mkt_price, buy_price, price])
    log(f"Нормализовано строк каталога: {len(normalized)}")
    return normalized


# ---------------------------------------------------------------------------
# Рыночные данные по категориям (Ozon Analytics API)
# ---------------------------------------------------------------------------

def _ozon_headers():
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json",
    }


def fetch_category_tree(category_ids):
    """
    Получить дерево категорий Ozon для заданных ID.
    Возвращает словарь {category_id: category_name}.
    """
    if not OZON_CLIENT_ID or not OZON_API_KEY:
        return {}

    cat_names = {}
    for cid in set(str(c) for c in category_ids if c):
        try:
            r = session.post(
                "https://api-seller.ozon.ru/v2/category/tree",
                headers=_ozon_headers(),
                json={"category_id": int(cid), "language": "DEFAULT"},
                timeout=15,
            )
            if r.status_code == 200:
                items = r.json().get("result", [])
                for item in items:
                    cat_names[str(item.get("category_id", ""))] = item.get("title", "")
        except Exception:
            pass
        time.sleep(0.1)
    return cat_names


def fetch_market_data_by_category(category_ids, date_from, date_to):
    """
    Получить рыночные данные по категориям через Ozon Analytics API.
    Возвращает список строк для листа ZIM_MARKET.
    Формат: [category_id, category_name, date_from, date_to, revenue, orders, avg_price, items_count]
    """
    if not OZON_CLIENT_ID or not OZON_API_KEY:
        log("OZON_CLIENT_ID / OZON_API_KEY не заданы — рыночные данные пропускаются.")
        return []

    log(f"Запрос рыночных данных по {len(set(category_ids))} категориям...")
    cat_names = fetch_category_tree(category_ids)

    rows = []
    for cid in set(str(c) for c in category_ids if c):
        try:
            r = session.post(
                "https://api-seller.ozon.ru/v1/analytics/data",
                headers=_ozon_headers(),
                json={
                    "date_from": date_from,
                    "date_to": date_to,
                    "metrics": ["revenue", "ordered_units", "avg_price", "items_with_orders"],
                    "filters": [{"key": "category_id", "op": "EQ", "value": cid}],
                    "dimension": ["category_id"],
                    "limit": 1000,
                    "offset": 0,
                },
                timeout=30,
            )
            if r.status_code == 200:
                result = r.json().get("result", {})
                for item in result.get("data", []):
                    dims = item.get("dimensions", [{}])
                    cat_id = dims[0].get("id", cid) if dims else cid
                    cat_name = cat_names.get(str(cat_id), "")
                    metrics = item.get("metrics", [0, 0, 0, 0])
                    revenue = _safe_float(metrics[0]) if len(metrics) > 0 else 0.0
                    orders = int(_safe_float(metrics[1])) if len(metrics) > 1 else 0
                    avg_price = _safe_float(metrics[2]) if len(metrics) > 2 else 0.0
                    items_cnt = int(_safe_float(metrics[3])) if len(metrics) > 3 else 0
                    rows.append([cat_id, cat_name, date_from, date_to,
                                 revenue, orders, avg_price, items_cnt])
        except Exception as exc:
            log(f"  Ошибка категории {cid}: {exc}")
        time.sleep(0.2)

    log(f"Рыночных строк получено: {len(rows)}")
    return rows


# ---------------------------------------------------------------------------
# Сводка ZIM_SUMMARY
# ---------------------------------------------------------------------------

def build_summary(catalog_rows, market_rows):
    """
    Сформировать сводную таблицу ZIM_SUMMARY.
    Формат: [brand, category_id, sku_count, min_price, max_price, avg_price,
              market_revenue, market_orders, market_avg_price]
    """
    from collections import defaultdict

    brand_cat = defaultdict(list)
    for row in catalog_rows:
        # row: [account, image, ozon_id, offer_id, brand, category_id, name,
        #        old_price, mkt_price, buy_price, price]
        brand = row[4] if len(row) > 4 else BRAND_NAME
        cat_id = str(row[5]) if len(row) > 5 else ""
        price = _safe_float(row[10]) if len(row) > 10 else 0.0
        brand_cat[(brand, cat_id)].append(price)

    market_map = {}
    for row in market_rows:
        # row: [category_id, category_name, date_from, date_to,
        #        revenue, orders, avg_price, items_count]
        cat_id = str(row[0]) if row else ""
        market_map[cat_id] = row

    summary = []
    for (brand, cat_id), prices in sorted(brand_cat.items()):
        prices_valid = [p for p in prices if p > 0]
        min_p = min(prices_valid) if prices_valid else 0.0
        max_p = max(prices_valid) if prices_valid else 0.0
        avg_p = sum(prices_valid) / len(prices_valid) if prices_valid else 0.0
        mrow = market_map.get(cat_id, [])
        mkt_rev = _safe_float(mrow[4]) if len(mrow) > 4 else 0.0
        mkt_ord = int(_safe_float(mrow[5])) if len(mrow) > 5 else 0
        mkt_avg = _safe_float(mrow[6]) if len(mrow) > 6 else 0.0
        summary.append([brand, cat_id, len(prices), min_p, max_p, round(avg_p, 2),
                         mkt_rev, mkt_ord, mkt_avg])

    log(f"Сводных строк: {len(summary)}")
    return summary


# ---------------------------------------------------------------------------
# Основной поток
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Генерация ZIM-отчёта Primanova из данных парсинга."
    )
    parser.add_argument(
        "--data",
        required=True,
        metavar="FILE",
        help="Путь к файлу с данными парсинга (.csv или .xlsx)",
    )
    parser.add_argument(
        "--date-from",
        default=(datetime.date.today() - datetime.timedelta(days=30)).isoformat(),
        metavar="YYYY-MM-DD",
        help="Дата начала периода для рыночных данных (по умолчанию: 30 дней назад)",
    )
    parser.add_argument(
        "--date-to",
        default=datetime.date.today().isoformat(),
        metavar="YYYY-MM-DD",
        help="Дата конца периода для рыночных данных (по умолчанию: сегодня)",
    )
    parser.add_argument(
        "--no-market",
        action="store_true",
        help="Пропустить получение рыночных данных из Ozon API",
    )
    args = parser.parse_args()

    log(f"=== ZIM-отчёт Primanova | период: {args.date_from} — {args.date_to} ===")

    # 1. Загрузка данных парсинга
    raw_rows, header = load_parse_data(args.data)

    # 2. Нормализация
    catalog_rows = normalize_rows(raw_rows, header)
    if not catalog_rows:
        log("ОШИБКА: после нормализации нет данных. Проверьте формат входного файла.")
        sys.exit(1)

    # 3. Рыночные данные (если не отключены)
    market_rows = []
    if not args.no_market:
        category_ids = list({row[5] for row in catalog_rows if row[5]})
        market_rows = fetch_market_data_by_category(category_ids, args.date_from, args.date_to)

    # 4. Сводка
    summary_rows = build_summary(catalog_rows, market_rows)

    # 5. Отправка в GAS
    clear_gas_buffers()
    time.sleep(1)

    send_to_gas("ZIM_CATALOG", catalog_rows)
    if market_rows:
        send_to_gas("ZIM_MARKET", market_rows)
    send_to_gas("ZIM_SUMMARY", summary_rows)

    log("=== Готово ===")


if __name__ == "__main__":
    main()
