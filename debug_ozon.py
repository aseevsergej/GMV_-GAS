import requests
import json
import os
from dotenv import load_dotenv

# --- НАСТРОЙКИ (МОЖНО ВПИСАТЬ РУКАМИ ДЛЯ НАДЕЖНОСТИ) ---
CLIENT_ID = "21745"           # ID кабинета OZ_INHOUSE
API_KEY = "ВАШ_НОВЫЙ_КЛЮЧ"    # Вставьте сюда ключ (роль Товары)

# Прокси (если есть в .env, возьмет оттуда, иначе впишите руками)
# Format: "http://login:pass@ip:port"
load_dotenv()
PROXY = os.getenv("PROXY_URL") 
# PROXY = "http://user:pass@ip:port" # Раскомментируйте и впишите, если .env не сработает

# -------------------------------------------------------

print(f"--- ЗАПУСК СЫРОГО ТЕСТА ---")
print(f"ID: {CLIENT_ID}")
print(f"Key: {API_KEY[:5]}...")
print(f"Proxy: {PROXY}")

session = requests.Session()
if PROXY:
    session.proxies = {"http": PROXY, "https": PROXY}

headers = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

def test_endpoint(name, url, payload=None):
    print(f"\n[TEST] {name} -> {url}")
    try:
        if payload:
            r = session.post(url, json=payload, headers=headers, timeout=15)
        else:
            r = session.get(url, headers=headers, timeout=15)
        
        print(f"STATUS: {r.status_code}")
        print(f"HEADERS: {r.headers}")
        print(f"BODY (First 300 chars): {r.text[:300]}")
        
        if r.status_code == 200:
            print(">>> УСПЕХ! Данные получены.")
        elif r.status_code == 404:
            print(">>> ОШИБКА 404: Метод не найден или нет доступа.")
        elif r.status_code == 403:
            print(">>> ОШИБКА 403: Доступ запрещен (проверьте роль ключа).")
            
    except Exception as e:
        print(f"CRASH: {e}")

# 1. ТЕСТ СВЯЗИ (Категории) - Это самый простой метод, он обычно открыт всем
# Если здесь 404/403 - значит проблема глобальная (IP, Ключ, Хедеры)
test_endpoint("1. Категории (v2/category/tree)", "https://api-seller.ozon.ru/v2/category/tree", {
    "category_id": 17027949, # Просто любой ID или 0
    "language": "DEFAULT"
})

# 2. ТЕСТ ТОВАРОВ v2 (Ваш проблемный метод)
test_endpoint("2. Товары v2 (Filter: Empty)", "https://api-seller.ozon.ru/v2/product/list", {
    "filter": {},
    "limit": 10
})

# 3. ТЕСТ ТОВАРОВ v1 (Старый метод)
test_endpoint("3. Товары v1", "https://api-seller.ozon.ru/v1/product/list", {
    "filter": {},
    "limit": 10
})

# 4. ТЕСТ ПРОДАЖ (Который работает) - Для сравнения
test_endpoint("4. Продажи (FBO)", "https://api-seller.ozon.ru/v2/posting/fbo/list", {
    "filter": {"since": "2026-01-20T00:00:00Z", "to": "2026-01-22T23:59:59Z"},
    "limit": 5
})