import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()
CID = os.getenv("CLIENT_ID") # Если их нет в env, скрипт попробует найти их в конфиге GAS, но лучше возьмем из .env если вы их туда писали.
# Но так как мы не писали их в .env, мы возьмем их "Жестко" для теста.

# !!! ВНИМАНИЕ: ВСТАВЬТЕ СЮДА ВАШИ КЛЮЧИ OZON ДЛЯ ТЕСТА !!!
CLIENT_ID = "21745" # Вставьте ваш ID (из лога я вижу 21745 или 1701257)
API_KEY = "3e5357da-f901-48b0-9eaa-8b07b3637ad3" # Вставьте ваш ключ

print(f"--- ТЕСТ OZON API ДЛЯ {CLIENT_ID} ---")

headers = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; TestScript/1.0)"
}

def try_url(name, url, payload):
    print(f"\nTesting {name}...")
    print(f"URL: {url}")
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            cnt = len(data.get("result", {}).get("items", []))
            print(f"УСПЕХ! Найдено элементов: {cnt}")
        else:
            print(f"ОШИБКА: {r.text[:300]}")
            print(f"Headers ответа: {r.headers}")
    except Exception as e:
        print(f"CRASH: {e}")

# 1. Тест Товаров (v2) - Стандарт
try_url("Товары v2 (Standart)", "https://api-seller.ozon.ru/v2/product/list", 
        {"filter": {"visibility": "ALL"}, "limit": 10})

# 2. Тест Товаров (v2) - Пустой фильтр
try_url("Товары v2 (Empty Filter)", "https://api-seller.ozon.ru/v2/product/list", 
        {"filter": {}, "limit": 10})

# 3. Тест Товаров (v1) - Старый метод
try_url("Товары v1 (Old)", "https://api-seller.ozon.ru/v1/product/list", 
        {"filter": {}, "limit": 10})

# 4. Тест Продаж (Контрольный)
try_url("Продажи (Check)", "https://api-seller.ozon.ru/v2/posting/fbo/list", 
        {"filter": {"since": "2026-01-20T00:00:00Z", "to": "2026-01-22T23:59:59Z"}, "limit": 10})