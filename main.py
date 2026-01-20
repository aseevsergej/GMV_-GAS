import os
import time
import requests
from flask import Flask, jsonify, Response

app = Flask(__name__)

# --- КОНФИГУРАЦИЯ ---
OZON_CLIENT_ID = os.environ.get("OZON_CLIENT_ID")
OZON_API_KEY = os.environ.get("OZON_API_KEY")

def get_ozon_headers():
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }

@app.route("/")
def health_check():
    return "Ozon Data Provider is Ready!", 200

@app.route("/get-ozon-cards")
def get_cards():
    """Скачивает товары и отдает их как JSON для Google Apps Script"""
    
    # Проверка ключей
    if not OZON_CLIENT_ID or not OZON_API_KEY:
        return jsonify({"error": "Ozon keys are missing on server"}), 500

    all_items = []
    last_id = ""
    
    # --- СБОР ДАННЫХ С OZON ---
    try:
        while True:
            payload = { "filter": { "visibility": "ALL" }, "limit": 100 }
            if last_id: payload["last_id"] = last_id
            
            # 1. Список
            resp = requests.post(
                "https://api-seller.ozon.ru/v2/product/list",
                headers=get_ozon_headers(),
                json=payload,
                timeout=20
            )
            if resp.status_code != 200:
                print(f"Ozon List Error: {resp.text}")
                break
                
            data = resp.json().get("result", {}).get("items", [])
            if not data: break
                
            # 2. Детали (Info)
            product_ids = [item["product_id"] for item in data]
            info_resp = requests.post(
                "https://api-seller.ozon.ru/v2/product/info/list",
                headers=get_ozon_headers(),
                json={"product_id": product_ids},
                timeout=20
            )
            
            info_items = []
            if info_resp.status_code == 200:
                info_items = info_resp.json().get("result", {}).get("items", [])
            else:
                # Если Info не сработал, берем хотя бы ID из списка
                info_items = data 

            # Обработка
            for item in info_items:
                # Безопасное получение цены (если Info не сработал, там может не быть price)
                price_obj = item.get("price", {})
                price = float(price_obj.get("price", 0) if price_obj else 0)
                m_price = float(price_obj.get("marketing_price", 0) if price_obj else 0)
                if m_price == 0: m_price = price
                
                stocks = item.get("stocks", {}).get("present", 0)
                
                # Формируем строку для таблицы
                row = [
                    item.get("primary_image", ""),      # Фото
                    str(item.get("id", item.get("product_id", ""))), # nmID (Ozon ID)
                    str(item.get("id", item.get("product_id", ""))), # Артикул WB (дубль)
                    str(item.get("offer_id", "")),      # Артикул Прод
                    "OZON",                             # Бренд
                    str(item.get("category_id", "")),   # Категория
                    item.get("name", "Товар Ozon"),     # Название
                    price,                              # Цена
                    m_price,                            # Цена Прод
                    m_price,                            # Цена СПП
                    stocks                              # Остаток
                ]
                all_items.append(row)
            
            last_id = data[-1]["product_id"]
            if len(data) < 100: break
            time.sleep(0.3)
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Отдаем чистый JSON с массивом данных
    return jsonify({"data": all_items})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)