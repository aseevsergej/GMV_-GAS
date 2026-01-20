import os
import json
import time
import requests
import gspread
from flask import Flask, jsonify
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# --- КОНФИГУРАЦИЯ ---
# Эти переменные мы зададим в настройках Render, чтобы не светить их в коде
OZON_CLIENT_ID = os.environ.get("OZON_CLIENT_ID")
OZON_API_KEY = os.environ.get("OZON_API_KEY")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
# Google Credentials JSON передадим как строку целиком
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDS_JSON")

def get_ozon_headers():
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }

def fetch_ozon_data():
    """Скачивает товары пачками и собирает единый список"""
    all_items = []
    last_id = ""
    
    print("🚀 Start fetching Ozon data...")
    
    while True:
        payload = {
            "filter": {
                "visibility": "ALL"
            },
            "limit": 100  # Берем по 100 шт
        }
        if last_id:
            payload["last_id"] = last_id
            
        try:
            # 1. Получаем список ID
            resp = requests.post(
                "https://api-seller.ozon.ru/v2/product/list",
                headers=get_ozon_headers(),
                json=payload,
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json().get("result", {}).get("items", [])
            
            if not data:
                break
                
            # 2. Получаем детали (Info)
            product_ids = [item["product_id"] for item in data]
            
            info_resp = requests.post(
                "https://api-seller.ozon.ru/v2/product/info/list",
                headers=get_ozon_headers(),
                json={"product_id": product_ids},
                timeout=30
            )
            info_data = info_resp.json().get("result", {}).get("items", [])
            
            # Собираем данные в список
            for item in info_data:
                price = float(item.get("price", {}).get("price", 0))
                marketing_price = float(item.get("price", {}).get("marketing_price", 0))
                if marketing_price == 0: marketing_price = price
                
                stocks = item.get("stocks", {}).get("present", 0)
                
                row = [
                    item.get("primary_image", ""),
                    str(item.get("id", "")),
                    str(item.get("id", "")),
                    str(item.get("offer_id", "")),
                    "OZON", # Brand placeholder
                    str(item.get("category_id", "")),
                    item.get("name", ""),
                    price,
                    marketing_price,
                    marketing_price,
                    stocks
                ]
                all_items.append(row)
            
            print(f"✅ Fetched batch: {len(data)} items. Total: {len(all_items)}")
            
            last_id = data[-1]["product_id"]
            if len(data) < 100:
                break
                
            # Пауза, чтобы не злить Ozon
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Error Ozon API: {e}")
            break
            
    return all_items

@app.route("/")
def health_check():
    return "Ozon Worker is Alive!", 200

@app.route("/sync")
def sync_ozon():
    """Точка входа для Google Apps Script"""
    if not OZON_API_KEY or not GOOGLE_CREDS_JSON:
        return jsonify({"error": "Env vars not set"}), 500
        
    try:
        # 1. Получаем данные
        rows = fetch_ozon_data()
        
        if not rows:
             return jsonify({"status": "No data found"}), 200

        # 2. Авторизация в Google
        creds_dict = json.loads(GOOGLE_CREDS_JSON)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        
        # 3. Открываем таблицу
        sh = gc.open_by_key(SPREADSHEET_ID)
        # Ищем лист или создаем
        try:
            worksheet = sh.worksheet("OZ_CARDS_PY")
        except:
            worksheet = sh.add_worksheet(title="OZ_CARDS_PY", rows=1000, cols=20)
            
        # 4. Очищаем и пишем
        worksheet.clear()
        
        headers = ['Фото', 'nmID', 'Артикул WB', 'Артикул Прод', 'Бренд', 'Категория', 'Название', 'Цена (База)', 'Цена (Прод)', 'Цена (СПП)', 'Остаток']
        
        # Пишем все одним большим запросом
        worksheet.update('O1', [headers] + rows, value_input_option='USER_ENTERED')
        
        return jsonify({
            "status": "success", 
            "count": len(rows),
            "sheet": "OZ_CARDS_PY"
        }), 200
        
    except Exception as e:
        print(e)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Для локального запуска
    app.run(host="0.0.0.0", port=10000)