import os
import time
import requests
from flask import Flask, jsonify

app = Flask(__name__)

# --- КОНФИГУРАЦИЯ ---
OZON_CLIENT_ID = os.environ.get("OZON_CLIENT_ID")
OZON_API_KEY = os.environ.get("OZON_API_KEY")
# Сюда вставим URL, который получили в Google Apps Script
GAS_WEBAPP_URL = os.environ.get("GAS_WEBAPP_URL") 
SECRET_KEY = "MY_SUPER_SECRET_PASSWORD_123" # Тот же пароль, что в GAS

def get_ozon_headers():
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json"
    }

def fetch_ozon_data():
    all_items = []
    last_id = ""
    print("🚀 Fetching Ozon...")
    
    while True:
        payload = { "filter": { "visibility": "ALL" }, "limit": 100 }
        if last_id: payload["last_id"] = last_id
            
        try:
            resp = requests.post("https://api-seller.ozon.ru/v2/product/list", headers=get_ozon_headers(), json=payload, timeout=30)
            data = resp.json().get("result", {}).get("items", [])
            if not data: break
                
            p_ids = [i["product_id"] for i in data]
            info_resp = requests.post("https://api-seller.ozon.ru/v2/product/info/list", headers=get_ozon_headers(), json={"product_id": p_ids}, timeout=30)
            info_data = info_resp.json().get("result", {}).get("items", [])
            
            for item in info_data:
                price = float(item.get("price", {}).get("price", 0))
                m_price = float(item.get("price", {}).get("marketing_price", 0) or price)
                stocks = item.get("stocks", {}).get("present", 0)
                
                # Формируем строку таблицы. 
                # Порядок должен совпадать с тем, что вы ожидаете в Google Sheets
                row = [
                    item.get("primary_image", ""),
                    str(item.get("id", "")),
                    str(item.get("id", "")),
                    str(item.get("offer_id", "")),
                    "OZON", 
                    str(item.get("category_id", "")),
                    item.get("name", ""),
                    price,
                    m_price,
                    m_price,
                    stocks
                ]
                all_items.append(row)
            
            last_id = data[-1]["product_id"]
            if len(data) < 100: break
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            break
            
    return all_items

@app.route("/sync")
def sync_ozon():
    if not OZON_API_KEY or not GAS_WEBAPP_URL:
        return jsonify({"error": "Env vars missing"}), 500
        
    rows = fetch_ozon_data()
    if not rows: return jsonify({"status": "No data"}), 200
    
    # Отправляем в Google Script
    payload = {
        "sheetName": "OZ_CARDS_PY", # Имя листа, куда вставлять
        "rows": rows,
        "secret": SECRET_KEY
    }
    
    try:
        r = requests.post(GAS_WEBAPP_URL, json=payload, allow_redirects=True)
        return jsonify({"ozon_count": len(rows), "google_response": r.text}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)