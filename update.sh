#!/bin/bash
echo "=== ОСТАНОВКА ==="
pkill -f main.py

echo "=== ОБНОВЛЕНИЕ КОДА ==="
cd ~/GMV_-GAS
git pull

echo "=== УСТАНОВКА БИБЛИОТЕК (Force) ==="
pip3 install -r requirements.txt --break-system-packages

echo "=== ЗАПУСК v124 ==="
nohup python3 -u main.py > ../ozon.log 2>&1 &
echo "=== ГОТОВО ==="