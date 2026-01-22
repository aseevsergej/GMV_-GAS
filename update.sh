#!/bin/bash
echo "=== ОСТАНОВКА СТАРОГО СКРИПТА ==="
pkill -f main.py

echo "=== СКАЧИВАНИЕ ОБНОВЛЕНИЙ ==="
# Убедитесь, что папка правильная
cd ~/GMV_-GAS
git pull

echo "=== ОБНОВЛЕНИЕ БИБЛИОТЕК ==="
pip3 install -r requirements.txt --break-system-packages

echo "=== ЗАПУСК НОВОЙ ВЕРСИИ ==="
nohup python3 -u main.py > ../ozon.log 2>&1 &
echo "=== ГОТОВО! ==="