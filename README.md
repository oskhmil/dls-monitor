# DLS Monitor for Railway

Автономний сервіс під Railway.

Що робить:
- моніторить сайт ДЛС кожні 4 години;
- нові записи надсилає в Telegram;
- нові записи зберігає в SQLite;
- журнал доступний у браузері в будь-який час;
- журнал можна скачати як Excel.

## Ендпоїнти
- `/health`
- `/journal`
- `/journal/download`

## Railway
1. Завантаж цей код у GitHub
2. Створи Railway Project
3. Deploy from GitHub Repo
4. Додай Volume і змонтуй його в `/data`
5. У Variables задай:
   - `TELEGRAM_TOKEN`
   - `TELEGRAM_CHAT_ID`
   - `POLL_INTERVAL_SECONDS=14400`
   - `INITIAL_BOOTSTRAP_SILENT=true`
   - `DATA_DIR=/data`
   - `DB_PATH=/data/dls_monitor.db`
   - `XLSX_PATH=/data/Журнал_ДЛС.xlsx`
6. У Networking згенеруй домен
7. У Healthcheck path задай `/health`

## Перший запуск
Якщо `INITIAL_BOOTSTRAP_SILENT=true`, сервіс:
- імпортує поточні записи в журнал;
- не розсилає весь поточний місяць у Telegram;
- далі шле тільки справді нові записи.

## Локально
```bat
py -m pip install -r requirements.txt
py app.py
```
