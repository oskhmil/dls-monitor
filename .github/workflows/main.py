from dotenv import load_dotenv
load_dotenv()

import html
import logging
import os
import sqlite3
import threading
import time
from contextlib import closing
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook

URL = "https://pub-mex.dls.gov.ua/QLA/DocList.aspx"

PORT = int(os.environ.get("PORT", "5000"))
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "14400"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "20"))

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

DATA_DIR = Path(
    os.environ.get("DATA_DIR")
    or os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
    or "./data"
)
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = Path(os.environ.get("DB_PATH", str(DATA_DIR / "dls_monitor.db")))
XLSX_PATH = Path(os.environ.get("XLSX_PATH", str(DATA_DIR / "Журнал_ДЛС.xlsx")))

INITIAL_BOOTSTRAP_SILENT = os.environ.get("INITIAL_BOOTSTRAP_SILENT", "true").strip().lower() == "true"

VALID_TYPES = [
    "тимч. заборона",
    "пост. заборона",
    "скасув. тимч. заборони",
    "скасув. пост. заборони",
    "вилучення",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

JOURNAL_HEADERS = [
    "№ з/п",
    "Дата і номер розпорядження/ рішення/припису",
    "Дата одержання розпорядження/ рішення/припису",
    "Назва лікарських засобів та перелік серій лікарських засобів, зазначених у розпорядженні/ рішенні/приписі",
    "Результати перевірки щодо наявності зазначених лікарських засобів",
    "Вжиті заходи у разі виявлення зазначених лікарських засобів",
    "Дата і номер листа-повідомлення територіальному органу",
    "Підпис уповноваженої особи",
]
JOURNAL_NUMBERS = ["1", "2", "3", "4", "5", "6", "7", "8"]

db_lock = threading.Lock()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_lock, closing(get_conn()) as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS documents (
                uid TEXT PRIMARY KEY,
                doc_num TEXT NOT NULL,
                doc_date TEXT NOT NULL,
                doc_type TEXT NOT NULL,
                drug_name TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                telegram_sent INTEGER NOT NULL DEFAULT 0,
                telegram_sent_at TEXT,
                source_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS app_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS run_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                documents_found INTEGER NOT NULL DEFAULT 0,
                new_documents INTEGER NOT NULL DEFAULT 0,
                telegram_sent INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'running',
                message TEXT
            );
            '''
        )
        conn.commit()


def get_meta(key, default=""):
    with db_lock, closing(get_conn()) as conn:
        row = conn.execute("SELECT value FROM app_meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default


def set_meta(key, value):
    with db_lock, closing(get_conn()) as conn:
        conn.execute(
            '''
            INSERT INTO app_meta(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            ''',
            (key, value),
        )
        conn.commit()


def telegram_enabled():
    return bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)


def build_uid(doc_num, doc_date, drug_name):
    return f"{doc_num.strip()}|{doc_date.strip()}|{drug_name.strip()}".lower()


def get_hidden_fields(soup):
    def val(field_id):
        el = soup.find("input", {"id": field_id})
        return el.get("value", "") if el else ""

    return (
        val("__VIEWSTATE"),
        val("__VIEWSTATEGENERATOR"),
        val("__EVENTVALIDATION"),
    )


def request_with_retry(session, method, url, **kwargs):
    last_error = None
    for attempt in range(1, 4):
        try:
            response = session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            response.raise_for_status()
            return response
        except Exception as exc:
            last_error = exc
            logging.warning("HTTP %s %s failed (%s/3): %s", method, url, attempt, exc)
            time.sleep(attempt * 2)
    raise last_error


def parse_row(cols):
    type_idx = None
    for idx, col in enumerate(cols):
        text = " ".join(col.stripped_strings).strip().lower()
        if any(v in text for v in VALID_TYPES):
            type_idx = idx
            break

    if type_idx is None:
        return None

    def get(i):
        return " ".join(cols[i].stripped_strings).strip() if i < len(cols) else ""

    return {
        "doc_type": get(type_idx),
        "reg_num": get(type_idx + 1),
        "drug_name": get(type_idx + 2),
        "series": get(type_idx + 3),
        "manufacturer": get(type_idx + 4),
    }


def parse_grid_rows(grid, current_month, current_year):
    records = []
    hit_previous_month = False

    for row in grid.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        doc_date = " ".join(cols[0].stripped_strings).strip()
        doc_num = " ".join(cols[1].stripped_strings).strip()
        if not doc_date or not doc_num:
            continue

        try:
            d = datetime.strptime(doc_date, "%d.%m.%Y")
        except ValueError:
            continue

        if d.month != current_month or d.year != current_year:
            hit_previous_month = True
            continue

        parsed = parse_row(cols)
        if not parsed or not parsed["drug_name"]:
            continue

        parts = [parsed["drug_name"]]
        series = parsed["series"]
        if series:
            if "додатку" in series.lower():
                parts.append("(серії зазначені у додатку)")
            else:
                parts.append(f"Серія № {series}")
        if parsed["manufacturer"]:
            parts.append(parsed["manufacturer"])

        drug_full = ", ".join(parts)
        records.append({
            "uid": build_uid(doc_num, doc_date, drug_full),
            "doc_num": doc_num,
            "doc_date": doc_date,
            "doc_type": parsed["doc_type"],
            "drug_name": drug_full,
        })

    return records, hit_previous_month


def get_all_documents():
    session = requests.Session()
    now = datetime.now()
    current_month = now.month
    current_year = now.year

    try:
        response = request_with_retry(session, "GET", URL, headers=HEADERS)
        soup = BeautifulSoup(response.text, "html.parser")
        vs, vsg, ev = get_hidden_fields(soup)

        payload = {
            "__EVENTTARGET": "ctl00$Content$fvParams$UpdateButton",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": vs,
            "__VIEWSTATEGENERATOR": vsg,
            "__EVENTVALIDATION": ev,
        }
        response = request_with_retry(session, "POST", URL, headers=HEADERS, data=payload)
        soup = BeautifulSoup(response.text, "html.parser")
        vs, vsg, ev = get_hidden_fields(soup)

        grid = soup.find("table", {"id": "ctl00_Content_gridList"})
        if not grid:
            logging.error("Таблицю документів не знайдено")
            return []

        all_records = []
        page_num = 1

        while True:
            page_records, hit_previous_month = parse_grid_rows(grid, current_month, current_year)
            all_records.extend(page_records)
            logging.info("Сторінка %s: %s записів", page_num, len(page_records))

            if hit_previous_month:
                break

            next_token = f"Page${page_num + 1}"
            has_next = any(next_token in (a.get("href") or "") for a in grid.find_all("a"))
            if not has_next:
                break

            page_num += 1
            payload = {
                "__EVENTTARGET": "ctl00$Content$gridList",
                "__EVENTARGUMENT": f"Page${page_num}",
                "__VIEWSTATE": vs,
                "__VIEWSTATEGENERATOR": vsg,
                "__EVENTVALIDATION": ev,
            }
            response = request_with_retry(session, "POST", URL, headers=HEADERS, data=payload)
            soup = BeautifulSoup(response.text, "html.parser")
            vs, vsg, ev = get_hidden_fields(soup)
            grid = soup.find("table", {"id": "ctl00_Content_gridList"})
            if not grid:
                break

            time.sleep(1)

        unique = {}
        for item in all_records:
            unique[item["uid"]] = item
        return list(unique.values())

    except Exception as exc:
        logging.exception("Помилка зчитування сайту: %s", exc)
        return []


def send_telegram(doc):
    if not telegram_enabled():
        logging.warning("Telegram вимкнений: не заповнені TELEGRAM_TOKEN / TELEGRAM_CHAT_ID")
        return False

    message = (
        "🚨 <b>Новий припис ДЛС!</b>\n\n"
        f"<b>№</b> {html.escape(doc['doc_num'])}\n"
        f"<b>Дата:</b> {html.escape(doc['doc_date'])}\n"
        f"<b>Захід:</b> {html.escape(doc['doc_type'])}\n"
        f"<b>Ліки:</b> {html.escape(doc['drug_name'])}"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    for attempt in range(1, 4):
        try:
            response = requests.post(url, data=payload, timeout=20)
            if response.status_code == 200:
                logging.info("Telegram надіслано: %s", doc["doc_num"])
                return True
            logging.warning("Telegram error %s: %s", response.status_code, response.text[:300])
        except Exception as exc:
            logging.warning("Telegram failed (%s/3): %s", attempt, exc)
        time.sleep(attempt * 2)

    return False


def insert_or_update_documents(documents, bootstrap_silent):
    new_count = 0
    telegram_sent_count = 0

    with db_lock, closing(get_conn()) as conn:
        for doc in documents:
            existing = conn.execute(
                "SELECT uid FROM documents WHERE uid = ?",
                (doc["uid"],)
            ).fetchone()

            if existing:
                conn.execute(
                    "UPDATE documents SET source_seen_at = CURRENT_TIMESTAMP WHERE uid = ?",
                    (doc["uid"],),
                )
                continue

            telegram_sent = 1 if bootstrap_silent else 0
            telegram_sent_at = datetime.utcnow().isoformat(timespec="seconds") if bootstrap_silent else None

            conn.execute(
                '''
                INSERT INTO documents(
                    uid, doc_num, doc_date, doc_type, drug_name,
                    telegram_sent, telegram_sent_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    doc["uid"],
                    doc["doc_num"],
                    doc["doc_date"],
                    doc["doc_type"],
                    doc["drug_name"],
                    telegram_sent,
                    telegram_sent_at,
                ),
            )
            new_count += 1

        conn.commit()

    if not bootstrap_silent:
        unsent = get_unsent_documents()
        for doc in unsent:
            if send_telegram(dict(doc)):
                mark_telegram_sent(doc["uid"])
                telegram_sent_count += 1

    return new_count, telegram_sent_count


def get_unsent_documents():
    with db_lock, closing(get_conn()) as conn:
        return conn.execute(
            '''
            SELECT uid, doc_num, doc_date, doc_type, drug_name
            FROM documents
            WHERE telegram_sent = 0
            ORDER BY
                substr(doc_date, 7, 4) || '-' || substr(doc_date, 4, 2) || '-' || substr(doc_date, 1, 2),
                doc_num,
                drug_name
            '''
        ).fetchall()


def mark_telegram_sent(uid):
    with db_lock, closing(get_conn()) as conn:
        conn.execute(
            '''
            UPDATE documents
            SET telegram_sent = 1,
                telegram_sent_at = ?
            WHERE uid = ?
            ''',
            (datetime.utcnow().isoformat(timespec="seconds"), uid),
        )
        conn.commit()


def export_excel():
    wb = Workbook()
    ws = wb.active
    ws.title = "Журнал ДЛС"
    ws.append(JOURNAL_HEADERS)
    ws.append(JOURNAL_NUMBERS)

    for col, width in {"A": 8, "B": 36, "C": 22, "D": 70, "E": 38, "F": 32, "G": 30, "H": 22}.items():
        ws.column_dimensions[col].width = width

    with db_lock, closing(get_conn()) as conn:
        rows = conn.execute(
            '''
            SELECT doc_num, doc_date, drug_name
            FROM documents
            ORDER BY
                substr(doc_date, 7, 4) || '-' || substr(doc_date, 4, 2) || '-' || substr(doc_date, 1, 2),
                doc_num,
                drug_name
            '''
        ).fetchall()

    for index, row in enumerate(rows, start=1):
        ws.append([
            index,
            f"№ {row['doc_num']} від {row['doc_date']}",
            row["doc_date"],
            row["drug_name"],
            "відсутній",
            "",
            "",
            "",
        ])

    wb.save(XLSX_PATH)


def get_journal_rows():
    with db_lock, closing(get_conn()) as conn:
        return conn.execute(
            '''
            SELECT doc_num, doc_date, drug_name, telegram_sent
            FROM documents
            ORDER BY
                substr(doc_date, 7, 4) || '-' || substr(doc_date, 4, 2) || '-' || substr(doc_date, 1, 2) DESC,
                doc_num DESC
            '''
        ).fetchall()


def create_run_log():
    with db_lock, closing(get_conn()) as conn:
        cur = conn.execute(
            "INSERT INTO run_log(started_at, status) VALUES(?, 'running')",
            (datetime.utcnow().isoformat(timespec="seconds"),),
        )
        conn.commit()
        return cur.lastrowid


def finish_run_log(run_id, documents_found, new_documents, telegram_sent_count, status, message=""):
    with db_lock, closing(get_conn()) as conn:
        conn.execute(
            '''
            UPDATE run_log
            SET finished_at = ?,
                documents_found = ?,
                new_documents = ?,
                telegram_sent = ?,
                status = ?,
                message = ?
            WHERE id = ?
            ''',
            (
                datetime.utcnow().isoformat(timespec="seconds"),
                documents_found,
                new_documents,
                telegram_sent_count,
                status,
                message,
                run_id,
            ),
        )
        conn.commit()


def run_cycle():
    run_id = create_run_log()
    try:
        documents = get_all_documents()
        if not documents:
            finish_run_log(run_id, 0, 0, 0, "warning", "Не отримано жодного документа")
            logging.warning("Не отримано жодного документа")
            return

        bootstrap_done = get_meta("bootstrap_done", "false").lower() == "true"
        bootstrap_silent = (not bootstrap_done) and INITIAL_BOOTSTRAP_SILENT

        new_documents, telegram_sent_count = insert_or_update_documents(documents, bootstrap_silent=bootstrap_silent)
        export_excel()

        if not bootstrap_done:
            set_meta("bootstrap_done", "true")
            set_meta("bootstrap_mode", "silent" if bootstrap_silent else "notify")

        set_meta("last_success_at", datetime.utcnow().isoformat(timespec="seconds"))
        set_meta("last_documents_found", str(len(documents)))
        set_meta("last_new_documents", str(new_documents))
        set_meta("last_telegram_sent", str(telegram_sent_count))

        msg = "Bootstrap silent import completed" if bootstrap_silent else "Cycle completed"
        finish_run_log(run_id, len(documents), new_documents, telegram_sent_count, "success", msg)
        logging.info("Цикл завершено. Документів: %s | Нових: %s | Telegram: %s", len(documents), new_documents, telegram_sent_count)

    except Exception as exc:
        finish_run_log(run_id, 0, 0, 0, "error", str(exc))
        logging.exception("Помилка циклу: %s", exc)


class AppHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health"):
            return self._send_health()
        if self.path in ("/journal", "/journal/"):
            return self._send_journal()
        if self.path == "/journal/download":
            return self._send_journal_download()
        self.send_response(404)
        self.end_headers()

    def _send_health(self):
        payload = {
            "status": "ok",
            "last_success_at": get_meta("last_success_at", ""),
            "last_documents_found": get_meta("last_documents_found", "0"),
            "last_new_documents": get_meta("last_new_documents", "0"),
            "last_telegram_sent": get_meta("last_telegram_sent", "0"),
            "data_dir": str(DATA_DIR),
        }
        body = ("{\n" + ",\n".join(f'  "{k}": "{str(v).replace(chr(34), "")}"' for k, v in payload.items()) + "\n}").encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_journal_download(self):
        try:
            export_excel()
            data = XLSX_PATH.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            self.send_header("Content-Disposition", 'attachment; filename="Журнал_ДЛС.xlsx"')
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as exc:
            body = f"Помилка: {exc}".encode("utf-8", "replace")
            self.send_response(500)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def _send_journal(self):
        rows = get_journal_rows()
        th = "".join(f"<th>{html.escape(h)}</th>" for h in ["№", "Розпорядження", "Дата", "Ліки", "Telegram"])
        body_rows = []
        for index, row in enumerate(rows, start=1):
            tg = "Надіслано" if row["telegram_sent"] else "Очікує"
            body_rows.append(
                "<tr>"
                f"<td>{index}</td>"
                f"<td>{html.escape('№ ' + row['doc_num'])}</td>"
                f"<td>{html.escape(row['doc_date'])}</td>"
                f"<td>{html.escape(row['drug_name'])}</td>"
                f"<td>{html.escape(tg)}</td>"
                "</tr>"
            )

        page = f'''<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Журнал ДЛС</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 20px; background: #f7f7f7; }}
h2 {{ margin-bottom: 6px; }}
.meta {{ color: #666; margin-bottom: 12px; }}
a.button {{ display:inline-block; padding:10px 14px; background:#1565c0; color:#fff; text-decoration:none; border-radius:6px; margin-bottom:12px; }}
table {{ border-collapse: collapse; width: 100%; background: white; }}
th, td {{ border:1px solid #ddd; padding:8px; text-align:left; vertical-align: top; }}
th {{ background:#1a237e; color:white; position: sticky; top:0; }}
tr:nth-child(even) {{ background:#f4f7ff; }}
.wrap {{ overflow-x:auto; }}
.card {{ background:white; padding:12px 14px; border-radius:8px; margin-bottom:12px; border:1px solid #ddd; }}
</style>
</head>
<body>
<h2>Журнал обліку розпоряджень ДЛС</h2>
<div class="card">
<div><b>Останній успішний цикл:</b> {html.escape(get_meta("last_success_at", "—"))}</div>
<div><b>Документів знайдено в останньому циклі:</b> {html.escape(get_meta("last_documents_found", "0"))}</div>
<div><b>Нових документів:</b> {html.escape(get_meta("last_new_documents", "0"))}</div>
<div><b>Telegram надіслано:</b> {html.escape(get_meta("last_telegram_sent", "0"))}</div>
</div>
<a class="button" href="/journal/download">Завантажити Excel</a>
<div class="wrap">
<table>
<thead><tr>{th}</tr></thead>
<tbody>{''.join(body_rows)}</tbody>
</table>
</div>
</body>
</html>'''
        body = page.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


def scheduler_loop():
    run_cycle()
    while True:
        time.sleep(POLL_INTERVAL_SECONDS)
        run_cycle()


def main():
    init_db()
    export_excel()

    if not telegram_enabled():
        logging.warning("TELEGRAM_TOKEN / TELEGRAM_CHAT_ID не задані. Telegram вимкнений.")

    worker = threading.Thread(target=scheduler_loop, daemon=True)
    worker.start()

    server = HTTPServer(("0.0.0.0", PORT), AppHandler)
    logging.info("Веб-сервер працює на порту %s", PORT)
    logging.info("Дані зберігаються в %s", DATA_DIR)
    logging.info("Інтервал перевірки: %s сек", POLL_INTERVAL_SECONDS)
    server.serve_forever()


if __name__ == "__main__":
    main()
