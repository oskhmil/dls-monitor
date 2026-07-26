"""
Ядро розбору та нормалізації даних ДЛС.

Виділено окремим модулем, щоб цю ж логіку можна було прогнати тестами
на реальній базі без запуску скрапера.

ВАЖЛИВО: функція fold() має один-в-один збігатися з _dlsFold() у Liki.on!
Будь-яка зміна тут вимагає дзеркальної зміни в index.html.
"""

import json
import re

# ── Гомогліфи ────────────────────────────────────────────────────────────────
# У приписах ДЛС кирилиця регулярно домішується в латинські серії:
# FD255985С (кир. С), ТR056 (кир. Т), А010624 (кир. А), 24Е001 (кир. Е), Н/105.
# Без згортання ~1.6% серій мовчки не зматчаться із залишками 1С.
CYR2LAT = {
    "А": "A", "В": "B", "Е": "E", "З": "3", "І": "I", "К": "K", "М": "M",
    "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T", "У": "Y", "Х": "X",
    "Ѕ": "S", "Ј": "J", "Ї": "I", "Ё": "E",
    "а": "A", "в": "B", "е": "E", "і": "I", "к": "K", "м": "M",
    "н": "H", "о": "O", "р": "P", "с": "C", "т": "T", "у": "Y", "х": "X",
}


def fold(s):
    """Агресивна нормалізація серії: гомогліфи → латиниця, лишаємо [A-Z0-9]."""
    if not s:
        return ""
    out = []
    for ch in str(s).upper():
        ch = CYR2LAT.get(ch, ch)
        if ("A" <= ch <= "Z") or ("0" <= ch <= "9"):
            out.append(ch)
    return "".join(out)


def fold_name(s):
    """Нормалізація назви: гомогліфи, лишаємо літери/цифри/пробіл, схлопуємо пробіли."""
    if not s:
        return ""
    out = []
    for ch in str(s).upper():
        ch = CYR2LAT.get(ch, ch)
        if ch.isalnum():
            out.append(ch)
        else:
            out.append(" ")
    return re.sub(r"\s+", " ", "".join(out)).strip()


# ── Розбір склеєного drug_name ───────────────────────────────────────────────
_SER_TOKEN = re.compile(r"^[0-9A-Za-zА-Яа-яІіЇїЄєҐґ][0-9A-Za-zА-Яа-яІіЇїЄєҐґ\-/.]{1,24}$")
_SER_MARK = "Серія №"


def split_drug_full(full):
    """
    Розбирає рядок вигляду
        "НАЗВА, форма опис, Серія № AAA, BBB, Виробник, Країна"
    на (head, series[], manufacturer, all_series, ok).

    Потрібно лише для беквфілу 296 наявних записів — нові пишуться
    з чистих полів, які parse_row() бере прямо з колонок сайту.
    """
    full = (full or "").strip()
    i = full.find(_SER_MARK)
    if i < 0:
        low = full.lower()
        if low.startswith("всі серії") or "всі серії" in low:
            return full, [], None, True, True
        return full, [], None, False, False

    head = full[:i].rstrip(" ,")
    tail = full[i + len(_SER_MARK):].strip()

    low = tail.lower()
    if low.startswith("всі серії"):
        rest = tail[len("всі серії"):].lstrip(" ,")
        return head, [], (rest or None), True, True
    if "додатк" in low[:60]:
        rest = tail.split(",", 1)[1].strip() if "," in tail else None
        return head, [], rest, True, True

    parts = [p.strip() for p in tail.split(",")]
    series, k = [], 0
    for p in parts:
        if p and " " not in p and _SER_TOKEN.match(p):
            series.append(p)
            k += 1
        else:
            break
    manufacturer = ", ".join(parts[k:]).strip() or None
    return head, series, manufacturer, False, bool(series)


def brand_of(head):
    """Торгова назва = перший сегмент до коми, без ® і зайвого."""
    b = (head or "").split(",")[0]
    b = b.replace("®", " ").replace("™", " ")
    return re.sub(r"\s+", " ", b).strip()


# ── Тип припису ──────────────────────────────────────────────────────────────
def kind_of(doc_type):
    """ban | revoke — скасування знімає раніше накладену заборону."""
    t = (doc_type or "").lower()
    if t.startswith("скасув"):
        return "revoke"
    return "ban"


def _date_key(d):
    """'01.04.2026' → '20260401' для сортування."""
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", d or "")
    return (m.group(3) + m.group(2) + m.group(1)) if m else "00000000"


def ym_of(d):
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", d or "")
    return (m.group(3) + "-" + m.group(2)) if m else "0000-00"


# ── Обчислення актуального статусу ───────────────────────────────────────────
def compute_status(records):
    """
    Проставляє record['active'] і record['revoked_by'].

    Ключ — (згорнутий бренд, згорнута серія). Для «всі серії» ключ серії = "*".
    Хронологічно остання дія по ключу перемагає: скасування знімає заборону.
    """
    for r in records:
        r["active"] = (r["kind"] == "ban")
        r["revoked_by"] = None

    buckets = {}
    for r in records:
        keys = [fold(s) for s in r["series"]] or ["*"]
        b = fold_name(r["brand"])
        for k in keys:
            buckets.setdefault((b, k), []).append(r)

    for _key, group in buckets.items():
        group.sort(key=lambda r: (_date_key(r["date"]), r["num"]))
        for idx, r in enumerate(group):
            if r["kind"] != "ban":
                continue
            later = [x for x in group[idx + 1:] if x["kind"] == "revoke"]
            if later:
                rev = later[0]
                r["active"] = False
                r["revoked_by"] = {"num": rev["num"], "date": rev["date"]}
    return records


# ── Побудова записів ─────────────────────────────────────────────────────────
def build_record(uid, num, date, doc_type, full,
                 series_raw=None, manufacturer=None):
    """
    Якщо series_raw/manufacturer передані (нові записи — беруться прямо
    з колонок сайту), використовуємо їх. Інакше розбираємо склеєний рядок.
    """
    if series_raw is not None:
        head = full
        i = full.find(_SER_MARK)
        if i >= 0:
            head = full[:i].rstrip(" ,")
        low = (series_raw or "").strip().lower()
        if not low or low.startswith("всі серії") or "додатк" in low:
            series, all_series, ok = [], True, True
        else:
            series = [p.strip() for p in re.split(r"[,;]", series_raw) if p.strip()]
            series = [s for s in series if " " not in s and _SER_TOKEN.match(s)]
            all_series = False
            ok = bool(series)
        manuf = manufacturer
    else:
        head, series, manuf, all_series, ok = split_drug_full(full)

    return {
        "uid": uid,
        "num": num,
        "date": date,
        "ym": ym_of(date),
        "type": doc_type,
        "kind": kind_of(doc_type),
        "name": head,
        "brand": brand_of(head),
        "series": series,
        "all_series": all_series,
        "manufacturer": manuf,
        "parse_ok": ok,
        # Оригінальний рядок тримаємо лише там, де розбір не вдався —
        # інакше він відновлюється з name+series+manufacturer і лише
        # роздуває dls.json удвічі.
        "full": (None if ok else full),
    }


def build_payload(records, generated_at):
    records = compute_status(records)
    records.sort(key=lambda r: (_date_key(r["date"]), r["num"], r["brand"]))
    return {
        "generated_at": generated_at,
        "source": "https://pub-mex.dls.gov.ua/QLA/DocList.aspx",
        "count": len(records),
        "active_bans": sum(1 for r in records if r["active"]),
        "records": records,
    }


def dumps(payload):
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
