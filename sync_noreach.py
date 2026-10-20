#!/usr/bin/env python3
"""
amoCRM (Inbound, «Причина отказа.» = «Не удалось связаться») → Sendsay (список pl42130)

Передаёт: имя, фамилию, компанию, телефон, email, должность, сферу компании.

Режимы:
  python sync_noreach.py                 # инкремент: сделки, изменённые за LOOKBACK_HOURS
  python sync_noreach.py --full          # бэкфилл: все сделки воронки (или за --since-days)
  python sync_noreach.py --dry-run       # ничего не отправляет, печатает, кого бы отправил
  python sync_noreach.py --describe      # печатает найденные поля amoCRM и анкеты Sendsay
  python sync_noreach.py --test-email you@solar-staff.com   # отправить в список один тестовый адрес

Все настройки — через переменные окружения (см. README.md).
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

# ---------- настройки ----------
AMO_DOMAIN = os.getenv("AMO_DOMAIN", "solarstaff.amocrm.ru")
AMO_TOKEN = os.getenv("AMO_TOKEN", "")
PIPELINE_ID = int(os.getenv("AMO_PIPELINE_ID", "8454514"))          # Inbound
REASON_FIELD_ID = int(os.getenv("AMO_REASON_FIELD_ID", "701893"))   # «Причина отказа.»
REASON_VALUE = os.getenv("AMO_REASON_VALUE", "Не удалось связаться")
ONLY_LOST = os.getenv("ONLY_LOST", "1") == "1"                      # только этап «Закрыто и не реализовано» (143)
MAIN_CONTACT_ONLY = os.getenv("MAIN_CONTACT_ONLY", "1") == "1"
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "3"))
POSITION_FIELD_ID = os.getenv("AMO_POSITION_FIELD_ID")              # если не задано — ищем по названию «Должность»
INDUSTRY_FIELD_ID = os.getenv("AMO_INDUSTRY_FIELD_ID")              # если не задано — ищем по названию «Сфера»

SENDSAY_LOGIN = os.getenv("SENDSAY_LOGIN", "")                      # общий логин аккаунта Sendsay
SENDSAY_APIKEY = os.getenv("SENDSAY_APIKEY", "")
SENDSAY_LIST_ID = os.getenv("SENDSAY_LIST_ID", "pl42130")
# Куда класть данные в Sendsay: «анкета.поле». Анкету нужно создать заранее (README, шаг 2).
SENDSAY_FIELDS = json.loads(os.getenv("SENDSAY_FIELDS", json.dumps({
    "first_name": "amo.first_name",
    "last_name": "amo.last_name",
    "company": "amo.company",
    "phone": "amo.phone",
    "position": "amo.position",
    "industry": "amo.industry",
    "lead_id": "amo.lead_id",
})))
LOST_STATUS_ID = 143
BATCH = 500


# ---------- amoCRM ----------
session = requests.Session()


def amo_get(path, params=None):
    url = f"https://{AMO_DOMAIN}{path}"
    for attempt in range(5):
        r = session.get(url, params=params, headers={"Authorization": f"Bearer {AMO_TOKEN}"}, timeout=60)
        if r.status_code == 429:
            time.sleep(1 + attempt)
            continue
        if r.status_code == 204:
            return {}
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"amoCRM 429 слишком долго: {path}")


def amo_paginate(path, key, params=None):
    params = dict(params or {})
    params.setdefault("limit", 250)
    page = 1
    while True:
        params["page"] = page
        data = amo_get(path, params)
        items = (data.get("_embedded") or {}).get(key) or []
        if not items:
            return
        yield from items
        if not (data.get("_links") or {}).get("next"):
            return
        page += 1
        time.sleep(0.15)  # лимит amo ~7 rps


def amo_by_ids(path, key, ids, extra=None):
    ids = list(ids)
    out = {}
    for i in range(0, len(ids), 50):
        params = {f"filter[id][{j}]": v for j, v in enumerate(ids[i:i + 50])}
        params.update(extra or {})
        for item in amo_paginate(path, key, params):
            out[item["id"]] = item
    return out


def find_field(entity, explicit_id, name_part, exclude=()):
    if explicit_id:
        return int(explicit_id)
    fields = list(amo_paginate(f"/api/v4/{entity}/custom_fields", "custom_fields"))
    exact = [f for f in fields if f["name"].strip(" •").lower() == name_part.lower()]
    if exact:
        return exact[0]["id"]
    loose = [f for f in fields if name_part.lower() in f["name"].lower()
             and not any(x.lower() in f["name"].lower() for x in exclude)]
    return loose[0]["id"] if loose else None


def cf_value(entity, field_id=None, code=None):
    for f in entity.get("custom_fields_values") or []:
        if (field_id and f.get("field_id") == field_id) or (code and f.get("field_code") == code):
            vals = [str(v.get("value", "")).strip() for v in f.get("values") or [] if v.get("value")]
            return vals
    return []


def has_reason(lead):
    return any(v == REASON_VALUE for v in cf_value(lead, field_id=REASON_FIELD_ID))


def fetch_leads(updated_from=None):
    params = {"filter[pipeline_id][0]": PIPELINE_ID, "with": "contacts"}
    if updated_from:
        params["filter[updated_at][from]"] = int(updated_from.timestamp())
    for lead in amo_paginate("/api/v4/leads", "leads", params):
        if lead.get("pipeline_id") != PIPELINE_ID:
            continue
        if ONLY_LOST and lead.get("status_id") != LOST_STATUS_ID:
            continue
        if has_reason(lead):
            yield lead


def build_rows(leads, position_fid, industry_fid):
    contact_ids, company_ids = set(), set()
    for lead in leads:
        emb = lead.get("_embedded") or {}
        for c in emb.get("contacts") or []:
            if c.get("is_main") or not MAIN_CONTACT_ONLY:
                contact_ids.add(c["id"])
        for co in emb.get("companies") or []:
            company_ids.add(co["id"])

    contacts = amo_by_ids("/api/v4/contacts", "contacts", contact_ids)
    for c in contacts.values():  # компания контакта — запасной вариант, если у сделки нет компании
        for co in (c.get("_embedded") or {}).get("companies") or []:
            company_ids.add(co["id"])
    companies = amo_by_ids("/api/v4/companies", "companies", company_ids)

    rows, skipped_no_email = {}, 0
    for lead in leads:
        emb = lead.get("_embedded") or {}
        lead_company = next(iter(emb.get("companies") or []), None)
        for link in emb.get("contacts") or []:
            if MAIN_CONTACT_ONLY and not link.get("is_main"):
                continue
            c = contacts.get(link["id"])
            if not c:
                continue
            emails = cf_value(c, code="EMAIL")
            if not emails:
                skipped_no_email += 1
                continue
            co_ref = lead_company or next(iter((c.get("_embedded") or {}).get("companies") or []), None)
            co = companies.get(co_ref["id"]) if co_ref else None

            first, last = (c.get("first_name") or "").strip(), (c.get("last_name") or "").strip()
            if not first and not last and c.get("name"):
                parts = c["name"].split(maxsplit=1)
                first, last = parts[0], (parts[1] if len(parts) > 1 else "")

            email = emails[0].lower()
            rows[email] = {  # дедуп по email: последняя сделка перезаписывает
                "email": email,
                "first_name": first,
                "last_name": last,
                "company": (co or {}).get("name", "") or "",
                "phone": ", ".join(cf_value(c, code="PHONE")),
                "position": ", ".join(cf_value(c, field_id=position_fid)) if position_fid else "",
                "industry": ", ".join(cf_value(co, field_id=industry_fid)) if (co and industry_fid) else "",
                "lead_id": str(lead["id"]),
            }
    return list(rows.values()), skipped_no_email


# ---------- Sendsay ----------
SENDSAY_URL = f"https://api.sendsay.ru/general/api/v100/json/{SENDSAY_LOGIN}"


def sendsay(action, payload):
    global SENDSAY_URL
    body = {"action": action, **payload}
    for _ in range(3):
        r = requests.post(SENDSAY_URL, json=body,
                          headers={"Authorization": f"sendsay apikey={SENDSAY_APIKEY}"}, timeout=120)
        r.raise_for_status()
        data = r.json()
        if "REDIRECT" in data:  # Sendsay может перенаправить на другой сервер аккаунта
            SENDSAY_URL = "https://api.sendsay.ru/general/api/v100/json" + data["REDIRECT"]
            continue
        if data.get("errors"):
            raise RuntimeError(f"Sendsay {action}: {json.dumps(data['errors'], ensure_ascii=False)}")
        return data
    raise RuntimeError("Sendsay: слишком много редиректов")


def push_to_sendsay(rows):
    keys = ["first_name", "last_name", "company", "phone", "position", "industry", "lead_id"]
    caption = [{"anketa": "member", "quest": "email"}]
    for k in keys:
        anketa, quest = SENDSAY_FIELDS[k].split(".", 1)
        caption.append({"anketa": anketa, "quest": quest})

    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i + BATCH]
        payload = {
            "addr_type": "email",
            "newbie.confirm": 0,
            "if_exists": "must_merge",
            "users.list": {"caption": caption, "rows": [[r["email"]] + [r[k] for k in keys] for r in chunk]},
            "auto_group": {"id": SENDSAY_LIST_ID, "clean": 0},  # добавить в список, не очищая его
        }
        res = sendsay("member.import", payload)
        print(f"  → Sendsay: отправлено {len(chunk)} (пачка {i // BATCH + 1})")
        track_id = res.get("track.id") or (res.get("track") or {}).get("id")
        if track_id:  # импорт в Sendsay асинхронный — результат виден в «Журнале заданий»
            print(f"    задание Sendsay: {track_id}")


# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="все сделки воронки, без окна по дате изменения")
    ap.add_argument("--since-days", type=int, help="с --full: только сделки, изменённые за N дней")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--describe", action="store_true")
    ap.add_argument("--test-email")
    args = ap.parse_args()

    missing = [n for n, v in [("AMO_TOKEN", AMO_TOKEN), ("SENDSAY_LOGIN", SENDSAY_LOGIN),
                              ("SENDSAY_APIKEY", SENDSAY_APIKEY)] if not v]
    if missing and not (args.dry_run or args.describe):
        sys.exit(f"Не заданы переменные: {', '.join(missing)}")

    if args.test_email:
        push_to_sendsay([{"email": args.test_email.lower(), "first_name": "Тест", "last_name": "Интеграции",
                          "company": "Solar Staff", "phone": "+70000000000", "position": "Тест",
                          "industry": "Тест", "lead_id": "0"}])
        print(f"Готово. Проверь в Sendsay, что в списке {SENDSAY_LIST_ID} появился {args.test_email} с полями.")
        return

    position_fid = find_field("contacts", POSITION_FIELD_ID, "Должность", exclude=("Полное",))
    industry_fid = find_field("companies", INDUSTRY_FIELD_ID, "Сфера")

    if args.describe:
        print(f"Воронка: {PIPELINE_ID}, поле причины: {REASON_FIELD_ID} = «{REASON_VALUE}», только этап 143: {ONLY_LOST}")
        print(f"Контакт «Должность»: field_id={position_fid}")
        print(f"Компания «Сфера»:    field_id={industry_fid}")
        if SENDSAY_LOGIN and SENDSAY_APIKEY:
            print("Анкеты Sendsay:", json.dumps(sendsay("anketa.list", {}).get("list"), ensure_ascii=False, indent=1))
        return

    updated_from = None
    if not args.full:
        updated_from = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    elif args.since_days:
        updated_from = datetime.now(timezone.utc) - timedelta(days=args.since_days)

    print(f"amoCRM: ищу сделки Inbound с причиной «{REASON_VALUE}»"
          + (f", изменённые с {updated_from:%Y-%m-%d %H:%M} UTC" if updated_from else " (вся история)"))
    leads = list(fetch_leads(updated_from))
    rows, no_email = build_rows(leads, position_fid, industry_fid)
    print(f"  сделок: {len(leads)}, контактов с email: {len(rows)}, без email (пропущены): {no_email}")

    if args.dry_run:
        for r in rows[:50]:
            print("  ", json.dumps(r, ensure_ascii=False))
        if len(rows) > 50:
            print(f"   … и ещё {len(rows) - 50}")
        return
    if rows:
        push_to_sendsay(rows)
    print("Готово.")


if __name__ == "__main__":
    main()
