#!/usr/bin/env python3
"""
amoCRM (Inbound, «Причина отказа.» = «Не удалось связаться») → Sendsay (список pl42130)

Добавляет контакт в список и запускает его в сценарий Sendsay (по умолчанию ID 7).
Передаёт: имя, фамилию, компанию, email, id сделки — только контакты с включённым
«Согласием на рекламные рассылки».

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
CONSENT_FIELD_ID = int(os.getenv("AMO_CONSENT_FIELD_ID", "704923"))  # контакт: «Согласие на рекламные рассылки» (чекбокс)

SENDSAY_LOGIN = os.getenv("SENDSAY_LOGIN", "")                      # общий логин аккаунта Sendsay
SENDSAY_APIKEY = os.getenv("SENDSAY_APIKEY", "")
SENDSAY_LIST_ID = os.getenv("SENDSAY_LIST_ID", "pl42130")
SENDSAY_WORKFLOW_ID = os.getenv("SENDSAY_WORKFLOW_ID", "7")            # сценарий «Inbound (не удалось связаться)»; пусто — не запускать
SENDSAY_START_ACTION = os.getenv("SENDSAY_START_ACTION", "sequence.member.start")
# Куда класть данные в Sendsay: «анкета.поле» (существующие поля аккаунта). Email — в member.email.
SENDSAY_FIELDS = json.loads(os.getenv("SENDSAY_FIELDS", json.dumps({
    "first_name": "base.firstName",
    "last_name": "base.lastName",
    "company": "custom.kompaniya",
    "lead_id": "custom.id_sdelki",
})))
LOST_STATUS_ID = 143


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


def cf_value(entity, field_id=None, code=None):
    for f in entity.get("custom_fields_values") or []:
        if (field_id and f.get("field_id") == field_id) or (code and f.get("field_code") == code):
            vals = [str(v.get("value", "")).strip() for v in f.get("values") or [] if v.get("value")]
            return vals
    return []


def has_consent(contact):
    for f in contact.get("custom_fields_values") or []:
        if f.get("field_id") == CONSENT_FIELD_ID:
            return any(v.get("value") in (True, 1, "1", "true", "True") for v in f.get("values") or [])
    return False


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


def build_rows(leads):
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

    rows, skipped = {}, {"no_email": 0, "no_consent": 0}
    for lead in leads:
        emb = lead.get("_embedded") or {}
        lead_company = next(iter(emb.get("companies") or []), None)
        for link in emb.get("contacts") or []:
            if MAIN_CONTACT_ONLY and not link.get("is_main"):
                continue
            c = contacts.get(link["id"])
            if not c:
                continue
            if not has_consent(c):
                skipped["no_consent"] += 1
                continue
            emails = cf_value(c, code="EMAIL")
            if not emails:
                skipped["no_email"] += 1
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
                "lead_id": str(lead["id"]),
            }
    return list(rows.values()), skipped


# ---------- Sendsay ----------
SENDSAY_URL = f"https://api.sendsay.ru/general/api/v100/json/{SENDSAY_LOGIN}"


def sendsay(action, payload, raise_errors=True):
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
        if data.get("errors") and raise_errors:
            raise RuntimeError(f"Sendsay {action}: {json.dumps(data['errors'], ensure_ascii=False)}")
        return data
    raise RuntimeError("Sendsay: слишком много редиректов")


def upsert_member(row):
    """Создать/обновить контакт, записать поля и добавить в список — синхронно."""
    datakey = []
    for k in ("first_name", "last_name", "company", "lead_id"):
        if row.get(k):  # пустым не затираем то, что уже есть в Sendsay
            datakey.append([SENDSAY_FIELDS[k], "set", row[k]])
    datakey.append([f"-group.{SENDSAY_LIST_ID}", "set", "1"])
    sendsay("member.set", {
        "email": row["email"],
        "addr_type": "email",
        "if_exists": "must_merge",
        "newbie.confirm": 0,
        "datakey": datakey,
    })


def start_workflow(email):
    """Запустить контакт в сценарий. Повторные прохождения в сценарии выключены —
    Sendsay сам не пустит контакт второй раз, такие ответы считаем «уже был»."""
    res = sendsay(SENDSAY_START_ACTION, {"id": int(SENDSAY_WORKFLOW_ID), "email": email, "addr_type": "email"},
                  raise_errors=False)
    errors = res.get("errors") or []
    if not errors:
        return "started", res
    text = json.dumps(errors, ensure_ascii=False)
    if any(w in text.lower() for w in ("already", "exists", "уже", "repeat", "повтор")):
        return "already", text
    return "error", text


def push_to_sendsay(rows, start=True, verbose=False):
    stats = {"saved": 0, "started": 0, "already": 0, "errors": 0}
    do_start = start and bool(SENDSAY_WORKFLOW_ID)
    for r in rows:
        try:
            upsert_member(r)
            stats["saved"] += 1
        except Exception as e:
            stats["errors"] += 1
            print(f"  ! {r['email']}: не удалось записать в Sendsay — {e}")
            continue
        if do_start:
            status, info = start_workflow(r["email"])
            stats[status if status != "error" else "errors"] += 1
            if status == "error" or verbose:
                print(f"  {'!' if status == 'error' else '·'} {r['email']}: сценарий {SENDSAY_WORKFLOW_ID} — {status}: {info}")
        time.sleep(0.1)
    print(f"  → Sendsay: в списке {SENDSAY_LIST_ID}: {stats['saved']}"
          + (f"; сценарий {SENDSAY_WORKFLOW_ID}: запущено {stats['started']}, уже проходили {stats['already']}"
             if do_start else "; сценарий не запускался")
          + f"; ошибок: {stats['errors']}")
    return stats


# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="все сделки воронки, без окна по дате изменения")
    ap.add_argument("--since-days", type=int, help="с --full: только сделки, изменённые за N дней")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--describe", action="store_true")
    ap.add_argument("--test-email")
    ap.add_argument("--no-start", action="store_true", help="только список, без запуска сценария")
    args = ap.parse_args()

    missing = [n for n, v in [("AMO_TOKEN", AMO_TOKEN), ("SENDSAY_LOGIN", SENDSAY_LOGIN),
                              ("SENDSAY_APIKEY", SENDSAY_APIKEY)] if not v]
    if missing and not (args.dry_run or args.describe):
        sys.exit(f"Не заданы переменные: {', '.join(missing)}")

    if args.test_email:
        push_to_sendsay([{"email": args.test_email.lower(), "first_name": "Тест", "last_name": "Интеграции",
                          "company": "Solar Staff", "lead_id": "0"}], start=not args.no_start, verbose=True)
        print(f"Проверь в Sendsay: {args.test_email} в списке {SENDSAY_LIST_ID} и среди участников сценария {SENDSAY_WORKFLOW_ID}.")
        return

    if args.describe:
        print(f"Воронка: {PIPELINE_ID}, поле причины: {REASON_FIELD_ID} = «{REASON_VALUE}», только этап 143: {ONLY_LOST}")
        consent = amo_get(f"/api/v4/contacts/custom_fields/{CONSENT_FIELD_ID}")
        print(f"Поле согласия у контакта: {CONSENT_FIELD_ID} — «{consent.get('name')}» (тип {consent.get('type')})")
        if SENDSAY_LOGIN and SENDSAY_APIKEY:
            res = sendsay("sequence.list", {}, raise_errors=False)
            print("\nСценарии Sendsay (sequence.list):", json.dumps(res.get("list") or res.get("errors") or res,
                                                               ensure_ascii=False)[:3000])
            for a in sendsay("anketa.list", {}).get("list") or []:
                print(f"\nАнкета Sendsay «{a.get('name')}» — id: {a.get('id')}")
                obj = sendsay("anketa.get", {"id": a["id"]}).get("obj") or {}
                for qid, q in (obj.get("quests") or {}).items():
                    print(f"   {a['id']}.{qid}  —  {q.get('name')}")
        return

    updated_from = None
    if not args.full:
        updated_from = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    elif args.since_days:
        updated_from = datetime.now(timezone.utc) - timedelta(days=args.since_days)

    print(f"amoCRM: ищу сделки Inbound с причиной «{REASON_VALUE}»"
          + (f", изменённые с {updated_from:%Y-%m-%d %H:%M} UTC" if updated_from else " (вся история)"))
    leads = list(fetch_leads(updated_from))
    rows, skipped = build_rows(leads)
    print(f"  сделок: {len(leads)}, к отправке: {len(rows)}; пропущено — без согласия: {skipped['no_consent']}, "
          f"без email: {skipped['no_email']}")

    if args.dry_run:
        for r in rows[:50]:
            print("  ", json.dumps(r, ensure_ascii=False))
        if len(rows) > 50:
            print(f"   … и ещё {len(rows) - 50}")
        return
    if rows:
        push_to_sendsay(rows, start=not args.no_start)
    print("Готово.")


if __name__ == "__main__":
    main()
