import os
import hmac
import base64
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

import pytz
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from pydantic import BaseModel

from twilio.rest import Client
from twilio.request_validator import RequestValidator

import gspread
from google.oauth2.service_account import Credentials

# ------------ Env & Globals ------------
TZ = pytz.timezone(os.getenv("TIMEZONE", "Asia/Jerusalem"))
SENDING_WINDOW_START = os.getenv("SENDING_WINDOW_START", "09:00")
SENDING_WINDOW_END = os.getenv("SENDING_WINDOW_END", "17:00")
TEMPLATE_NAME = os.getenv("TWILIO_TEMPLATE_NAME", "tech_entry_request_he")

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM", "")  # e.g., 'whatsapp:+1415...'

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "Hertsliya-Hall-Tech-Bot-10-25")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "google.json")

if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM]):
    print("[WARN] Missing Twilio env vars - set them in Render or .env")

client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# Google Sheets auth
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
if not os.path.exists(GOOGLE_SERVICE_ACCOUNT_JSON):
    print(f"[WARN] Service account JSON not found at {GOOGLE_SERVICE_ACCOUNT_JSON}")
creds = Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_JSON, scopes=SCOPES)
gclient = gspread.authorize(creds)
sheet = gclient.open(GOOGLE_SHEET_NAME)
events_ws = sheet.worksheet("Events")
tech_ws = sheet.worksheet("TechContacts")
log_ws = sheet.worksheet("Log")

app = FastAPI()

# ------------ Helpers ------------
def now_ts() -> str:
    return datetime.now(TZ).isoformat()

def in_sending_window(dt: Optional[datetime] = None) -> bool:
    if dt is None:
        dt = datetime.now(TZ)
    sh, sm = map(int, SENDING_WINDOW_START.split(":"))
    eh, em = map(int, SENDING_WINDOW_END.split(":"))
    start = dt.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end = dt.replace(hour=eh, minute=em, second=0, microsecond=0)
    return start <= dt <= end

def normalize_il_phone(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = "".join(ch for ch in raw if ch.isdigit() or ch == "+")
    s = s.replace(" ", "")
    if s.startswith("+972"):
        return "whatsapp:" + s
    if s.startswith("972"):
        return "whatsapp:+" + s
    if s.startswith("0") and len(s) in (9, 10):  # e.g., 05XXXXXXXX
        return "whatsapp:+972" + s[1:]
    if s.startswith("+"):
        return "whatsapp:" + s
    if len(s) == 10 and s.startswith("05"):
        return "whatsapp:+972" + s[1:]
    return None

def log_event(event_key: str, to_phone: str, direction: str, payload_summary: str, result: str):
    log_ws.append_row([now_ts(), event_key, to_phone, direction, payload_summary, result], value_input_option="RAW")

def read_table(ws) -> List[Dict[str, Any]]:
    rows = ws.get_all_records()
    return rows

def find_row_index_by_key(ws, key: str) -> Optional[int]:
    col_values = ws.col_values(1)  # assuming event_key is column A
    try:
        i = col_values.index(key)
        return i + 1
    except ValueError:
        return None

def update_row_values(ws, row_idx: int, updates: Dict[str, Any]):
    header = ws.row_values(1)
    cell_list = ws.range(row_idx, 1, row_idx, len(header))
    hmap = {name: i for i, name in enumerate(header)}
    row_current = ws.row_values(row_idx)
    if len(row_current) < len(header):
        row_current += [""] * (len(header) - len(row_current))
    for k, v in updates.items():
        if k in hmap:
            row_current[hmap[k]] = v if v is not None else ""
    for j, cell in enumerate(cell_list):
        cell.value = row_current[j] if j < len(row_current) else ""
    ws.update_cells(cell_list, value_input_option="RAW")

def tech_sections() -> List[Dict[str, Any]]:
    # Sections: "06–08", "08–10", ..., "18–20"; each with 30-min items
    sections = []
    for block_start in range(6, 20, 2):
        block_end = block_start + 2
        title = f"{block_start:02d}–{block_end:02d}"
        rows = []
        for hour in range(block_start, block_end):
            for minute in (0, 30):
                hh = f"{hour:02d}"
                mm = f"{minute:02d}"
                rows.append({
                    "id": f"time_{hh}_{mm}",
                    "title": f"{hh}:{mm}",
                    "description": "שעת כניסה להקמות"
                })
        sections.append({"title": title, "rows": rows})
    return sections

def twilio_send_template(to_whatsapp: str, name: str, show: str, date_str: str, show_time_str: str, event_key: str):
    # 1) Open session (simple body matching the approved template text)
    msg = client.messages.create(
        from_=TWILIO_WHATSAPP_FROM,
        to=to_whatsapp,
        body=(
            f"היי {name},\n"
            f"כאן העוזר האישי של יונתן מההיכל בהרצליה.\n"
            f"בנוגע ל“{show}” בתאריך {date_str} בשעה {show_time_str} – מה שעת הכניסה להקמות שנוחה לכם?\n"
            f"אפשר לבחור מכפתורים (מרווחים של 30 דק׳).\n"
            f"אם זה לא אצלך, ניתן להפנות אותנו לאיש הקשר הנכון. תודה!"
        )
    )
    log_event(event_key, to_whatsapp, "out", "template_open", f"sid={msg.sid}")

    # 2) Interactive List
    interactive = {
        "type": "list",
        "header": {"type": "text", "text": f"שעת המופע: {show_time_str}"},
        "body": {"text": "בחר/י שעת כניסה להקמות:"},
        "footer": {"text": "טווח: 06:00–20:00"},
        "action": {
            "button": "בחר/י שעה",
            "sections": tech_sections()
        }
    }
    msg2 = client.messages.create(
        from_=TWILIO_WHATSAPP_FROM,
        to=to_whatsapp,
        interactive=interactive
    )
    log_event(event_key, to_whatsapp, "out", "interactive_list", f"sid={msg2.sid}")

    # 3) Meta buttons
    buttons = {
        "type": "button",
        "body": {"text": "אפשרויות נוספות:"},
        "action": {
            "buttons": [
                {"type": "reply", "reply": {"id": "btn_unknown", "title": "אני עוד לא יודע"}},
                {"type": "reply", "reply": {"id": "btn_redirect", "title": "אני לא איש הקשר"}},
            ]
        }
    }
    msg3 = client.messages.create(
        from_=TWILIO_WHATSAPP_FROM,
        to=to_whatsapp,
        interactive=buttons
    )
    log_event(event_key, to_whatsapp, "out", "meta_buttons", f"sid={msg3.sid}")

def twilio_send_followup(to_whatsapp: str, show: str, date_str: str, show_time_str: str, event_key: str):
    msg = client.messages.create(
        from_=TWILIO_WHATSAPP_FROM,
        to=to_whatsapp,
        body=f"היי, חוזרים לגבי “{show}” בתאריך {date_str} בשעה {show_time_str}. נוכל לקבוע שעת כניסה להקמות?",
    )
    log_event(event_key, to_whatsapp, "out", "followup_template", f"sid={msg.sid}")

    interactive = {
        "type": "list",
        "header": {"type": "text", "text": f"שעת המופע: {show_time_str}"},
        "body": {"text": "בחר/י שעת כניסה להקמות:"},
        "footer": {"text": "טווח: 06:00–20:00"},
        "action": {
            "button": "בחר/י שעה",
            "sections": tech_sections()
        }
    }
    msg2 = client.messages.create(
        from_=TWILIO_WHATSAPP_FROM,
        to=to_whatsapp,
        interactive=interactive
    )
    log_event(event_key, to_whatsapp, "out", "interactive_list", f"sid={msg2.sid}")

def send_followup_choice_buttons(to_whatsapp: str, event_key: str):
    buttons = {
        "type": "button",
        "body": {"text": "מתי תרצה שנבדוק שוב?"},
        "action": {
            "buttons": [
                {"type": "reply", "reply": {"id": "btn_followup_1d", "title": "מחר"}},
                {"type": "reply", "reply": {"id": "btn_followup_3d", "title": "עוד 3 ימים"}},
                {"type": "reply", "reply": {"id": "btn_followup_7d", "title": "שבוע"}},
            ]
        }
    }
    msg = client.messages.create(
        from_=TWILIO_WHATSAPP_FROM,
        to=to_whatsapp,
        interactive=buttons
    )
    log_event(event_key, to_whatsapp, "out", "followup_buttons", f"sid={msg.sid}")

def parse_selected_time_id(time_id: str) -> Optional[str]:
    try:
        _, hh, mm = time_id.split("_")
        return f"{int(hh):02d}:{int(mm):02d}"
    except Exception:
        return None

def compute_entity_key(contact_name: str, show_name: str) -> str:
    base = (contact_name or "").strip().lower()
    if not base:
        base = (show_name or "").strip().lower()
    return "_".join(base.split())

def upsert_tech_contact(tech_ws, entity_key: str, name: str, phone_e164: str, source_event_key: str):
    rows = tech_ws.get_all_records()
    header = tech_ws.row_values(1)
    ek_idx = header.index("entity_key") + 1 if "entity_key" in header else 1
    to_update_row = None
    for i, r in enumerate(rows, start=2):
        if str(r.get("entity_key", "")).strip().lower() == entity_key:
            to_update_row = i
            break
    payload = {
        "entity_key": entity_key,
        "tech_contact_name": name or "",
        "tech_contact_phone_e164": phone_e164 or "",
        "source_event_key": source_event_key,
        "last_verified_at": now_ts(),
        "notes": "",
    }
    if to_update_row:
        update_row_values(tech_ws, to_update_row, payload)
    else:
        tech_ws.append_row([payload[k] for k in ["entity_key","tech_contact_name","tech_contact_phone_e164","source_event_key","last_verified_at","notes"]], value_input_option="RAW")

# ------------ Webhook & API ------------
@app.post("/twilio/webhook")
async def twilio_webhook(request: Request, background_tasks: BackgroundTasks):
    # Verify Twilio signature
    validator = RequestValidator(TWILIO_AUTH_TOKEN)
    form = await request.form()
    url = str(request.url)
    signature = request.headers.get("X-Twilio-Signature", "")
    if not validator.validate(url, dict(form), signature):
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")

    from_ = form.get("From")  # 'whatsapp:+972...'
    # Interactive payloads:
    button_id = form.get("ButtonPayload") or form.get("ButtonReplyId") or ""
    list_id = form.get("ListReplyId") or ""
    contact_vcard = form.get("AttachedContacts")

    # Find target event by phone
    rows = read_table(events_ws)
    target_idx = None
    target = None
    for i, r in enumerate(rows, start=2):
        if r.get("contact_phone_e164") == from_ and r.get("status") in ("Sent", "FollowUp", "Waiting"):
            target_idx = i
            target = r
    if not target:
        log_event("-", from_ or "-", "in", "unmatched_incoming", "ignored")
        return {"ok": True}

    ek = target["event_key"]
    show_name = target.get("show_name","")
    event_date = target.get("event_date","")
    show_time = target.get("show_time","")

    if list_id:
        chosen = parse_selected_time_id(list_id)
        if chosen:
            update_row_values(events_ws, target_idx, {
                "tech_entry_time": chosen,
                "chosen_time": chosen,
                "status": "Confirmed",
                "response_type": "hour",
                "last_inbound_at": now_ts(),
                "updated_at": now_ts(),
            })
            log_event(ek, from_, "in", f"time_selected={chosen}", "success")
            client.messages.create(
                from_=TWILIO_WHATSAPP_FROM,
                to=from_,
                body=f"מעולה, שמרנו {chosen} כשעת הכניסה להקמות. נתראה ב{show_time}!"
            )
            return {"ok": True}

    if button_id == "btn_unknown":
        send_followup_choice_buttons(from_, ek)
        update_row_values(events_ws, target_idx, {
            "response_type": "unknown",
            "status": "FollowUp",
            "last_inbound_at": now_ts(),
            "updated_at": now_ts(),
        })
        log_event(ek, from_, "in", "unknown_clicked", "success")
        return {"ok": True}

    if button_id in ("btn_followup_1d", "btn_followup_3d", "btn_followup_7d"):
        delta = {"btn_followup_1d": 1, "btn_followup_3d": 3, "btn_followup_7d": 7}[button_id]
        due = (datetime.now(TZ) + timedelta(days=delta)).isoformat()
        update_row_values(events_ws, target_idx, {
            "followup_date": due,
            "status": "FollowUp",
            "updated_at": now_ts(),
        })
        log_event(ek, from_, "in", f"followup_set={delta}d", "success")
        client.messages.create(
            from_=TWILIO_WHATSAPP_FROM,
            to=from_,
            body="מסומן. נחזור אליך בהתאם."
        )
        return {"ok": True}

    if button_id == "btn_redirect":
        client.messages.create(
            from_=TWILIO_WHATSAPP_FROM,
            to=from_,
            body="מי איש הקשר הטכני הנכון? אפשר לשתף כאן כרטיס איש קשר (Contact)."
        )
        log_event(ek, from_, "in", "redirect_clicked", "await_contact")
        update_row_values(events_ws, target_idx, {
            "response_type": "redirect",
            "last_inbound_at": now_ts(),
            "updated_at": now_ts(),
        })
        return {"ok": True}

    if contact_vcard:
        new_name = form.get("ContactName") or "איש קשר טכני"
        new_phone_raw = form.get("ContactPhone") or ""
        new_e164 = normalize_il_phone(new_phone_raw)
        if new_e164:
            entity_key = compute_entity_key(target.get("contact_name",""), show_name)
            upsert_tech_contact(tech_ws, entity_key, new_name, new_e164, ek)
            update_row_values(events_ws, target_idx, {
                "contact_name": new_name,
                "contact_phone_e164": new_e164,
                "updated_at": now_ts(),
                "notes": f"redirected_from={from_}",
            })
            twilio_send_template(new_e164, new_name, show_name, event_date, show_time, ek)
            client.messages.create(
                from_=TWILIO_WHATSAPP_FROM,
                to=from_,
                body="תודה! פנינו לאיש הקשר הנכון."
            )
            log_event(ek, new_e164, "out", "template_to_new_contact", "sent")
            return {"ok": True}
        else:
            client.messages.create(
                from_=TWILIO_WHATSAPP_FROM,
                to=from_,
                body="לא הצלחתי לקרוא את מספר הטלפון מהכרטיס. אפשר לכתוב את המספר בפורמט 05XXXXXXXX?"
            )
            log_event(ek, from_, "in", "contact_card_invalid", "ask_plain_number")
            return {"ok": True}

    body = (form.get("Body") or "").strip()
    if body.startswith("05"):
        new_e164 = normalize_il_phone(body)
        if new_e164:
            entity_key = compute_entity_key(target.get("contact_name",""), show_name)
            upsert_tech_contact(tech_ws, entity_key, "איש קשר טכני", new_e164, ek)
            update_row_values(events_ws, target_idx, {
                "contact_name": "איש קשר טכני",
                "contact_phone_e164": new_e164,
                "updated_at": now_ts(),
                "notes": f"redirected_from={from_}",
            })
            twilio_send_template(new_e164, "שלום", show_name, event_date, show_time, ek)
            client.messages.create(
                from_=TWILIO_WHATSAPP_FROM,
                to=from_,
                body="תודה! פנינו לאיש הקשר הטכני."
            )
            log_event(ek, new_e164, "out", "template_to_new_contact", "sent")
            return {"ok": True}

    log_event(ek, from_, "in", "unhandled_incoming", "noop")
    return {"ok": True}

# ---- Outbound APIs ----
class KickRequest(BaseModel):
    limit: int = 20

@app.post("/ops/kick")
def kick_send(req: KickRequest):
    if not in_sending_window():
        raise HTTPException(status_code=409, detail="Outside sending window")
    rows = read_table(events_ws)
    sent = 0
    for i, r in enumerate(rows, start=2):
        status = (r.get("status") or "Waiting").strip()
        event_date = r.get("event_date","")
        show_time = r.get("show_time","")
        show_name = r.get("show_name","")
        phone_raw = r.get("contact_phone_raw","")
        phone_e164 = r.get("contact_phone_e164") or normalize_il_phone(phone_raw)
        if phone_e164 and r.get("event_key"):
            if status in ("Waiting","FollowUp"):
                followup_date = r.get("followup_date","")
                ok_followup = True
                if followup_date:
                    try:
                        ok_followup = datetime.fromisoformat(followup_date) <= datetime.now(TZ)
                    except Exception:
                        ok_followup = True
                try:
                    ed = datetime.fromisoformat(str(event_date))
                    if ed.date() < datetime.now(TZ).date():
                        continue
                except Exception:
                    pass

                if ok_followup and sent < req.limit:
                    ek = r["event_key"]
                    name = r.get("contact_name","") or "שלום"
                    update_row_values(events_ws, i, {
                        "contact_phone_e164": phone_e164,
                        "status": "Sent",
                        "last_outbound_at": now_ts(),
                        "updated_at": now_ts(),
                    })
                    twilio_send_template(phone_e164, name, show_name, str(event_date), str(show_time), ek)
                    sent += 1
        else:
            if r.get("event_key"):
                update_row_values(events_ws, i, {
                    "status": "Failed",
                    "notes": "Missing/invalid phone",
                    "updated_at": now_ts(),
                })
                log_event(r["event_key"], phone_e164 or "-", "out", "send_attempt", "failed_missing_phone")
    return {"sent": sent}

@app.post("/ops/followup_sweep")
def followup_sweep():
    if not in_sending_window():
        raise HTTPException(status_code=409, detail="Outside sending window")
    rows = read_table(events_ws)
    sent = 0
    for i, r in enumerate(rows, start=2):
        status = r.get("status","")
        if status not in ("FollowUp","Sent"):
            continue
        ek = r.get("event_key","")
        event_date = r.get("event_date","")
        show_time = r.get("show_time","")
        show_name = r.get("show_name","")
        to_whatsapp = r.get("contact_phone_e164") or normalize_il_phone(r.get("contact_phone_raw",""))
        if not to_whatsapp:
            continue

        last_in = r.get("last_inbound_at","")
        last_out = r.get("last_outbound_at","")
        try:
            last_in_dt = datetime.fromisoformat(last_in) if last_in else None
        except:
            last_in_dt = None
        try:
            last_out_dt = datetime.fromisoformat(last_out) if last_out else None
        except:
            last_out_dt = None
        need_followup48 = False
        if last_out_dt and (datetime.now(TZ) - last_out_dt) >= timedelta(hours=48) and (not last_in_dt or last_in_dt < last_out_dt):
            need_followup48 = True

        followup_due = False
        fd = r.get("followup_date","")
        if fd:
            try:
                followup_due = datetime.fromisoformat(fd) <= datetime.now(TZ)
            except:
                pass

        if need_followup48 or followup_due:
            twilio_send_followup(to_whatsapp, show_name, str(event_date), str(show_time), ek)
            update_row_values(events_ws, i, {
                "status": "FollowUp",
                "last_outbound_at": now_ts(),
                "updated_at": now_ts(),
            })
            sent += 1

        try:
            ed = datetime.fromisoformat(str(event_date))
            if (ed - datetime.now(TZ)) <= timedelta(days=10) and r.get("tech_entry_time","") == "":
                update_row_values(events_ws, i, {
                    "status": "NeedHuman",
                    "updated_at": now_ts(),
                })
        except:
            pass

    return {"followups_sent": sent}

@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": now_ts()}
