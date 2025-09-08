import os, json, datetime, re
from fastapi import FastAPI, Request
import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------- ENV ----------
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "WhatsApp Ingest")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")  # full JSON string
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_MESSAGING_SERVICE_SID = os.getenv("TWILIO_MESSAGING_SERVICE_SID")  # or set TWILIO_FROM
TWILIO_FROM = os.getenv("TWILIO_FROM")  # e.g., "whatsapp:+1415...."
LOOKBACK_HOURS = int(os.getenv("INCOMING_LOOKBACK_HOURS", "72"))

SHEET_OUTBOX = os.getenv("SHEET_OUTBOX", "Outbox")
SHEET_MESSAGES = os.getenv("SHEET_MESSAGES", "Messages")
SHEET_CONVERSATIONS = os.getenv("SHEET_CONVERSATIONS", "Conversations")

app = FastAPI()

# ---------- Google Sheets ----------
def gs_client():
    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

def open_sheets():
    gc = gs_client()
    sh = gc.open(GOOGLE_SHEET_NAME)
    return sh.worksheet(SHEET_OUTBOX), sh.worksheet(SHEET_MESSAGES), sh.worksheet(SHEET_CONVERSATIONS)

def headers_map(ws):
    hdrs = ws.row_values(1)
    return {h:i for i,h in enumerate(hdrs)}, hdrs

def now_iso_utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def ensure_e164(phone: str):
    p = (phone or "").strip().replace("whatsapp:","")
    if p.startswith("+"):
        return p
    if re.match(r"^0[2-9]\d+$", p):
        return "+972" + p[1:]
    raise ValueError(f"phone not E.164: {phone}")

def rand_id(n=4):
    import random, string
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choice(chars) for _ in range(n))

# ---------- Messages log ----------
def message_exists(ws_messages, sid):
    if not sid: return False
    col = ws_messages.col_values(ws_messages.find("message_sid").col)[1:]
    return sid in col

def append_message(ws_messages, obj: dict):
    m, hdrs = headers_map(ws_messages)
    row = [""] * len(hdrs)
    def setcol(name, val):
        if name in m: row[m[name]] = val
    for k,v in obj.items():
        setcol(k, v)
    # idempotency
    if obj.get("message_sid") and message_exists(ws_messages, obj["message_sid"]):
        return
    ws_messages.append_row(row, value_input_option="RAW")

def update_delivery_status(ws_messages, message_sid, status):
    cell = ws_messages.find("message_sid")
    sid_col = cell.col
    del_col = ws_messages.find("delivery_status").col
    sids = ws_messages.col_values(sid_col)[1:]
    for i, val in enumerate(sids, start=2):
        if val == message_sid:
            ws_messages.update_cell(i, del_col, status)
            return

# ---------- Conversation linking ----------
def resolve_conversation(ws_outbox, contact_e164, corr_id):
    m, _ = headers_map(ws_outbox)
    last_row = ws_outbox.row_count
    values = ws_outbox.get_all_values()[1:]  # without header

    # by correlation id
    if corr_id:
        for row in reversed(values):
            if row[m["correlation_id"]] == corr_id and row[m["contact_phone_e164"]] == contact_e164:
                conv = row[m["conversation_id"]] or f'{row[m["event_id"]]}:{contact_e164}'
                return row[m["event_id"]], conv

    # fallback by recent sent_at and same phone
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=LOOKBACK_HOURS)
    best = None; best_ts = None
    for row in values:
        if row[m["contact_phone_e164"]] != contact_e164: 
            continue
        ts = row[m["sent_at_utc"]]
        try:
            dt = datetime.datetime.fromisoformat(ts)
        except:
            continue
        if dt >= cutoff and (best_ts is None or dt > best_ts):
            best = row; best_ts = dt
    if best:
        conv = best[m["conversation_id"]] or f'{best[m["event_id"]]}:{contact_e164}'
        return best[m["event_id"]], conv
    return "", f'unknown:{contact_e164}'

def upsert_conversation(ws_conversations, payload: dict):
    m, hdrs = headers_map(ws_conversations)
    conv_col = ws_conversations.find("conversation_id").col
    ids = ws_conversations.col_values(conv_col)[1:]
    try:
        idx = ids.index(payload["conversation_id"]) + 2
        # update existing row fields
        for k,v in payload.items():
            if k in m:
                ws_conversations.update_cell(idx, m[k]+1, v)
    except ValueError:
        # append new
        row = [""] * len(hdrs)
        for k,v in payload.items():
            if k in m: row[m[k]] = v
        ws_conversations.append_row(row, value_input_option="RAW")

# ---------- Twilio ----------
def twilio_send(to_e164, body):
    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
    data = {"To": f"whatsapp:{to_e164}", "Body": body}
    if TWILIO_MESSAGING_SERVICE_SID:
        data["MessagingServiceSid"] = TWILIO_MESSAGING_SERVICE_SID
    elif TWILIO_FROM:
        data["From"] = TWILIO_FROM
    else:
        raise RuntimeError("Set TWILIO_MESSAGING_SERVICE_SID or TWILIO_FROM")
    resp = requests.post(url, data=data, auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN))
    resp.raise_for_status()
    return resp.json()

# ---------- Endpoints ----------
@app.post("/twilio/incoming")
async def twilio_incoming(request: Request):
    form = await request.form()
    params = dict(form)
    body = params.get("Body", "")
    from_ = params.get("From", "")
    to_ = params.get("To", "")
    profile = params.get("ProfileName", "")
    message_sid = params.get("MessageSid") or params.get("SmsMessageSid") or ""
    message_status = params.get("MessageStatus") or params.get("SmsStatus") or ""

    ws_out, ws_msg, ws_conv = open_sheets()

    # Status callback (delivery updates)
    if message_status and not body:
        update_delivery_status(ws_msg, message_sid, message_status)
        return {"ok": True, "type": "status", "status": message_status}

    # Incoming message
    if body and from_:
        ts = now_iso_utc()
        contact = ensure_e164(from_)
        # try extract correlation code: "קוד אישור: ABCD" or "code: ABCD"
        m = re.search(r"(?:קוד\s*אישור|code)\s*:\s*([A-Z0-9]{4})", body, re.I)
        corr = m.group(1).upper() if m else ""
        event_id, conv_id = resolve_conversation(ws_out, contact, corr)

        append_message(ws_msg, {
            "ts_utc": ts,
            "direction": "in",
            "contact_phone_e164": contact,
            "wa_from": from_,
            "wa_to": to_,
            "profile_name": profile,
            "body_raw": body,
            "message_sid": message_sid,
            "correlation_id": corr,
            "conversation_id": conv_id,
            "event_id": event_id,
            "delivery_status": "",
            "error": ""
        })

        upsert_conversation(ws_conv, {
            "conversation_id": conv_id,
            "event_id": event_id,
            "contact_phone_e164": contact,
            "contact_name": profile or "",
            "last_message_at_utc": ts,
            "last_direction": "in",
            "last_body": body
        })

        return {"ok": True, "type": "incoming"}

    return {"ok": True, "type": "noop"}

@app.post("/send-pending")
def send_pending():
    ws_out, ws_msg, ws_conv = open_sheets()
    m, hdrs = headers_map(ws_out)
    rows = ws_out.get_all_values()
    if len(rows) <= 1:
        return {"sent": 0}
    sent = 0
    updates = []
    for i, row in enumerate(rows[1:], start=2):
        status = row[m["status"]] if "status" in m else ""
        body = row[m["body_to_send"]] if "body_to_send" in m else ""
        phone = row[m["contact_phone_e164"]] if "contact_phone_e164" in m else ""
        if not body or not phone: 
            continue
        if status not in ("", "queued"):
            continue

        # ensure conversation_id
        event_id = row[m["event_id"]]
        phone_e164 = ensure_e164(phone)
        conv_id = row[m["conversation_id"]] or f"{event_id}:{phone_e164}"
        corr = row[m["correlation_id"]] or rand_id(4)

        body_with_code = f"{body}\n\nקוד אישור: {corr}"
        resp = twilio_send(phone_e164, body_with_code)
        sid = resp.get("sid","")
        sent_at = now_iso_utc()

        # append to Messages (out)
        append_message(ws_msg, {
            "ts_utc": sent_at,
            "direction": "out",
            "contact_phone_e164": phone_e164,
            "wa_from": "",
            "wa_to": "",
            "profile_name": "",
            "body_raw": body_with_code,
            "message_sid": sid,
            "correlation_id": corr,
            "conversation_id": conv_id,
            "event_id": event_id,
            "delivery_status": "sent",
            "error": ""
        })

        # queue update for Outbox row
        row[m["conversation_id"]] = conv_id
        row[m["correlation_id"]] = corr
        row[m["twilio_message_sid"]] = sid
        row[m["sent_at_utc"]] = sent_at
        row[m["status"]] = "sent"
        updates.append((i, row))
        sent += 1

    # bulk-ish update (row-by-row; gspread לא תומך בטרנזקציה)
    for i, newrow in updates:
        ws_out.update(f"A{i}:{gspread.utils.rowcol_to_a1(1, len(newrow)).replace('1', str(i))}", [newrow], value_input_option="RAW")

    return {"sent": sent}
