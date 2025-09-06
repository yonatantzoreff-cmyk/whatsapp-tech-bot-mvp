# WhatsApp Tech Entry Bot (MVP)

This is the minimal FastAPI server for your WhatsApp automation:
- Sends a template message
- Shows an Interactive List of times (06:00–20:00, every 30min)
- Handles "I'm not sure" (follow-ups) and "I'm not the contact" (redirect to a new contact)
- Writes/reads from Google Sheets (Events, TechContacts, Log)

## 1) Local run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Put your service account JSON as google.json in the project root, or set GOOGLE_SERVICE_ACCOUNT_JSON to its path
uvicorn app:app --reload --port 10000
```

Expose your local server (e.g., ngrok) and paste the https URL to Twilio WhatsApp webhook: `POST /twilio/webhook`.

## 2) Render deploy
- New Web Service → Connect repo → Build command: *(none)* → Start command (uses Procfile)
- Add env vars from `.env.example`
- Upload your service account JSON as a Secret file, set `GOOGLE_SERVICE_ACCOUNT_JSON` to that path.

## 3) Twilio setup
- Create WhatsApp template: `tech_entry_request_he` (hebrew text you approved)
- Set webhook to your Render URL `/twilio/webhook`
- Verify the sender is WhatsApp-enabled

## 4) API ops
- Kick initial sends: `POST /ops/kick` with JSON `{"limit": 10}`
- Follow-up sweep: `POST /ops/followup_sweep`

## 5) Google Sheets
- File name: `Hertsliya-Hall-Tech-Bot-10-25` with tabs: `Events`, `TechContacts`, `Log`
- Share with your service account email as **Editor**

## Notes
- This MVP uses body text to open a template session. For production, consider Twilio Content API with `content_sid`.
- Contact card parsing depends on your Twilio payload; adjust in `/twilio/webhook` if needed.
