# AgroMath MVP (Flask + SQLite)

## Run locally
```bash
py -m pip install -r requirements.txt
py app.py
```
Open http://127.0.0.1:5000

## Notes
- OTP is demo-only (shown on screen).
- Admin phone (can approve farmers): 09066454125 (change via env `AGROMATH_ADMIN_PHONE`)

## Push notifications (OneSignal)
This app supports:
- **In-app notifications** (polling `/api/notifications`) with toast + sound while the user is online.
- **Web Push notifications** via **OneSignal** for background/offline delivery.

### Required environment variables (Render/production)
- `ONESIGNAL_APP_ID` (public) — required for the browser SDK (defaults to the app id already in the templates)
- `ONESIGNAL_API_KEY` (private REST API key) — required for the backend to send push

Optional:
- `ONESIGNAL_API_URL` (defaults to `https://api.onesignal.com/notifications?c=push`)

### OneSignal targeting model
The browser SDK logs a user into OneSignal using:
- **External ID = app user_id**

The backend then targets that user with:
- `include_aliases: { external_id: ["<user_id>"] }`.
