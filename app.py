# app.py (FIXED – SAFE VERSION)
# Replace your entire app.py with this file.

from flask import Flask, render_template, request, redirect, url_for, session, flash
from datetime import datetime, timedelta
import psycopg2, psycopg2.extras
import os, secrets, requests

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")

DATABASE_URL = os.environ.get("DATABASE_URL")
OTP_TTL_MINUTES = 10

# -------------------- DB HELPERS --------------------

def db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def qmark():
    return "%s"

def fetchone(q, p=()):
    with db() as c:
        with c.cursor() as cur:
            cur.execute(q, p)
            return cur.fetchone()

def fetchall(q, p=()):
    with db() as c:
        with c.cursor() as cur:
            cur.execute(q, p)
            return cur.fetchall()

def execute(q, p=()):
    with db() as c:
        with c.cursor() as cur:
            cur.execute(q, p)

# -------------------- SMS / OTP --------------------

def termii_enabled():
    return bool(os.environ.get("TERMII_API_KEY"))

def send_sms(phone, text):
    if not termii_enabled():
        return False, "disabled"
    try:
        r = requests.post(
            "https://api.ng.termii.com/api/sms/send",
            json={
                "to": phone,
                "from": "AgroMath",
                "sms": text,
                "type": "plain",
                "channel": "generic",
                "api_key": os.environ["TERMII_API_KEY"],
            },
            timeout=10,
        )
        return r.status_code == 200, r.text
    except Exception as e:
        return False, str(e)

# -------------------- AUTH --------------------

@app.get("/login")
def login():
    return render_template("login.html", sms_enabled=termii_enabled())

@app.post("/login")
def login_post():
    phone = request.form.get("phone", "").strip()
    if not phone:
        flash("Phone number required", "error")
        return redirect(url_for("login"))

    u = fetchone(f"SELECT * FROM users WHERE phone={qmark()}", (phone,))
    if not u:
        execute(
            f"INSERT INTO users(phone, created_at, farmer_status) VALUES({qmark()}, {qmark()}, 'NONE')",
            (phone, now_str()),
        )

    otp = f"{secrets.randbelow(1_000_000):06d}"
    expires = (datetime.now() + timedelta(minutes=OTP_TTL_MINUTES)).strftime("%Y-%m-%d %H:%M:%S")

    execute(f"DELETE FROM otps WHERE phone={qmark()}", (phone,))
    execute(
        f"INSERT INTO otps(phone, otp, expires_at, created_at) VALUES({qmark()}, {qmark()}, {qmark()}, {qmark()})",
        (phone, otp, expires, now_str()),
    )

    session["pending_phone"] = phone

    if termii_enabled():
        ok, _ = send_sms(phone, f"Your AgroMath OTP is {otp}. Valid for 10 minutes.")
        if ok:
            flash("OTP sent via SMS", "ok")
            return render_template("login.html", phone=phone, sms_enabled=True)

    return render_template("login.html", phone=phone, demo_otp=otp, sms_enabled=False)

@app.post("/verify")
def verify():
    phone = session.get("pending_phone")
    otp = request.form.get("otp", "").strip()

    if not phone or not otp:
        flash("Session expired", "error")
        return redirect(url_for("login"))

    row = fetchone(
        f"SELECT * FROM otps WHERE phone={qmark()} ORDER BY created_at DESC LIMIT 1",
        (phone,),
    )

    if not row or row["otp"] != otp or row["expires_at"] < now_str():
        flash("Invalid or expired OTP", "error")
        return redirect(url_for("login"))

    u = fetchone(f"SELECT * FROM users WHERE phone={qmark()}", (phone,))

    session.clear()
    session["uid"] = u["id"]

    execute(f"DELETE FROM otps WHERE phone={qmark()}", (phone,))

    return redirect(url_for("profile"))

@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# -------------------- BASIC VIEWS --------------------

@app.get("/")
def home():
    if session.get("uid"):
        return redirect(url_for("buyer"))
    return redirect(url_for("login"))

@app.get("/profile")
def profile():
    u = fetchone(f"SELECT * FROM users WHERE id={qmark()}", (session["uid"],))
    return render_template("profile.html", user=u)

@app.get("/buyer")
def buyer():
    products = fetchall("SELECT * FROM products")
    return render_template("buyer.html", products=products)

# -------------------- RUN --------------------

if __name__ == "__main__":
    app.run(debug=True)
