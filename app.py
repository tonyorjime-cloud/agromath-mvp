from __future__ import annotations

import os
import sqlite3
import secrets
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, Optional

import psycopg2
import psycopg2.extras

import os, random
import requests

def send_sms_termii(phone: str, message: str) -> bool:
    api_key = os.environ.get("TERMII_API_KEY")
    sender_id = os.environ.get("TERMII_SENDER_ID", "AgroMath")

    if not api_key:
        print("TERMII_API_KEY missing")
        return False

    # Termii generally expects international format, e.g. +2349066454125
    # If your app stores local numbers, you can normalize here:
    if phone.startswith("0"):
        phone = "+234" + phone[1:]
    elif phone.startswith("234"):
        phone = "+" + phone

    url = "https://api.ng.termii.com/api/sms/send"
    payload = {
        "to": phone,
        "from": sender_id,
        "sms": message,
        "type": "plain",
        "channel": "generic",
        "api_key": api_key
    }

    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code >= 400:
            print("Termii error:", r.status_code, r.text)
            return False
        return True
    except Exception as e:
        print("Termii exception:", e)
        return False

from flask import Flask, g, redirect, render_template, request, session, url_for, flash, jsonify

APP_TITLE = "AgroMath MVP"

# SQLite fallback (local/dev)
DB_NAME = os.environ.get("AGROMATH_DB", "agromath.db")

# Postgres (Render)
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
USE_POSTGRES = bool(DATABASE_URL)

# Admin (can approve farmer registrations)
ADMIN_PHONE = os.environ.get("AGROMATH_ADMIN_PHONE", "09066454125")

# OTP settings (demo: displayed on screen)
OTP_TTL_MINUTES = 10

app = Flask(__name__)
# IMPORTANT: On Render set a stable SECRET_KEY env var (do not rely on dev default)
app.secret_key = os.environ.get("SECRET_KEY") or "dev-CHANGE-ME"


# -----------------------------
# DB helpers + schema
# -----------------------------

def now_str() -> str:
    # Keep same format so your string comparisons still work
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _ph() -> str:
    """Return the placeholder token for the active DB driver."""
    return "%s" if USE_POSTGRES else "?"

def _ph_list(n: int) -> str:
    """Return comma-separated placeholders for IN (...) clauses."""
    if n <= 0:
        return ""
    return ",".join([_ph()] * n)

def connect_db():
    if USE_POSTGRES:
        # Render Postgres
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    # Local SQLite fallback
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def get_db():
    if "db" not in g:
        g.db = connect_db()
    return g.db

@app.teardown_appcontext
def close_db(exception: Exception | None):
    conn = g.pop("db", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

def db_fetchone(sql: str, params: tuple = ()):
    conn = get_db()
    if USE_POSTGRES:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    else:
        return conn.execute(sql, params).fetchone()

def db_fetchall(sql: str, params: tuple = ()):
    conn = get_db()
    if USE_POSTGRES:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    else:
        return conn.execute(sql, params).fetchall()

def db_execute(sql: str, params: tuple = ()):
    conn = get_db()
    if USE_POSTGRES:
        with conn.cursor() as cur:
            cur.execute(sql, params)
    else:
        conn.execute(sql, params)

def db_commit():
    get_db().commit()

def ensure_schema_sqlite() -> None:
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")

    def colnames(table: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {r["name"] for r in rows}

    # Core tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone TEXT UNIQUE NOT NULL,
            name TEXT,
            role TEXT, -- buyer|farmer|transporter
            hub TEXT,  -- optional: Makurdi, Jos, etc.
            is_active INTEGER NOT NULL DEFAULT 1,
            farmer_status TEXT NOT NULL DEFAULT 'NONE', -- NONE|PENDING|APPROVED|DECLINED
            created_at TEXT NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS otps(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone TEXT NOT NULL,
            otp TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS products(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            farmer_user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            unit TEXT NOT NULL DEFAULT 'unit',
            price INTEGER NOT NULL, -- NGN
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            FOREIGN KEY(farmer_user_id) REFERENCES users(id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders(
            id TEXT PRIMARY KEY,
            buyer_user_id INTEGER NOT NULL,
            origin TEXT NOT NULL,
            dest TEXT NOT NULL,
            status TEXT NOT NULL, -- NEEDS_QUOTES|QUOTE_ACCEPTED|DELIVERED|CANCELLED
            accepted_quote_id INTEGER,
            created_at TEXT NOT NULL,
            FOREIGN KEY(buyer_user_id) REFERENCES users(id),
            FOREIGN KEY(accepted_quote_id) REFERENCES quotes(id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_items(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            product_id INTEGER NOT NULL,
            qty INTEGER NOT NULL,
            unit_price INTEGER NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id),
            FOREIGN KEY(product_id) REFERENCES products(id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS quotes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            transporter_user_id INTEGER NOT NULL,
            price INTEGER NOT NULL,
            eta_hours INTEGER NOT NULL DEFAULT 24,
            status TEXT NOT NULL, -- SUBMITTED|DECLINED|ACCEPTED|DELIVERED
            created_at TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id),
            FOREIGN KEY(transporter_user_id) REFERENCES users(id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS notifications(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            message TEXT NOT NULL,
            link TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    # Lightweight migrations for older dbs
    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "users" in tables:
        cols = colnames("users")
        if "created_at" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN created_at TEXT")
            conn.execute("UPDATE users SET created_at = COALESCE(created_at, ?)", (now_str(),))
        if "farmer_status" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN farmer_status TEXT NOT NULL DEFAULT 'NONE'")
    if "orders" in tables:
        cols = colnames("orders")
        if "accepted_quote_id" not in cols:
            conn.execute("ALTER TABLE orders ADD COLUMN accepted_quote_id INTEGER")

    conn.commit()
    conn.close()

def ensure_schema_postgres() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            # In Postgres, use SERIAL for auto-increment IDs; keep timestamps as TEXT for minimal code changes.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users(
                    id SERIAL PRIMARY KEY,
                    phone TEXT UNIQUE NOT NULL,
                    name TEXT,
                    role TEXT,
                    hub TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    farmer_status TEXT NOT NULL DEFAULT 'NONE',
                    created_at TEXT NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS otps(
                    id SERIAL PRIMARY KEY,
                    phone TEXT NOT NULL,
                    otp TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS products(
                    id SERIAL PRIMARY KEY,
                    farmer_user_id INTEGER NOT NULL REFERENCES users(id),
                    name TEXT NOT NULL,
                    unit TEXT NOT NULL DEFAULT 'unit',
                    price INTEGER NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS quotes(
                    id SERIAL PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    transporter_user_id INTEGER NOT NULL REFERENCES users(id),
                    price INTEGER NOT NULL,
                    eta_hours INTEGER NOT NULL DEFAULT 24,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders(
                    id TEXT PRIMARY KEY,
                    buyer_user_id INTEGER NOT NULL REFERENCES users(id),
                    origin TEXT NOT NULL,
                    dest TEXT NOT NULL,
                    status TEXT NOT NULL,
                    accepted_quote_id INTEGER REFERENCES quotes(id),
                    created_at TEXT NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS order_items(
                    id SERIAL PRIMARY KEY,
                    order_id TEXT NOT NULL REFERENCES orders(id),
                    product_id INTEGER NOT NULL REFERENCES products(id),
                    qty INTEGER NOT NULL,
                    unit_price INTEGER NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS notifications(
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    kind TEXT NOT NULL,
                    message TEXT NOT NULL,
                    link TEXT,
                    created_at TEXT NOT NULL
                );
            """)

        conn.commit()
    finally:
        conn.close()

def ensure_schema() -> None:
    if USE_POSTGRES:
        ensure_schema_postgres()
    else:
        ensure_schema_sqlite()

ensure_schema()


# -----------------------------
# Auth helpers
# -----------------------------

def current_user():
    uid = session.get("uid")
    if not uid:
        return None
    return db_fetchone(f"SELECT * FROM users WHERE id = {_ph()}", (uid,))

def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("uid"):
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper

def role_required(*roles):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            u = current_user()
            if not u:
                return redirect(url_for("login"))
            if u["role"] not in roles:
                flash("Access denied for your role.", "error")
                return redirect(url_for("profile"))
            # Farmer gating
            if u["role"] == "farmer" and u["farmer_status"] != "APPROVED":
                flash("Farmer account pending approval. You can browse, but cannot list products yet.", "warn")
                return redirect(url_for("farmer_pending"))
            return fn(*args, **kwargs)
        return wrapper
    return deco


# -----------------------------
# Notifications (Phase 1: polling + sound)
# -----------------------------

def notify_user(user_id: int, kind: str, message: str, link: str = "/orders") -> None:
    """Create an in-app notification for a specific user."""
    if not user_id:
        return
    db_execute(
        f"INSERT INTO notifications(user_id, kind, message, link, created_at) VALUES({_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()})",
        (user_id, kind, message, link, now_str()),
    )


def notify_role(role: str, kind: str, message: str, link: str = "/orders") -> None:
    """Notify all active users in a role (used for transporters on new orders)."""
    rows = db_fetchall(
        f"SELECT id FROM users WHERE role = {_ph()} AND is_active = 1",
        (role,),
    )
    for r in rows:
        notify_user(int(r["id"]), kind, message, link)


# -----------------------------
# UI helpers
# -----------------------------

def money(n: Any) -> str:
    try:
        n = int(n)
    except Exception:
        return "₦0"
    return "₦{:,.0f}".format(n)

app.jinja_env.globals["money"] = money


# -----------------------------
# Routes: auth
# -----------------------------

@app.get("/")
def index():
    if session.get("uid"):
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))

@app.get("/login")
def login():
    return render_template("login.html")

@app.post("/login")
def login_post():
    phone = (request.form.get("phone") or "").strip()
    if not phone:
        flash("Phone is required.", "error")
        return redirect(url_for("login"))

    u = db_fetchone(f"SELECT * FROM users WHERE phone = {_ph()}", (phone,))
    if not u:
        db_execute(
            f"INSERT INTO users(phone, created_at, farmer_status) VALUES({_ph()}, {_ph()}, 'NONE')",
            (phone, now_str()),
        )
        db_commit()

    otp = str(secrets.randbelow(900000) + 100000)  # 6-digit
    expires = (datetime.now() + timedelta(minutes=OTP_TTL_MINUTES)).strftime("%Y-%m-%d %H:%M:%S")
    db_execute(
        f"INSERT INTO otps(phone, otp, expires_at, created_at) VALUES({_ph()}, {_ph()}, {_ph()}, {_ph()})",
        (phone, otp, expires, now_str()),
    )
    db_commit()

    session["pending_phone"] = phone
    return render_template("login.html", demo_otp=otp, phone=phone)

@app.post("/verify")
def verify():
    phone = (session.get("pending_phone") or "").strip()
    otp = (request.form.get("otp") or "").strip()

    if not phone:
        flash("Please request an OTP first.", "error")
        return redirect(url_for("login"))

    row = db_fetchone(
        f"SELECT * FROM otps WHERE phone = {_ph()} ORDER BY id DESC LIMIT 1",
        (phone,),
    )

    if not row:
        flash("No OTP found. Please request again.", "error")
        return redirect(url_for("login"))

    if otp != row["otp"]:
        flash("Invalid OTP.", "error")
        return render_template("login.html", demo_otp=row["otp"], phone=phone)

    if row["expires_at"] < now_str():
        flash("OTP expired. Please request a new one.", "error")
        return redirect(url_for("login"))

    u = db_fetchone(f"SELECT * FROM users WHERE phone = {_ph()}", (phone,))
    if not u:
        flash("User record missing. Please retry.", "error")
        return redirect(url_for("login"))

    session.pop("pending_phone", None)
    session["uid"] = u["id"]
    return redirect(url_for("profile"))

@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.get("/api/notifications")
@login_required
def api_notifications():
    """Return new notifications for the logged-in user.

    The client passes `since=<last_seen_id>`; we respond with notifications where id > since.
    This is deliberately simple (Phase 1) and works without websockets/push.
    """
    u = current_user()
    since_raw = request.args.get("since", "0")
    try:
        since = int(since_raw)
    except Exception:
        since = 0

    rows = db_fetchall(
        f"SELECT id, kind, message, link, created_at FROM notifications WHERE user_id = {_ph()} AND id > {_ph()} ORDER BY id ASC LIMIT 25",
        (u["id"], since),
    )
    items = [
        {
            "id": int(r["id"]),
            "kind": r["kind"],
            "message": r["message"],
            "link": r.get("link") or "",
            "created_at": r["created_at"],
        }
        for r in rows
    ]

    latest_id = items[-1]["id"] if items else since
    return jsonify({"latest_id": latest_id, "items": items})


# -----------------------------
# Routes: profile + onboarding
# -----------------------------

@app.get("/profile")
@login_required
def profile():
    u = current_user()
    return render_template("profile.html", user=u, admin_phone=ADMIN_PHONE)

@app.post("/profile")
@login_required
def profile_post():
    name = (request.form.get("name") or "").strip()
    role = (request.form.get("role") or "").strip()
    hub = (request.form.get("hub") or "").strip()

    if role not in ("buyer", "farmer", "transporter"):
        flash("Please select a role.", "error")
        return redirect(url_for("profile"))

    u = current_user()
    if not u:
        return redirect(url_for("login"))

    farmer_status = u["farmer_status"]
    if role == "farmer":
        if u["phone"] == ADMIN_PHONE:
            farmer_status = "APPROVED"
        else:
            farmer_status = "PENDING" if u["farmer_status"] in ("NONE", "DECLINED") else u["farmer_status"]
    else:
        farmer_status = "NONE"

    db_execute(
        f"UPDATE users SET name={_ph()}, role={_ph()}, hub={_ph()}, farmer_status={_ph()} WHERE id={_ph()}",
        (name, role, hub, farmer_status, u["id"]),
    )
    db_commit()

    if role == "farmer" and farmer_status != "APPROVED":
        return redirect(url_for("farmer_pending"))

    return redirect(url_for("dashboard"))

@app.get("/farmer/pending")
@login_required
def farmer_pending():
    u = current_user()
    return render_template("farmer_pending.html", user=u, admin_phone=ADMIN_PHONE)


# -----------------------------
# Admin approvals
# -----------------------------

@app.get("/admin")
@login_required
def admin():
    u = current_user()
    if not u or u["phone"] != ADMIN_PHONE:
        flash("Admin access only.", "error")
        return redirect(url_for("dashboard"))

    pending = db_fetchall(
        "SELECT * FROM users WHERE role='farmer' AND farmer_status='PENDING' ORDER BY created_at DESC"
    )
    return render_template("admin.html", user=u, pending=pending)

@app.post("/admin/farmer/approve")
@login_required
def admin_farmer_approve():
    u = current_user()
    if not u or u["phone"] != ADMIN_PHONE:
        flash("Admin access only.", "error")
        return redirect(url_for("dashboard"))

    user_id = request.form.get("user_id")
    db_execute(f"UPDATE users SET farmer_status='APPROVED' WHERE id={_ph()}", (user_id,))
    db_commit()
    flash("Farmer approved.", "ok")
    return redirect(url_for("admin"))

@app.post("/admin/farmer/decline")
@login_required
def admin_farmer_decline():
    u = current_user()
    if not u or u["phone"] != ADMIN_PHONE:
        flash("Admin access only.", "error")
        return redirect(url_for("dashboard"))

    user_id = request.form.get("user_id")
    db_execute(f"UPDATE users SET farmer_status='DECLINED' WHERE id={_ph()}", (user_id,))
    db_commit()
    flash("Farmer declined.", "ok")
    return redirect(url_for("admin"))


# -----------------------------
# Dashboards
# -----------------------------

@app.get("/dashboard")
@login_required
def dashboard():
    u = current_user()
    if not u or not u["role"]:
        return redirect(url_for("profile"))

    if u["role"] == "buyer":
        return redirect(url_for("buyer_dashboard"))
    if u["role"] == "farmer":
        if u["farmer_status"] != "APPROVED":
            return redirect(url_for("farmer_pending"))
        return redirect(url_for("farmer_dashboard"))
    if u["role"] == "transporter":
        return redirect(url_for("transporter_dashboard"))

    return redirect(url_for("profile"))


# -----------------------------
# Buyer flows
# -----------------------------

def get_cart() -> Dict[str, int]:
    cart = session.get("cart") or {}
    if not isinstance(cart, dict):
        cart = {}
    out = {}
    for k, v in cart.items():
        try:
            out[str(k)] = int(v)
        except Exception:
            continue
    return out

def set_cart(cart: Dict[str, int]) -> None:
    session["cart"] = cart

@app.get("/buyer")
@login_required
@role_required("buyer")
def buyer_dashboard():
    products = db_fetchall("""
        SELECT p.*, u.name AS farmer_name, u.hub AS farmer_hub
        FROM products p
        JOIN users u ON u.id = p.farmer_user_id
        WHERE p.is_active=1 AND u.role='farmer' AND u.farmer_status='APPROVED'
        ORDER BY p.created_at DESC
    """)
    cart = get_cart()
    cart_count = sum(cart.values())
    return render_template(
        "buyer_dashboard.html",
        user=current_user(),
        products=products,
        cart=cart,
        cart_count=cart_count,
    )

@app.post("/cart/add")
@login_required
@role_required("buyer")
def cart_add():
    pid = (request.form.get("product_id") or "").strip()
    qty = int((request.form.get("qty") or "1").strip() or "1")
    if qty < 1:
        qty = 1
    cart = get_cart()
    cart[pid] = cart.get(pid, 0) + qty
    set_cart(cart)
    flash("Added to cart.", "ok")
    return redirect(url_for("buyer_dashboard"))

@app.post("/cart/remove")
@login_required
@role_required("buyer")
def cart_remove():
    pid = (request.form.get("product_id") or "").strip()
    cart = get_cart()
    cart.pop(pid, None)
    set_cart(cart)
    flash("Removed.", "ok")
    return redirect(url_for("checkout"))

@app.get("/checkout")
@login_required
@role_required("buyer")
def checkout():
    cart = get_cart()
    items = []
    subtotal = 0
    if cart:
        marks = _ph_list(len(cart))
        rows = db_fetchall(
            f"""
            SELECT p.*, u.name AS farmer_name, u.hub AS farmer_hub
            FROM products p
            JOIN users u ON u.id=p.farmer_user_id
            WHERE p.id IN ({marks})
            """,
            tuple(cart.keys()),
        )
        by_id = {str(r["id"]): r for r in rows}
        for pid, qty in cart.items():
            pr = by_id.get(str(pid))
            if not pr:
                continue
            line = int(pr["price"]) * int(qty)
            subtotal += line
            items.append({"product": pr, "qty": qty, "line_total": line})

    return render_template("checkout.html", user=current_user(), items=items, subtotal=subtotal)

def make_order_id() -> str:
    return "ORD-" + secrets.token_hex(4).upper()

@app.post("/order/place")
@login_required
@role_required("buyer")
def order_place():
    origin = (request.form.get("origin") or "").strip() or "Unknown"
    dest = (request.form.get("dest") or "").strip() or "Unknown"
    cart = get_cart()
    if not cart:
        flash("Cart is empty.", "error")
        return redirect(url_for("buyer_dashboard"))

    u = current_user()
    assert u is not None

    marks = _ph_list(len(cart))
    rows = db_fetchall(
        f"SELECT * FROM products WHERE id IN ({marks}) AND is_active=1",
        tuple(cart.keys()),
    )
    by_id = {str(r["id"]): r for r in rows}
    if not by_id:
        flash("No valid items in cart.", "error")
        return redirect(url_for("buyer_dashboard"))

    oid = make_order_id()
    db_execute(
        f"""
        INSERT INTO orders(id, buyer_user_id, origin, dest, status, created_at)
        VALUES({_ph()}, {_ph()}, {_ph()}, {_ph()}, 'NEEDS_QUOTES', {_ph()})
        """,
        (oid, u["id"], origin, dest, now_str()),
    )

    for pid, qty in cart.items():
        pr = by_id.get(str(pid))
        if not pr:
            continue
        db_execute(
            f"""
            INSERT INTO order_items(order_id, product_id, qty, unit_price)
            VALUES({_ph()}, {_ph()}, {_ph()}, {_ph()})
            """,
            (oid, int(pid), int(qty), int(pr["price"])),
        )

    db_commit()

    # Phase-1 in-app notifications
    # Notify all active transporters that a new order needs quotes
    for tr in db_fetchall("SELECT id FROM users WHERE role='transporter' AND is_active=1"):
        notify_user(tr["id"], "NEW_ORDER", f"New order {oid} needs transport quotes.")

    # Notify any farmers whose products are in the order
    farmer_ids = sorted({by_id.get(str(pid), {}).get("farmer_user_id") for pid in cart.keys() if by_id.get(str(pid))})
    for fid in [x for x in farmer_ids if x]:
        notify_user(int(fid), "NEW_ORDER", f"New order {oid} includes your products.")

    set_cart({})
    flash(f"Order {oid} placed. Waiting for transporter quotes.", "ok")
    return redirect(url_for("orders"))

@app.get("/orders")
@login_required
def orders():
    u = current_user()
    if not u:
        return redirect(url_for("login"))

    if u["role"] == "buyer":
        orders_list = db_fetchall(
            f"SELECT * FROM orders WHERE buyer_user_id={_ph()} ORDER BY created_at DESC",
            (u["id"],),
        )
    elif u["role"] == "farmer":
        orders_list = db_fetchall(f"""
            SELECT DISTINCT o.*
            FROM orders o
            JOIN order_items oi ON oi.order_id=o.id
            JOIN products p ON p.id=oi.product_id
            WHERE p.farmer_user_id={_ph()}
            ORDER BY o.created_at DESC
        """, (u["id"],))
    elif u["role"] == "transporter":
        orders_list = db_fetchall("""
            SELECT * FROM orders
            WHERE status IN ('NEEDS_QUOTES','QUOTE_ACCEPTED')
            ORDER BY created_at DESC
        """)
    else:
        orders_list = []

    out = []
    for o in orders_list:
        items = db_fetchall(f"""
            SELECT oi.*, p.name AS product_name, p.unit AS product_unit, u.name AS farmer_name
            FROM order_items oi
            JOIN products p ON p.id=oi.product_id
            JOIN users u ON u.id=p.farmer_user_id
            WHERE oi.order_id={_ph()}
        """, (o["id"],))

        quotes = db_fetchall(f"""
            SELECT q.*, u.name AS transporter_name, u.phone AS transporter_phone
            FROM quotes q
            JOIN users u ON u.id=q.transporter_user_id
            WHERE q.order_id={_ph()}
            ORDER BY q.created_at DESC
        """, (o["id"],))

        # NOTE: avoid using key name "items" because Jinja treats `pack.items` as dict.items() (a method).
        out.append({"order": o, "line_items": items, "quotes": quotes})

    return render_template("orders.html", user=u, orders=out)

@app.post("/quote/accept")
@login_required
@role_required("buyer")
def quote_accept():
    quote_id = request.form.get("quote_id")
    order_id = request.form.get("order_id")

    u = current_user()
    assert u is not None

    o = db_fetchone(
        f"SELECT * FROM orders WHERE id={_ph()} AND buyer_user_id={_ph()}",
        (order_id, u["id"]),
    )
    if not o:
        flash("Order not found.", "error")
        return redirect(url_for("orders"))

    q = db_fetchone(
        f"SELECT * FROM quotes WHERE id={_ph()} AND order_id={_ph()}",
        (quote_id, order_id),
    )
    if not q or q["status"] in ("DECLINED",):
        flash("Quote not found / already declined.", "error")
        return redirect(url_for("orders"))

    db_execute(f"UPDATE quotes SET status='ACCEPTED' WHERE id={_ph()}", (quote_id,))
    db_execute(
        f"UPDATE quotes SET status='DECLINED' WHERE order_id={_ph()} AND id<>{_ph()} AND status<>'DELIVERED'",
        (order_id, quote_id),
    )
    db_execute(
        f"UPDATE orders SET status='QUOTE_ACCEPTED', accepted_quote_id={_ph()} WHERE id={_ph()}",
        (quote_id, order_id),
    )
    db_commit()

    # Notifications
    notify_user(int(q["transporter_user_id"]), "QUOTE_ACCEPTED", f"Your quote was accepted for order {order_id}.")

    declined = db_fetchall(
        f"SELECT transporter_user_id FROM quotes WHERE order_id={_ph()} AND id<>{_ph()}",
        (order_id, quote_id),
    )
    for d in declined:
        notify_user(int(d["transporter_user_id"]), "QUOTE_DECLINED", f"Another quote was accepted for order {order_id}.")

    flash("Quote accepted. Transporter can now deliver.", "ok")
    return redirect(url_for("orders"))

@app.post("/quote/decline")
@login_required
@role_required("buyer")
def quote_decline():
    quote_id = request.form.get("quote_id")
    order_id = request.form.get("order_id")

    u = current_user()
    assert u is not None

    o = db_fetchone(
        f"SELECT * FROM orders WHERE id={_ph()} AND buyer_user_id={_ph()}",
        (order_id, u["id"]),
    )
    if not o:
        flash("Order not found.", "error")
        return redirect(url_for("orders"))

    db_execute(
        f"UPDATE quotes SET status='DECLINED' WHERE id={_ph()} AND order_id={_ph()} AND status='SUBMITTED'",
        (quote_id, order_id),
    )
    db_commit()

    q = db_fetchone(f"SELECT transporter_user_id FROM quotes WHERE id={_ph()}", (quote_id,))
    if q:
        notify_user(int(q["transporter_user_id"]), "QUOTE_DECLINED", f"Buyer declined your quote for order {order_id}.")
    flash("Quote declined.", "ok")
    return redirect(url_for("orders"))


# -----------------------------
# Farmer flows
# -----------------------------

@app.get("/farmer")
@login_required
@role_required("farmer")
def farmer_dashboard():
    u = current_user()
    assert u is not None

    products = db_fetchall(
        f"SELECT * FROM products WHERE farmer_user_id={_ph()} ORDER BY created_at DESC",
        (u["id"],),
    )

    my_orders = db_fetchall(f"""
        SELECT DISTINCT o.*
        FROM orders o
        JOIN order_items oi ON oi.order_id=o.id
        JOIN products p ON p.id=oi.product_id
        WHERE p.farmer_user_id={_ph()}
        ORDER BY o.created_at DESC
    """, (u["id"],))

    return render_template("farmer_dashboard.html", user=u, products=products, orders=my_orders)

@app.post("/farmer/product/add")
@login_required
@role_required("farmer")
def farmer_product_add():
    name = (request.form.get("name") or "").strip()
    unit = (request.form.get("unit") or "unit").strip()
    price = int((request.form.get("price") or "0").strip() or "0")

    if not name or price <= 0:
        flash("Product name and positive price required.", "error")
        return redirect(url_for("farmer_dashboard"))

    u = current_user()
    assert u is not None

    db_execute(
        f"INSERT INTO products(farmer_user_id, name, unit, price, created_at) VALUES({_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()})",
        (u["id"], name, unit, price, now_str()),
    )
    db_commit()
    flash("Product added.", "ok")
    return redirect(url_for("farmer_dashboard"))

@app.post("/farmer/product/toggle")
@login_required
@role_required("farmer")
def farmer_product_toggle():
    pid = request.form.get("product_id")
    u = current_user()
    assert u is not None

    row = db_fetchone(
        f"SELECT * FROM products WHERE id={_ph()} AND farmer_user_id={_ph()}",
        (pid, u["id"]),
    )
    if not row:
        flash("Product not found.", "error")
        return redirect(url_for("farmer_dashboard"))

    new_state = 0 if row["is_active"] == 1 else 1
    db_execute(f"UPDATE products SET is_active={_ph()} WHERE id={_ph()}", (new_state, pid))
    db_commit()
    flash("Updated.", "ok")
    return redirect(url_for("farmer_dashboard"))


# -----------------------------
# Transporter flows
# -----------------------------

@app.get("/transport")
@login_required
@role_required("transporter")
def transporter_dashboard():
    u = current_user()
    assert u is not None

    open_orders = db_fetchall("""
        SELECT o.*,
               (SELECT COUNT(*) FROM quotes q WHERE q.order_id=o.id) AS quote_count
        FROM orders o
        WHERE o.status='NEEDS_QUOTES'
        ORDER BY o.created_at DESC
    """)

    my_quotes = db_fetchall(f"""
        SELECT q.*, o.origin, o.dest, o.status AS order_status
        FROM quotes q
        JOIN orders o ON o.id=q.order_id
        WHERE q.transporter_user_id={_ph()}
        ORDER BY q.created_at DESC
    """, (u["id"],))

    accepted_orders = db_fetchall(f"""
        SELECT o.*, q.price, q.eta_hours, q.id AS quote_id
        FROM orders o
        JOIN quotes q ON q.id=o.accepted_quote_id
        WHERE q.transporter_user_id={_ph()} AND o.status='QUOTE_ACCEPTED'
        ORDER BY o.created_at DESC
    """, (u["id"],))

    return render_template(
        "transporter_dashboard.html",
        user=u,
        open_orders=open_orders,
        my_quotes=my_quotes,
        accepted_orders=accepted_orders,
    )

@app.post("/transport/quote")
@login_required
@role_required("transporter")
def transport_quote():
    order_id = request.form.get("order_id")
    price = int((request.form.get("price") or "0").strip() or "0")
    eta_hours = int((request.form.get("eta_hours") or "24").strip() or "24")

    if price <= 0:
        flash("Quote price must be positive.", "error")
        return redirect(url_for("transporter_dashboard"))

    u = current_user()
    assert u is not None

    o = db_fetchone(f"SELECT * FROM orders WHERE id={_ph()}", (order_id,))
    if not o or o["status"] != "NEEDS_QUOTES":
        flash("Order not available for quoting.", "error")
        return redirect(url_for("transporter_dashboard"))

    db_execute(
        f"""
        INSERT INTO quotes(order_id, transporter_user_id, price, eta_hours, status, created_at)
        VALUES({_ph()}, {_ph()}, {_ph()}, {_ph()}, 'SUBMITTED', {_ph()})
        """,
        (order_id, u["id"], price, eta_hours, now_str()),
    )
    db_commit()

    # Notify buyer: quote arrived
    buyer = db_fetchone(f"SELECT buyer_user_id FROM orders WHERE id={_ph()}", (order_id,))
    if buyer:
        notify_user(int(buyer["buyer_user_id"]), "QUOTE", f"New transport quote received for order {order_id}.")

    flash("Quote submitted.", "ok")
    return redirect(url_for("transporter_dashboard"))

@app.post("/transport/deliver")
@login_required
@role_required("transporter")
def transport_deliver():
    order_id = request.form.get("order_id")
    u = current_user()
    if not u:
        return redirect(url_for("login"))

    o = db_fetchone(
        f"SELECT * FROM orders WHERE id={_ph()} AND status='QUOTE_ACCEPTED'",
        (order_id,),
    )
    if not o:
        flash("Order not found / not ready for delivery.", "error")
        return redirect(url_for("transporter_dashboard"))

    q = db_fetchone(
        f"SELECT * FROM quotes WHERE id={_ph()} AND transporter_user_id={_ph()}",
        (o["accepted_quote_id"], u["id"]),
    )
    if not q or q["status"] != "ACCEPTED":
        flash("You are not assigned to this order.", "error")
        return redirect(url_for("transporter_dashboard"))

    db_execute(f"UPDATE quotes SET status='DELIVERED' WHERE id={_ph()}", (q["id"],))
    db_execute(f"UPDATE orders SET status='DELIVERED' WHERE id={_ph()}", (order_id,))
    db_commit()

    # Notifications
    buyer = db_fetchone(f"SELECT buyer_user_id FROM orders WHERE id={_ph()}", (order_id,))
    if buyer:
        notify_user(int(buyer["buyer_user_id"]), "DELIVERED", f"Order {order_id} marked delivered.")

    for fr in db_fetchall(
        f"SELECT DISTINCT p.farmer_user_id AS uid FROM order_items oi JOIN products p ON p.id=oi.product_id WHERE oi.order_id={_ph()}",
        (order_id,),
    ):
        notify_user(int(fr["uid"]), "DELIVERED", f"Order {order_id} has been delivered.")

    flash("Order marked delivered.", "ok")
    return redirect(url_for("transporter_dashboard"))



# --- OneSignal Web Push service worker endpoints (must be served from site root) ---
# Place these files inside /static:
#   static/OneSignalSDKWorker.js
#   static/OneSignalSDKUpdaterWorker.js
# OneSignal requires them to be accessible at:
#   https://YOUR_DOMAIN/OneSignalSDKWorker.js
#   https://YOUR_DOMAIN/OneSignalSDKUpdaterWorker.js
@app.get("/OneSignalSDKWorker.js")
def onesignal_sdk_worker():
    return send_from_directory("static", "OneSignalSDKWorker.js")

@app.get("/OneSignalSDKUpdaterWorker.js")
def onesignal_sdk_updater_worker():
    return send_from_directory("static", "OneSignalSDKUpdaterWorker.js")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=os.getenv("FLASK_DEBUG","0")=="1")
