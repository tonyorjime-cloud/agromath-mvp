from __future__ import annotations

import os
import logging
import sqlite3
import secrets
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, Optional

import psycopg2
import psycopg2.extras

from flask import Flask, g, redirect, render_template, request, session, url_for, flash, jsonify, abort

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

# Basic structured logging (works on Render)
logging.basicConfig(level=os.environ.get("LOG_LEVEL","INFO"))
logger = logging.getLogger("agromath")
# IMPORTANT: On Render set a stable SECRET_KEY env var (do not rely on dev default)
app.secret_key = os.environ.get("SECRET_KEY") or "dev-CHANGE-ME"


# -----------------------------
# DB helpers + schema
# -----------------------------

def now_str() -> str:
    # Keep same format so your string comparisons still work
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance (km)."""
    from math import radians, sin, cos, asin, sqrt
    r = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return r * c

def order_distance_km(order_id: str) -> Optional[float]:
    """Compute distance between latest origin and dropoff GPS points for an order."""
    o = db_fetchone(
        f"""
        SELECT lat, lng
        FROM order_locations
        WHERE order_id={_ph()} AND role='origin'
        ORDER BY id DESC
        LIMIT 1
        """,
        (order_id,),
    )
    d = db_fetchone(
        f"""
        SELECT lat, lng
        FROM order_locations
        WHERE order_id={_ph()} AND role='dropoff'
        ORDER BY id DESC
        LIMIT 1
        """,
        (order_id,),
    )
    if not o or not d:
        return None
    try:
        return float(haversine_km(float(o["lat"]), float(o["lng"]), float(d["lat"]), float(d["lng"])))
    except Exception:
        return None

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
            hub_lat REAL,
            hub_lng REAL,
            hub_accuracy REAL,
            is_active INTEGER NOT NULL DEFAULT 1,
            farmer_status TEXT NOT NULL DEFAULT 'NONE', -- NONE|PENDING|APPROVED|DECLINED
            created_at TEXT NOT NULL
        )
    """)
    # --- lightweight migrations (SQLite) ---
    # Add farmer pickup GPS columns if repo was started before these fields existed.
    for col, typ in (("hub_lat", "REAL"), ("hub_lng", "REAL"), ("hub_accuracy", "REAL")):
        try:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col} {typ}")
        except Exception:
            pass


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
            status TEXT NOT NULL, -- NEEDS_QUOTES|QUOTE_ACCEPTED|IN_TRANSIT|ARRIVED|DELIVERED|CANCELLED
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

    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_locations(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL, -- origin|dropoff|transporter
            lat REAL NOT NULL,
            lng REAL NOT NULL,
            accuracy REAL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            sender_user_id INTEGER NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id),
            FOREIGN KEY(sender_user_id) REFERENCES users(id)
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
            # --- lightweight migrations (Postgres) ---
            for ddl in (
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS hub_lat DOUBLE PRECISION",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS hub_lng DOUBLE PRECISION",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS hub_accuracy DOUBLE PRECISION",
            ):
                try:
                    cur.execute(ddl + ";")
                except Exception:
                    pass


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

            cur.execute("""
                CREATE TABLE IF NOT EXISTS order_locations(
                    id SERIAL PRIMARY KEY,
                    order_id TEXT NOT NULL REFERENCES orders(id),
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    role TEXT NOT NULL,
                    lat DOUBLE PRECISION NOT NULL,
                    lng DOUBLE PRECISION NOT NULL,
                    accuracy DOUBLE PRECISION,
                    created_at TEXT NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS order_messages(
                    id SERIAL PRIMARY KEY,
                    order_id TEXT NOT NULL REFERENCES orders(id),
                    sender_user_id INTEGER NOT NULL REFERENCES users(id),
                    message TEXT NOT NULL,
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
# Cross-DB row helpers
# -----------------------------

def _row_get(r, key: str, default=None):
    """Safe getter for sqlite3.Row and psycopg2 RealDictCursor rows."""
    if r is None:
        return default
    try:
        return r.get(key, default)  # Postgres dict row
    except Exception:
        try:
            return r[key]  # SQLite Row
        except Exception:
            return default

def _row_to_dict(r) -> Optional[dict]:
    if r is None:
        return None
    try:
        # psycopg2 RealDictCursor row is already a dict
        if isinstance(r, dict):
            return dict(r)
    except Exception:
        pass
    try:
        return {k: r[k] for k in r.keys()}
    except Exception:
        try:
            return dict(r)
        except Exception:
            return None


# -----------------------------
# Order participants (tracking + chat)
# -----------------------------

def accepted_transporter_id(order_id: str):
    o = db_fetchone(f"SELECT * FROM orders WHERE id={_ph()}", (order_id,))
    if not o:
        return None
    aqid = _row_get(o, "accepted_quote_id")
    if not aqid:
        return None
    q = db_fetchone(
        f"SELECT * FROM quotes WHERE id={_ph()} AND order_id={_ph()}",
        (aqid, order_id),
    )
    if not q:
        return None
    return int(q["transporter_user_id"])

def order_participant_ids(order_id: str) -> set[int]:
    """buyer + farmers in order + accepted transporter (if any)."""
    o = db_fetchone(f"SELECT * FROM orders WHERE id={_ph()}", (order_id,))
    if not o:
        return set()

    ids: set[int] = {int(o["buyer_user_id"])}

    # Farmers involved (via products in order)
    rows = db_fetchall(
        f"""
        SELECT DISTINCT p.farmer_user_id AS uid
        FROM order_items oi
        JOIN products p ON p.id=oi.product_id
        WHERE oi.order_id={_ph()}
        """,
        (order_id,),
    )
    for r in rows:
        uid = _row_get(r, "uid")
        if uid is not None:
            try:
                ids.add(int(uid))
            except Exception:
                pass

    atid = accepted_transporter_id(order_id)
    if atid:
        ids.add(int(atid))

    return ids



# -----------------------------
# Notifications (Phase 1: polling + sound)
# -----------------------------

def notify_user(user_id: int, kind: str, message: str, link: str = "/orders") -> None:
    """Create an in-app notification for a specific user (and persist immediately)."""
    if not user_id:
        return
    db_execute(
        f"INSERT INTO notifications(user_id, kind, message, link, created_at) VALUES({_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()})",
        (int(user_id), kind, message, link, now_str()),
    )
    # Commit here to avoid silent drops when callers forget to commit after notifying.
    db_commit()


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


@app.get("/terms")
def terms():
    return render_template("terms.html")

@app.get("/privacy")
def privacy():
    return render_template("privacy.html")


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
            "link": (_row_get(r, "link") or ""),
            "created_at": r["created_at"],
        }
        for r in rows
    ]

    latest_id = items[-1]["id"] if items else since
    return jsonify({"latest_id": latest_id, "items": items})


# -----------------------------
# Tracking (browser GPS)
# -----------------------------

@app.get("/track/<order_id>")
@login_required
def track_order(order_id: str):
    u = current_user()
    if not u:
        return redirect(url_for("login"))

    allowed = order_participant_ids(order_id)
    if not allowed or int(u["id"]) not in allowed:
        flash("You don't have access to track this order.", "error")
        return redirect(url_for("orders"))

    o = db_fetchone(f"SELECT * FROM orders WHERE id={_ph()}", (order_id,))
    status = _row_get(o, "status", "") if o else ""
    assigned = False
    if u["role"] == "transporter":
        atid = accepted_transporter_id(order_id)
        assigned = bool(atid and int(atid) == int(u["id"]))

    return render_template("track.html", user=u, order_id=order_id, status=status, assigned=assigned)

@app.get("/api/order/<order_id>/track")
@login_required
def api_order_track(order_id: str):
    u = current_user()
    if not u:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401

    allowed = order_participant_ids(order_id)
    if not allowed or int(u["id"]) not in allowed:
        return jsonify({"ok": False, "error": "forbidden"}), 403

    origin = db_fetchone(
        f"""
        SELECT lat, lng, accuracy, created_at
        FROM order_locations
        WHERE order_id={_ph()} AND role='origin'
        ORDER BY id DESC
        LIMIT 1
        """,
        (order_id,),
    )
    dropoff = db_fetchone(
        f"""
        SELECT lat, lng, accuracy, created_at
        FROM order_locations
        WHERE order_id={_ph()} AND role='dropoff'
        ORDER BY id DESC
        LIMIT 1
        """,
        (order_id,),
    )
    transporter = db_fetchone(
        f"""
        SELECT lat, lng, accuracy, created_at
        FROM order_locations
        WHERE order_id={_ph()} AND role='transporter'
        ORDER BY id DESC
        LIMIT 1
        """,
        (order_id,),
    )

    return jsonify({"ok": True, "origin": _row_to_dict(origin), "buyer": _row_to_dict(dropoff), "transporter": _row_to_dict(transporter)})

@app.post("/api/order/<order_id>/location")
@login_required
@role_required("transporter")
def api_order_location(order_id: str):
    u = current_user()
    assert u is not None

    atid = accepted_transporter_id(order_id)
    if not atid or int(u["id"]) != int(atid):
        return jsonify({"ok": False, "error": "not_accepted_transporter"}), 403

    j = request.get_json(silent=True) or {}
    try:
        lat = float(j.get("lat"))
        lng = float(j.get("lng"))
        acc = j.get("accuracy")
        acc = float(acc) if acc is not None else None
    except Exception:
        return jsonify({"ok": False, "error": "bad_payload"}), 400

    if not (-90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0):
        return jsonify({"ok": False, "error": "bad_coords"}), 400

    db_execute(
        f"""
        INSERT INTO order_locations(order_id, user_id, role, lat, lng, accuracy, created_at)
        VALUES({_ph()}, {_ph()}, 'transporter', {_ph()}, {_ph()}, {_ph()}, {_ph()})
        """,
        (order_id, int(u["id"]), lat, lng, acc, now_str()),
    )
    db_commit()
    logger.info("location_ping order_id=%s transporter_id=%s", order_id, u["id"])
    return jsonify({"ok": True})


# -----------------------------
# Chat (per order)
# -----------------------------

@app.get("/chat/<order_id>")
@login_required
def chat_order(order_id: str):
    u = current_user()
    if not u:
        return redirect(url_for("login"))

    allowed = order_participant_ids(order_id)
    if not allowed or int(u["id"]) not in allowed:
        flash("You don't have access to this chat.", "error")
        return redirect(url_for("orders"))

    # Optionally gate chat until a transporter is accepted
    if not accepted_transporter_id(order_id):
        flash("Chat becomes available after you accept a transporter quote.", "warn")
        return redirect(url_for("orders"))

    msgs = db_fetchall(
        f"""
        SELECT m.*, u.name AS sender_name, u.role AS sender_role
        FROM order_messages m
        JOIN users u ON u.id=m.sender_user_id
        WHERE m.order_id={_ph()}
        ORDER BY m.id ASC
        LIMIT 200
        """,
        (order_id,),
    )

    return render_template("chat.html", user=u, order_id=order_id, messages=msgs)

@app.get("/api/order/<order_id>/messages")
@login_required
def api_order_messages(order_id: str):
    u = current_user()
    if not u:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401

    allowed = order_participant_ids(order_id)
    if not allowed or int(u["id"]) not in allowed:
        return jsonify({"ok": False, "error": "forbidden"}), 403

    since_raw = request.args.get("since", "0")
    try:
        since_id = int(since_raw)
    except Exception:
        since_id = 0

    msgs = db_fetchall(
        f"""
        SELECT m.*, u.name AS sender_name, u.role AS sender_role
        FROM order_messages m
        JOIN users u ON u.id=m.sender_user_id
        WHERE m.order_id={_ph()} AND m.id > {_ph()}
        ORDER BY m.id ASC
        LIMIT 200
        """,
        (order_id, since_id),
    )
    return jsonify({"ok": True, "messages": [ _row_to_dict(m) for m in msgs ]})

@app.post("/api/order/<order_id>/messages")
@login_required
def api_order_send_message(order_id: str):
    u = current_user()
    if not u:
        return jsonify({"ok": False, "error": "not_logged_in"}), 401

    allowed = order_participant_ids(order_id)
    if not allowed or int(u["id"]) not in allowed:
        return jsonify({"ok": False, "error": "forbidden"}), 403

    if not accepted_transporter_id(order_id):
        return jsonify({"ok": False, "error": "chat_not_active"}), 400

    j = request.get_json(silent=True) or {}
    msg = (j.get("message") or "").strip()
    if not msg:
        return jsonify({"ok": False, "error": "empty"}), 400
    if len(msg) > 2000:
        msg = msg[:2000]

    db_execute(
        f"""
        INSERT INTO order_messages(order_id, sender_user_id, message, created_at)
        VALUES({_ph()}, {_ph()}, {_ph()}, {_ph()})
        """,
        (order_id, int(u["id"]), msg, now_str()),
    )
    db_commit()

    # Notify other participants (in-app)
    for uid in allowed:
        if int(uid) == int(u["id"]):
            continue
        notify_user(int(uid), "CHAT", f"New message on order {order_id}.", link=f"/chat/{order_id}")

    # Return latest id
    row = db_fetchone(
        f"SELECT id FROM order_messages WHERE order_id={_ph()} ORDER BY id DESC LIMIT 1",
        (order_id,),
    )
    new_id = int(row["id"]) if row else 0
    return jsonify({"ok": True, "id": new_id})



# -----------------------------
# Routes: profile + onboarding
# -----------------------------


@app.get("/help")
def help_page():
    # Public help page (no auth required)
    return render_template("help.html", user=current_user())


@app.get("/healthz")
def healthz():
    """
    Lightweight health endpoint for Render / uptime checks.
    Returns JSON only; no secrets.
    """
    try:
        one = db_fetchone(f"SELECT 1 AS ok")
        ok = bool(one and int(_row_get(one, "ok", 0)) == 1)

        stats = {
            "users": int(_row_get(db_fetchone("SELECT COUNT(*) AS c FROM users"), "c", 0)),
            "orders": int(_row_get(db_fetchone("SELECT COUNT(*) AS c FROM orders"), "c", 0)),
            "quotes": int(_row_get(db_fetchone("SELECT COUNT(*) AS c FROM quotes"), "c", 0)),
            "notifications": int(_row_get(db_fetchone("SELECT COUNT(*) AS c FROM notifications"), "c", 0)),
        }

        return jsonify({"ok": ok, "stats": stats, "db": "postgres" if DB_URL else "sqlite"})
    except Exception as e:
        logger.exception("healthz_failed")
        return jsonify({"ok": False, "error": str(e)}), 500




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
    hub_lat = (request.form.get("hub_lat") or "").strip()
    hub_lng = (request.form.get("hub_lng") or "").strip()
    hub_accuracy = (request.form.get("hub_accuracy") or "").strip()

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

    def _to_float(s: str) -> Optional[float]:
        try:
            return float(s)
        except Exception:
            return None

    h_lat = _to_float(hub_lat) if hub_lat else None
    h_lng = _to_float(hub_lng) if hub_lng else None
    h_acc = _to_float(hub_accuracy) if hub_accuracy else None

    db_execute(
        f"UPDATE users SET name={_ph()}, role={_ph()}, hub={_ph()}, hub_lat={_ph()}, hub_lng={_ph()}, hub_accuracy={_ph()}, farmer_status={_ph()} WHERE id={_ph()}",
        (name, role, hub, h_lat, h_lng, h_acc, farmer_status, u["id"]),
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
    # Search / filter parameters
    q = (request.args.get("q") or "").strip()
    sort = (request.args.get("sort") or "newest").strip().lower()
    try:
        radius_km = float((request.args.get("radius_km") or "").strip()) if request.args.get("radius_km") else None
    except Exception:
        radius_km = None

    # Buyer browsing location (session-scoped)
    buyer_lat = session.get("buyer_lat")
    buyer_lng = session.get("buyer_lng")
    has_buyer_loc = (buyer_lat is not None and buyer_lng is not None)

    base_sql = f"""
        SELECT p.*, u.name AS farmer_name, u.hub AS farmer_hub,
               u.hub_lat AS farmer_hub_lat, u.hub_lng AS farmer_hub_lng, u.hub_accuracy AS farmer_hub_accuracy
        FROM products p
        JOIN users u ON u.id = p.farmer_user_id
        WHERE p.is_active=1 AND u.role='farmer' AND u.farmer_status='APPROVED'
    """

    params: tuple = ()
    if q:
        # Search by product name (primary) and farmer name (secondary).
        if USE_POSTGRES:
            base_sql += f" AND (p.name ILIKE {_ph()} OR u.name ILIKE {_ph()})"
            like = f"%{q}%"
            params = (like, like)
        else:
            base_sql += f" AND (LOWER(p.name) LIKE {_ph()} OR LOWER(COALESCE(u.name,'')) LIKE {_ph()})"
            like = f"%{q.lower()}%"
            params = (like, like)

    # Default DB order (may be overridden in Python for distance sort)
    base_sql += " ORDER BY p.created_at DESC"
    products = db_fetchall(base_sql, params)

    # Enrich with distance if buyer location exists and farmer hub coords exist
    enriched = []
    for p in products:
        dkm = None
        try:
            if has_buyer_loc and p.get("farmer_hub_lat") is not None and p.get("farmer_hub_lng") is not None:
                dkm = haversine_km(float(buyer_lat), float(buyer_lng), float(p["farmer_hub_lat"]), float(p["farmer_hub_lng"]))
        except Exception:
            dkm = None
        p2 = dict(p)
        p2["distance_km"] = dkm
        enriched.append(p2)

    # Apply radius filter (only when we can compute distance)
    if radius_km is not None and has_buyer_loc:
        enriched = [p for p in enriched if (p.get("distance_km") is not None and p["distance_km"] <= radius_km)]

    # Sort options
    if sort == "nearest" and has_buyer_loc:
        # Distance first; items without distance go last
        enriched.sort(key=lambda p: (p.get("distance_km") is None, p.get("distance_km") if p.get("distance_km") is not None else 1e18, p.get("created_at") or ""))
    elif sort == "price_asc":
        enriched.sort(key=lambda p: (p.get("price") is None, float(p.get("price") or 0)))
    elif sort == "price_desc":
        enriched.sort(key=lambda p: (p.get("price") is None, -float(p.get("price") or 0)))
    else:
        # newest (already roughly ordered)
        pass

    # Group results by farmer to make shopping faster while preserving the single-farmer-per-order rule.
    # This is a UI/UX grouping only; cart constraints remain enforced server-side.
    groups_map = {}
    groups_order = []
    for p in enriched:
        fid = p.get("farmer_user_id")
        if fid not in groups_map:
            g = {
                "farmer_user_id": fid,
                "farmer_name": p.get("farmer_name"),
                "farmer_hub": p.get("farmer_hub"),
                "distance_km": p.get("distance_km"),
                "products": []
            }
            groups_map[fid] = g
            groups_order.append(g)
        groups_map[fid]["products"].append(p)

    # If sorting by nearest, sort groups by farmer distance (unknown distances go last).
    if sort == "nearest" and has_buyer_loc:
        groups_order.sort(key=lambda g: (g.get("distance_km") is None, g.get("distance_km") if g.get("distance_km") is not None else 1e18))


    cart = get_cart()
    cart_count = sum(cart.values())

    return render_template(
        "buyer_dashboard.html",
        user=current_user(),
        groups=groups_order,
        products=enriched,
        cart=cart,
        cart_count=cart_count,
        q=q,
        sort=sort,
        radius_km=radius_km,
        has_buyer_loc=has_buyer_loc,
        buyer_lat=buyer_lat,
        buyer_lng=buyer_lng,
    )

@app.post('/buyer/location')
@login_required
@role_required('buyer')
def buyer_set_location():
    """Persist buyer browsing location in session (for 'Near Me' filters)."""
    data = request.get_json(silent=True) or {}
    try:
        lat = float(data.get('lat'))
        lng = float(data.get('lng'))
    except Exception:
        return jsonify({'ok': False, 'error': 'Invalid latitude/longitude.'}), 400
    # Basic bounds validation
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0):
        return jsonify({'ok': False, 'error': 'Latitude/longitude out of range.'}), 400
    session['buyer_lat'] = lat
    session['buyer_lng'] = lng
    session['buyer_loc_set_at'] = now_str()
    return jsonify({'ok': True})

@app.post('/buyer/location/clear')
@login_required
@role_required('buyer')
def buyer_clear_location():
    session.pop('buyer_lat', None)
    session.pop('buyer_lng', None)
    session.pop('buyer_loc_set_at', None)
    return jsonify({'ok': True})

@app.post('/cart/add')
@login_required
@role_required('buyer')
def cart_add():
    pid = (request.form.get("product_id") or "").strip()
    qty = int((request.form.get("qty") or "1").strip() or "1")
    if qty < 1:
        qty = 1

    # Enforce single-farmer checkout (MVP simplification).
    # This allows the farmer to define ONE pickup point per order.
    pr = db_fetchone(f"SELECT farmer_user_id FROM products WHERE id={_ph()} AND is_active=1", (pid,))
    if not pr:
        flash("Product not found.", "error")
        return redirect(url_for("buyer_dashboard"))

    cart = get_cart()
    if cart:
        marks = _ph_list(len(cart))
        existing = db_fetchall(f"SELECT DISTINCT farmer_user_id FROM products WHERE id IN ({marks})", tuple(cart.keys()))
        existing_farmers = {int(r["farmer_user_id"]) for r in existing if r and r.get("farmer_user_id") is not None}
        if existing_farmers and (int(pr["farmer_user_id"]) not in existing_farmers):
            flash("Cart can only contain items from one farmer per order. Please checkout (or clear) your current cart first.", "error")
            return redirect(url_for("buyer_dashboard"))

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
            SELECT p.*, u.name AS farmer_name, u.hub AS farmer_hub, u.hub_lat AS farmer_hub_lat, u.hub_lng AS farmer_hub_lng, u.hub_accuracy AS farmer_hub_accuracy
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

    farmer = None
    if items:
        p0 = items[0]["product"]
        farmer = {
            "name": p0.get("farmer_name"),
            "hub": p0.get("farmer_hub"),
            "hub_lat": p0.get("farmer_hub_lat"),
            "hub_lng": p0.get("farmer_hub_lng"),
            "hub_accuracy": p0.get("farmer_hub_accuracy"),
        }

    return render_template("checkout.html", user=current_user(), items=items, subtotal=subtotal, farmer=farmer)

def make_order_id() -> str:
    return "ORD-" + secrets.token_hex(4).upper()

@app.post("/order/place")
@login_required
@role_required("buyer")
def order_place():
    # Buyer provides ONLY the delivery destination. Pickup/origin is defined by the farmer.
    dest = (request.form.get("dest") or "").strip() or "Unknown"

    # Optional browser GPS capture for delivery point
    dest_lat = (request.form.get("dest_lat") or "").strip()
    dest_lng = (request.form.get("dest_lng") or "").strip()
    dest_accuracy = (request.form.get("dest_accuracy") or "").strip()

    cart = get_cart()
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


    # Derive farmer pickup (origin) from the single farmer represented in the cart
    farmer_ids_in_cart = sorted({int(by_id.get(str(pid))["farmer_user_id"]) for pid in cart.keys() if by_id.get(str(pid))})
    if not farmer_ids_in_cart:
        flash("Unable to determine farmer for this order.", "error")
        return redirect(url_for("buyer_dashboard"))
    if len(farmer_ids_in_cart) != 1:
        flash("This MVP supports one farmer per order (single pickup point). Please checkout items per farmer.", "error")
        return redirect(url_for("checkout"))

    farmer_id = farmer_ids_in_cart[0]
    farmer = db_fetchone(
        f"SELECT id, name, hub, hub_lat, hub_lng, hub_accuracy FROM users WHERE id={_ph()}",
        (farmer_id,),
    )
    origin = (farmer.get("hub") if farmer else None) or (farmer.get("name") if farmer else None) or "Farmer pickup"
    oid = make_order_id()
    db_execute(
        f"""
        INSERT INTO orders(id, buyer_user_id, origin, dest, status, created_at)
        VALUES({_ph()}, {_ph()}, {_ph()}, {_ph()}, 'NEEDS_QUOTES', {_ph()})
        """,
        (oid, u["id"], origin, dest, now_str()),
    )

    # Persist farmer pickup coordinates (from farmer profile) and buyer dropoff coordinates (optional GPS at checkout)
    def _to_float(s):
        try:
            return float(s)
        except Exception:
            return None

    h_lat = _to_float(str(farmer.get("hub_lat"))) if farmer and farmer.get("hub_lat") is not None else None
    h_lng = _to_float(str(farmer.get("hub_lng"))) if farmer and farmer.get("hub_lng") is not None else None
    h_acc = _to_float(str(farmer.get("hub_accuracy"))) if farmer and farmer.get("hub_accuracy") is not None else None

    d_lat = _to_float(dest_lat)
    d_lng = _to_float(dest_lng)
    d_acc = _to_float(dest_accuracy) if dest_accuracy else None

    if h_lat is not None and h_lng is not None:
        db_execute(
            f"""
            INSERT INTO order_locations(order_id, user_id, role, lat, lng, accuracy, created_at)
            VALUES({_ph()}, {_ph()}, 'origin', {_ph()}, {_ph()}, {_ph()}, {_ph()})
            """,
            (oid, int(farmer_id), h_lat, h_lng, h_acc, now_str()),
        )

    if d_lat is not None and d_lng is not None:
        db_execute(
            f"""
            INSERT INTO order_locations(order_id, user_id, role, lat, lng, accuracy, created_at)
            VALUES({_ph()}, {_ph()}, 'dropoff', {_ph()}, {_ph()}, {_ph()}, {_ph()})
            """,
            (oid, int(u["id"]), d_lat, d_lng, d_acc, now_str()),
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
    logger.info("quote_accepted order_id=%s quote_id=%s buyer_id=%s", order_id, quote_id, u["id"])
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

    open_orders = [dict(r) for r in db_fetchall("""
        SELECT o.*,
               (SELECT COUNT(*) FROM quotes q WHERE q.order_id=o.id) AS quote_count
        FROM orders o
        WHERE o.status='NEEDS_QUOTES'
        ORDER BY o.created_at DESC
    """)]
    for o in open_orders:
        o["distance_km"] = order_distance_km(o["id"])

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
        WHERE q.transporter_user_id={_ph()} AND o.status IN ('QUOTE_ACCEPTED','IN_TRANSIT','ARRIVED')
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


@app.post("/transport/start")
@login_required
@role_required("transporter")
def transport_start_trip():
    """
    Transition order from QUOTE_ACCEPTED -> IN_TRANSIT.
    Only the accepted transporter may do this.
    """
    order_id = request.form.get("order_id")
    u = current_user()
    if not u:
        return redirect(url_for("login"))

    o = db_fetchone(
        f"SELECT * FROM orders WHERE id={_ph()} AND status='QUOTE_ACCEPTED'",
        (order_id,),
    )
    if not o:
        flash("Order not found / not ready to start.", "error")
        return redirect(url_for("transporter_dashboard"))

    q = db_fetchone(
        f"SELECT * FROM quotes WHERE id={_ph()} AND transporter_user_id={_ph()}",
        (o["accepted_quote_id"], u["id"]),
    )
    if not q or q["status"] != "ACCEPTED":
        flash("You are not assigned to this order.", "error")
        return redirect(url_for("transporter_dashboard"))

    db_execute(f"UPDATE orders SET status='IN_TRANSIT' WHERE id={_ph()}", (order_id,))
    db_commit()

    logger.info("order_in_transit order_id=%s transporter_id=%s", order_id, u["id"])

    # Notifications
    buyer = db_fetchone(f"SELECT buyer_user_id FROM orders WHERE id={_ph()}", (order_id,))
    if buyer:
        notify_user(int(buyer["buyer_user_id"]), "IN_TRANSIT", f"Delivery started for order {order_id}.", link=f"/track/{order_id}")

    for fr in db_fetchall(
        f"SELECT DISTINCT p.farmer_user_id AS uid FROM order_items oi JOIN products p ON p.id=oi.product_id WHERE oi.order_id={_ph()}",
        (order_id,),
    ):
        notify_user(int(fr["uid"]), "IN_TRANSIT", f"Delivery started for order {order_id}.", link=f"/track/{order_id}")

    flash("Delivery started.", "ok")
    return redirect(url_for("transporter_dashboard"))


@app.post("/transport/arrive")
@login_required
@role_required("transporter")
def transport_arrive():
    """
    Transition order from IN_TRANSIT -> ARRIVED.
    Only the accepted transporter may do this.
    """
    order_id = request.form.get("order_id")
    u = current_user()
    if not u:
        return redirect(url_for("login"))

    o = db_fetchone(
        f"SELECT * FROM orders WHERE id={_ph()} AND status='IN_TRANSIT'",
        (order_id,),
    )
    if not o:
        flash("Order not found / not in transit.", "error")
        return redirect(url_for("transporter_dashboard"))

    q = db_fetchone(
        f"SELECT * FROM quotes WHERE id={_ph()} AND transporter_user_id={_ph()}",
        (o["accepted_quote_id"], u["id"]),
    )
    if not q or q["status"] != "ACCEPTED":
        flash("You are not assigned to this order.", "error")
        return redirect(url_for("transporter_dashboard"))

    db_execute(f"UPDATE orders SET status='ARRIVED' WHERE id={_ph()}", (order_id,))
    db_commit()

    logger.info("order_arrived order_id=%s transporter_id=%s", order_id, u["id"])

    # Notifications
    buyer = db_fetchone(f"SELECT buyer_user_id FROM orders WHERE id={_ph()}", (order_id,))
    if buyer:
        notify_user(int(buyer["buyer_user_id"]), "ARRIVED", f"Transporter arrived for order {order_id}.", link=f"/track/{order_id}")

    for fr in db_fetchall(
        f"SELECT DISTINCT p.farmer_user_id AS uid FROM order_items oi JOIN products p ON p.id=oi.product_id WHERE oi.order_id={_ph()}",
        (order_id,),
    ):
        notify_user(int(fr["uid"]), "ARRIVED", f"Transporter arrived for order {order_id}.", link=f"/track/{order_id}")

    flash("Marked as arrived.", "ok")
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
        f"SELECT * FROM orders WHERE id={_ph()} AND status='ARRIVED'",
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)