from __future__ import annotations

import os
import sqlite3
import secrets
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, List, Optional

from flask import Flask, g, redirect, render_template, request, session, url_for, flash

APP_TITLE = "AgroMath MVP"
DB_NAME = os.environ.get("AGROMATH_DB", "agromath.db")

# Admin (can approve farmer registrations)
ADMIN_PHONE = os.environ.get("AGROMATH_ADMIN_PHONE", "09066454125")

# OTP settings (demo: displayed on screen)
OTP_TTL_MINUTES = 10

app = Flask(__name__)
# NOTE: In production (e.g., Render), set a STABLE SECRET_KEY environment variable.
# If the secret key changes on restart, all sessions become invalid and users get
# randomly logged out.
app.secret_key = os.environ.get("SECRET_KEY") or "dev-CHANGE-ME"


# -----------------------------
# DB helpers + schema
# -----------------------------

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = connect_db()
    return g.db

@app.teardown_appcontext
def close_db(exception: Exception | None):
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()

def colnames(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r["name"] for r in rows}

def ensure_schema() -> None:
    conn = connect_db()
    conn.execute("PRAGMA foreign_keys = ON;")

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

    # Lightweight migrations for older dbs
    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "users" in tables:
        cols = colnames(conn, "users")
        if "created_at" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN created_at TEXT")
            conn.execute("UPDATE users SET created_at = COALESCE(created_at, ?)", (now_str(),))
        if "farmer_status" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN farmer_status TEXT NOT NULL DEFAULT 'NONE'")
    if "orders" in tables:
        cols = colnames(conn, "orders")
        if "accepted_quote_id" not in cols:
            conn.execute("ALTER TABLE orders ADD COLUMN accepted_quote_id INTEGER")
    conn.commit()
    conn.close()

ensure_schema()


# -----------------------------
# Auth helpers
# -----------------------------

def current_user() -> Optional[sqlite3.Row]:
    uid = session.get("uid")
    if not uid:
        return None
    return get_db().execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()

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

    conn = get_db()
    u = conn.execute("SELECT * FROM users WHERE phone = ?", (phone,)).fetchone()
    if not u:
        conn.execute(
            "INSERT INTO users(phone, created_at, farmer_status) VALUES(?, ?, 'NONE')",
            (phone, now_str()),
        )
        conn.commit()

    otp = str(secrets.randbelow(900000) + 100000)  # 6-digit
    expires = (datetime.now() + timedelta(minutes=OTP_TTL_MINUTES)).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO otps(phone, otp, expires_at, created_at) VALUES(?, ?, ?, ?)",
        (phone, otp, expires, now_str()),
    )
    conn.commit()

    # Store for verify flow
    session["pending_phone"] = phone
    return render_template("login.html", demo_otp=otp, phone=phone)

@app.post("/verify")
def verify():
    phone = (session.get("pending_phone") or "").strip()
    otp = (request.form.get("otp") or "").strip()

    if not phone:
        flash("Please request an OTP first.", "error")
        return redirect(url_for("login"))

    conn = get_db()
    row = conn.execute(
        "SELECT * FROM otps WHERE phone = ? ORDER BY id DESC LIMIT 1",
        (phone,),
    ).fetchone()

    if not row:
        flash("No OTP found. Please request again.", "error")
        return redirect(url_for("login"))

    if otp != row["otp"]:
        flash("Invalid OTP.", "error")
        return render_template("login.html", demo_otp=row["otp"], phone=phone)

    if row["expires_at"] < now_str():
        flash("OTP expired. Please request a new one.", "error")
        return redirect(url_for("login"))

    u = conn.execute("SELECT * FROM users WHERE phone = ?", (phone,)).fetchone()
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

    conn = get_db()
    u = current_user()
    if not u:
        return redirect(url_for("login"))

    farmer_status = u["farmer_status"]
    if role == "farmer":
        # Option 2: self-register, admin approves
        if u["phone"] == ADMIN_PHONE:
            farmer_status = "APPROVED"  # Admin can act as farmer for testing
        else:
            farmer_status = "PENDING" if u["farmer_status"] in ("NONE", "DECLINED") else u["farmer_status"]
    else:
        # Non-farmer roles don't need approval
        farmer_status = "NONE"

    conn.execute(
        "UPDATE users SET name=?, role=?, hub=?, farmer_status=? WHERE id=?",
        (name, role, hub, farmer_status, u["id"]),
    )
    conn.commit()

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

    conn = get_db()
    pending = conn.execute(
        "SELECT * FROM users WHERE role='farmer' AND farmer_status='PENDING' ORDER BY created_at DESC"
    ).fetchall()
    return render_template("admin.html", user=u, pending=pending)

@app.post("/admin/farmer/approve")
@login_required
def admin_farmer_approve():
    u = current_user()
    if not u or u["phone"] != ADMIN_PHONE:
        flash("Admin access only.", "error")
        return redirect(url_for("dashboard"))

    user_id = request.form.get("user_id")
    conn = get_db()
    conn.execute("UPDATE users SET farmer_status='APPROVED' WHERE id=?", (user_id,))
    conn.commit()
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
    conn = get_db()
    conn.execute("UPDATE users SET farmer_status='DECLINED' WHERE id=?", (user_id,))
    conn.commit()
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
    # normalize to int
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
    conn = get_db()
    products = conn.execute("""
        SELECT p.*, u.name AS farmer_name, u.hub AS farmer_hub
        FROM products p
        JOIN users u ON u.id = p.farmer_user_id
        WHERE p.is_active=1 AND u.role='farmer' AND u.farmer_status='APPROVED'
        ORDER BY p.created_at DESC
    """).fetchall()

    cart = get_cart()
    cart_count = sum(cart.values())
    return render_template("buyer_dashboard.html", user=current_user(), products=products, cart=cart, cart_count=cart_count)

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
    conn = get_db()
    cart = get_cart()
    items = []
    subtotal = 0
    if cart:
        qmarks = ",".join(["?"] * len(cart))
        rows = conn.execute(
            f"SELECT p.*, u.name AS farmer_name, u.hub AS farmer_hub FROM products p JOIN users u ON u.id=p.farmer_user_id WHERE p.id IN ({qmarks})",
            tuple(cart.keys()),
        ).fetchall()
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
    # short but unique enough for MVP
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

    conn = get_db()
    u = current_user()
    assert u is not None

    qmarks = ",".join(["?"] * len(cart))
    rows = conn.execute(
        f"SELECT * FROM products WHERE id IN ({qmarks}) AND is_active=1",
        tuple(cart.keys()),
    ).fetchall()
    by_id = {str(r["id"]): r for r in rows}
    if not by_id:
        flash("No valid items in cart.", "error")
        return redirect(url_for("buyer_dashboard"))

    oid = make_order_id()
    conn.execute(
        "INSERT INTO orders(id, buyer_user_id, origin, dest, status, created_at) VALUES(?, ?, ?, ?, 'NEEDS_QUOTES', ?)",
        (oid, u["id"], origin, dest, now_str()),
    )

    for pid, qty in cart.items():
        pr = by_id.get(str(pid))
        if not pr:
            continue
        conn.execute(
            "INSERT INTO order_items(order_id, product_id, qty, unit_price) VALUES(?, ?, ?, ?)",
            (oid, int(pid), int(qty), int(pr["price"])),
        )

    conn.commit()
    set_cart({})
    flash(f"Order {oid} placed. Waiting for transporter quotes.", "ok")
    return redirect(url_for("orders"))

@app.get("/orders")
@login_required
def orders():
    conn = get_db()
    u = current_user()
    if not u:
        return redirect(url_for("login"))

    if u["role"] == "buyer":
        orders = conn.execute(
            "SELECT * FROM orders WHERE buyer_user_id=? ORDER BY created_at DESC",
            (u["id"],),
        ).fetchall()
    elif u["role"] == "farmer":
        # Farmer sees orders that include their products
        orders = conn.execute("""
            SELECT DISTINCT o.*
            FROM orders o
            JOIN order_items oi ON oi.order_id=o.id
            JOIN products p ON p.id=oi.product_id
            WHERE p.farmer_user_id=?
            ORDER BY o.created_at DESC
        """, (u["id"],)).fetchall()
    elif u["role"] == "transporter":
        # Transporter sees: (1) open orders needing quotes + (2) orders where they have accepted quote
        orders = conn.execute("""
            SELECT * FROM orders
            WHERE status IN ('NEEDS_QUOTES','QUOTE_ACCEPTED')
            ORDER BY created_at DESC
        """).fetchall()
    else:
        orders = []

    # hydrate order details
    out = []
    for o in orders:
        items = conn.execute("""
            SELECT oi.*, p.name AS product_name, p.unit AS product_unit, u.name AS farmer_name
            FROM order_items oi
            JOIN products p ON p.id=oi.product_id
            JOIN users u ON u.id=p.farmer_user_id
            WHERE oi.order_id=?
        """, (o["id"],)).fetchall()

        quotes = conn.execute("""
            SELECT q.*, u.name AS transporter_name, u.phone AS transporter_phone
            FROM quotes q
            JOIN users u ON u.id=q.transporter_user_id
            WHERE q.order_id=?
            ORDER BY q.created_at DESC
        """, (o["id"],)).fetchall()

        out.append({"order": o, "items": items, "quotes": quotes})

    return render_template("orders.html", user=u, orders=out)

@app.post("/quote/accept")
@login_required
@role_required("buyer")
def quote_accept():
    quote_id = request.form.get("quote_id")
    order_id = request.form.get("order_id")

    conn = get_db()
    u = current_user()
    assert u is not None

    o = conn.execute("SELECT * FROM orders WHERE id=? AND buyer_user_id=?", (order_id, u["id"])).fetchone()
    if not o:
        flash("Order not found.", "error")
        return redirect(url_for("orders"))

    q = conn.execute("SELECT * FROM quotes WHERE id=? AND order_id=?", (quote_id, order_id)).fetchone()
    if not q or q["status"] in ("DECLINED",):
        flash("Quote not found / already declined.", "error")
        return redirect(url_for("orders"))

    # Accept selected quote, decline all others
    conn.execute("UPDATE quotes SET status='ACCEPTED' WHERE id=?", (quote_id,))
    conn.execute("UPDATE quotes SET status='DECLINED' WHERE order_id=? AND id<>? AND status<>'DELIVERED'", (order_id, quote_id))
    conn.execute("UPDATE orders SET status='QUOTE_ACCEPTED', accepted_quote_id=? WHERE id=?", (quote_id, order_id))
    conn.commit()

    flash("Quote accepted. Transporter can now deliver.", "ok")
    return redirect(url_for("orders"))

@app.post("/quote/decline")
@login_required
@role_required("buyer")
def quote_decline():
    quote_id = request.form.get("quote_id")
    order_id = request.form.get("order_id")

    conn = get_db()
    u = current_user()
    assert u is not None

    o = conn.execute("SELECT * FROM orders WHERE id=? AND buyer_user_id=?", (order_id, u["id"])).fetchone()
    if not o:
        flash("Order not found.", "error")
        return redirect(url_for("orders"))

    conn.execute("UPDATE quotes SET status='DECLINED' WHERE id=? AND order_id=? AND status='SUBMITTED'", (quote_id, order_id))
    conn.commit()
    flash("Quote declined.", "ok")
    return redirect(url_for("orders"))


# -----------------------------
# Farmer flows
# -----------------------------

@app.get("/farmer")
@login_required
@role_required("farmer")
def farmer_dashboard():
    conn = get_db()
    u = current_user()
    assert u is not None

    products = conn.execute(
        "SELECT * FROM products WHERE farmer_user_id=? ORDER BY created_at DESC",
        (u["id"],),
    ).fetchall()

    my_orders = conn.execute("""
        SELECT DISTINCT o.*
        FROM orders o
        JOIN order_items oi ON oi.order_id=o.id
        JOIN products p ON p.id=oi.product_id
        WHERE p.farmer_user_id=?
        ORDER BY o.created_at DESC
    """, (u["id"],)).fetchall()

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
    conn = get_db()
    conn.execute(
        "INSERT INTO products(farmer_user_id, name, unit, price, created_at) VALUES(?, ?, ?, ?, ?)",
        (u["id"], name, unit, price, now_str()),
    )
    conn.commit()
    flash("Product added.", "ok")
    return redirect(url_for("farmer_dashboard"))

@app.post("/farmer/product/toggle")
@login_required
@role_required("farmer")
def farmer_product_toggle():
    pid = request.form.get("product_id")
    u = current_user()
    assert u is not None
    conn = get_db()
    row = conn.execute("SELECT * FROM products WHERE id=? AND farmer_user_id=?", (pid, u["id"])).fetchone()
    if not row:
        flash("Product not found.", "error")
        return redirect(url_for("farmer_dashboard"))
    new_state = 0 if row["is_active"] == 1 else 1
    conn.execute("UPDATE products SET is_active=? WHERE id=?", (new_state, pid))
    conn.commit()
    flash("Updated.", "ok")
    return redirect(url_for("farmer_dashboard"))


# -----------------------------
# Transporter flows
# -----------------------------

@app.get("/transport")
@login_required
@role_required("transporter")
def transporter_dashboard():
    conn = get_db()
    u = current_user()
    assert u is not None

    open_orders = conn.execute("""
        SELECT o.*, 
               (SELECT COUNT(*) FROM quotes q WHERE q.order_id=o.id) AS quote_count
        FROM orders o
        WHERE o.status='NEEDS_QUOTES'
        ORDER BY o.created_at DESC
    """).fetchall()

    my_quotes = conn.execute("""
        SELECT q.*, o.origin, o.dest, o.status AS order_status
        FROM quotes q
        JOIN orders o ON o.id=q.order_id
        WHERE q.transporter_user_id=?
        ORDER BY q.created_at DESC
    """, (u["id"],)).fetchall()

    accepted_orders = conn.execute("""
        SELECT o.*, q.price, q.eta_hours, q.id AS quote_id
        FROM orders o
        JOIN quotes q ON q.id=o.accepted_quote_id
        WHERE q.transporter_user_id=? AND o.status='QUOTE_ACCEPTED'
        ORDER BY o.created_at DESC
    """, (u["id"],)).fetchall()

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

    conn = get_db()
    u = current_user()
    assert u is not None

    o = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
    if not o or o["status"] != "NEEDS_QUOTES":
        flash("Order not available for quoting.", "error")
        return redirect(url_for("transporter_dashboard"))

    conn.execute(
        "INSERT INTO quotes(order_id, transporter_user_id, price, eta_hours, status, created_at) VALUES(?, ?, ?, ?, 'SUBMITTED', ?)",
        (order_id, u["id"], price, eta_hours, now_str()),
    )
    conn.commit()
    flash("Quote submitted.", "ok")
    return redirect(url_for("transporter_dashboard"))

@app.post("/transport/deliver")
@login_required
@role_required("transporter")
def transport_deliver():
    order_id = request.form.get("order_id")
    conn = get_db()
    u = current_user()
    if not u:
        return redirect(url_for("login"))

    o = conn.execute("SELECT * FROM orders WHERE id=? AND status='QUOTE_ACCEPTED'", (order_id,)).fetchone()
    if not o:
        flash("Order not found / not ready for delivery.", "error")
        return redirect(url_for("transporter_dashboard"))

    q = conn.execute("SELECT * FROM quotes WHERE id=? AND transporter_user_id=?", (o["accepted_quote_id"], u["id"])).fetchone()
    if not q or q["status"] != "ACCEPTED":
        flash("You are not assigned to this order.", "error")
        return redirect(url_for("transporter_dashboard"))

    conn.execute("UPDATE quotes SET status='DELIVERED' WHERE id=?", (q["id"],))
    conn.execute("UPDATE orders SET status='DELIVERED' WHERE id=?", (order_id,))
    conn.commit()

    flash("Order marked delivered.", "ok")
    return redirect(url_for("transporter_dashboard"))


if __name__ == "__main__":
    # Local: `python app.py`  -> http://127.0.0.1:5000
    # Render/production: use gunicorn and bind to $PORT.
    port = int(os.environ.get("PORT", "5000"))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
