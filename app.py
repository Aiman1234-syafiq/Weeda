import os
import sqlite3
import threading
import time
from datetime import datetime
from decimal import Decimal
import json
from functools import wraps
from contextlib import contextmanager

from flask import (
    Flask, render_template, request, redirect,
    url_for, session, abort, flash, jsonify
)
from werkzeug.security import generate_password_hash, check_password_hash

# ==================================================
# BASIC CONFIG
# ==================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "pr_enterprise.db"))

app = Flask(__name__)
app.secret_key = os.environ.get(
    "SECRET_KEY",
    "change-this-in-production-32-char-secret"
)

# SESSION CONFIG (DEV SAFE)
app.config['SESSION_COOKIE_SECURE'] = os.environ.get("SESSION_COOKIE_SECURE", "false").lower() == "true"
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['PERMANENT_SESSION_LIFETIME'] = 1800

# ==================================================
# GLOBAL TEMPLATE CONTEXT
# ==================================================
@app.context_processor
def inject_globals():
    """Inject global variables into all templates"""
    return {
        'datetime': datetime,
        'now': datetime.now,
        'current_year': datetime.now().year
    }

# ==================================================
# CONSTANTS BERDASARKAN FLOWCHART
# ==================================================
APPROVAL_THRESHOLDS = {
    'LEVEL_1': 10000,      # Approver 1: Division Head/Director
    'LEVEL_2': 50000,      # Approver 2: Group CFO → Approver 3: Group CEO
    'LEVEL_3': 100000,     # Approver 4: Group MD
}

BUDGET_STATUS = {
    'IN_BUDGET': 'IN_BUDGET',
    'OUT_OF_BUDGET': 'OUT_OF_BUDGET',
    'EXCEPTION_APPROVED': 'EXCEPTION_APPROVED',
    'EXCEPTION_PENDING': 'BUDGET_EXCEPTION_PENDING'
}

# ==================================================
# DATABASE CONNECTION MANAGER
# ==================================================
thread_local = threading.local()

@contextmanager
def db():
    """
    Context manager untuk koneksi database dengan connection pooling
    dan WAL mode untuk menghindari database locked
    """
    max_retries = 5
    retry_delay = 0.1  # 100ms
    
    for attempt in range(max_retries):
        try:
            # Cek apakah sudah ada connection untuk thread ini
            if not hasattr(thread_local, 'connection'):
                thread_local.connection = sqlite3.connect(
                    DB_PATH,
                    check_same_thread=False,
                    timeout=30.0  # Timeout 30 detik
                )
                thread_local.connection.row_factory = sqlite3.Row
                # Enable WAL mode untuk concurrent access
                thread_local.connection.execute("PRAGMA journal_mode=WAL")
                thread_local.connection.execute("PRAGMA synchronous=NORMAL")
                thread_local.connection.execute("PRAGMA foreign_keys=ON")
                thread_local.connection.execute("PRAGMA busy_timeout=5000")
            
            conn = thread_local.connection
            
            try:
                yield conn
                conn.commit()  # Commit perubahan
                break  # Keluar dari loop retry jika berhasil
            except sqlite3.OperationalError as e:
                if 'locked' in str(e) and attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                    continue
                else:
                    conn.rollback()
                    raise e
            except Exception as e:
                conn.rollback()
                raise e
                
        except sqlite3.OperationalError as e:
            if 'locked' in str(e) and attempt < max_retries - 1:
                # Clean up failed connection
                if hasattr(thread_local, 'connection'):
                    try:
                        thread_local.connection.close()
                    except:
                        pass
                    del thread_local.connection
                time.sleep(retry_delay * (2 ** attempt))
                continue
            else:
                raise e

def close_db_connections():
    """
    Close semua database connections (dipanggil saat aplikasi shutdown)
    """
    if hasattr(thread_local, 'connection'):
        try:
            thread_local.connection.close()
        except:
            pass
        del thread_local.connection

# ==================================================
# DATABASE INITIALIZATION & MIGRATION
# ==================================================
def migrate_vendor_columns():
    """
    Add additional columns to vendors table if they don't exist
    """
    try:
        with db() as conn:
            # Check if columns exist
            columns_needed = [
                ('bank_address', 'TEXT'),
                ('bank_code', 'TEXT'),
                ('swift_code', 'TEXT'),
                ('fax_no', 'TEXT'),
                ('incoterms', 'TEXT'),
                ('order_currency', 'TEXT DEFAULT "MYR"'),
                ('year_established', 'TEXT'),
                ('created_status', 'TEXT DEFAULT "New Vendor"')
            ]
            
            for column_name, column_type in columns_needed:
                try:
                    conn.execute(f"ALTER TABLE vendors ADD COLUMN {column_name} {column_type}")
                    print(f"✅ Added column: {column_name}")
                except sqlite3.OperationalError:
                    # Column already exists
                    pass
            
            print("✅ Vendor table migration completed")
            
    except Exception as e:
        print(f"⚠️ Migration error: {e}")

def init_db():
    """
    Initialize database dengan WAL mode dan connection yang aman
    """
    try:
        print("🔄 Initializing database...")
        
        # Gunakan connection khusus untuk init
        conn = sqlite3.connect(DB_PATH, timeout=60.0, check_same_thread=False)
        
        # Enable WAL mode untuk concurrent access
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        
        # USERS
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            full_name TEXT NOT NULL,
            email TEXT,
            department TEXT,
            role TEXT NOT NULL,
            approval_limit REAL DEFAULT 0,
            active INTEGER DEFAULT 1,
            created_at TEXT NOT NULL,
            last_login TEXT
        )
        """)
        
        # BUDGET CATEGORIES
        conn.execute("""
        CREATE TABLE IF NOT EXISTS budget_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            department TEXT NOT NULL,
            category TEXT NOT NULL,
            fiscal_year TEXT NOT NULL,
            allocated_amount REAL NOT NULL,
            spent_amount REAL DEFAULT 0,
            remaining_amount REAL GENERATED ALWAYS AS (allocated_amount - spent_amount) VIRTUAL,
            UNIQUE(department, category, fiscal_year)
        )
        """)
        
        # PR
        conn.execute("""
        CREATE TABLE IF NOT EXISTS pr (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pr_no TEXT UNIQUE NOT NULL,
            fiscal_year TEXT NOT NULL,
            
            -- Basic Info
            created_at TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            requester_name TEXT NOT NULL,
            department TEXT NOT NULL,
            
            -- Budget Info
            budget_category TEXT,
            budget_status TEXT DEFAULT 'OUT_OF_BUDGET',
            budget_exception_approver INTEGER,
            budget_exception_date TEXT,
            budget_exception_notes TEXT,
            
            -- Purchase Details
            purpose TEXT NOT NULL,
            priority TEXT DEFAULT 'NORMAL',
            
            -- Vendor Info
            vendor_name TEXT NOT NULL,
            vendor_code TEXT,
            vendor_address TEXT,
            vendor_contact TEXT,
            vendor_email TEXT,
            payment_terms TEXT,
            delivery_address TEXT,
            
            -- Financial
            total_amount REAL NOT NULL,
            currency TEXT DEFAULT 'MYR',
            tax_amount REAL DEFAULT 0,
            grand_total REAL GENERATED ALWAYS AS (total_amount + tax_amount) VIRTUAL,
            
            -- Status & Approval
            status TEXT NOT NULL,
            current_approver_role TEXT,
            
            -- Approval Status Tracking
            approver1_id INTEGER,
            approver1_status TEXT DEFAULT 'PENDING',
            approver1_date TEXT,
            approver1_notes TEXT,
            
            approver2_id INTEGER,
            approver2_status TEXT DEFAULT 'PENDING',
            approver2_date TEXT,
            approver2_notes TEXT,
            
            approver3_id INTEGER,
            approver3_status TEXT DEFAULT 'PENDING',
            approver3_date TEXT,
            approver3_notes TEXT,
            
            approver4_id INTEGER,
            approver4_status TEXT DEFAULT 'PENDING',
            approver4_date TEXT,
            approver4_notes TEXT,
            
            -- Requestor Sign-off
            requestor_signoff_date TEXT,
            requestor_signoff_user INTEGER,
            
            -- Procurement
            procurement_received_date TEXT,
            procurement_officer_id INTEGER,
            
            -- Audit Trail
            last_updated TEXT NOT NULL,
            rejection_reason TEXT,
            
            FOREIGN KEY (created_by) REFERENCES users(id),
            FOREIGN KEY (budget_exception_approver) REFERENCES users(id)
        )
        """)
        
        # PR ITEMS
        conn.execute("""
        CREATE TABLE IF NOT EXISTS pr_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pr_id INTEGER NOT NULL,
            item_no INTEGER NOT NULL,
            item_description TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_of_measure TEXT DEFAULT 'UNIT',
            unit_price REAL NOT NULL,
            total_price REAL NOT NULL,
            catalog_number TEXT,
            specifications TEXT,
            notes TEXT,
            FOREIGN KEY (pr_id) REFERENCES pr(id) ON DELETE CASCADE
        )
        """)
        
        # APPROVAL HISTORY
        conn.execute("""
        CREATE TABLE IF NOT EXISTS approval_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pr_id INTEGER NOT NULL,
            approver_role TEXT NOT NULL,
            approver_id INTEGER,
            action TEXT NOT NULL,
            action_date TEXT NOT NULL,
            comments TEXT,
            ip_address TEXT,
            FOREIGN KEY (pr_id) REFERENCES pr(id),
            FOREIGN KEY (approver_id) REFERENCES users(id)
        )
        """)
        
        # NOTIFICATIONS
        conn.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            notification_type TEXT,
            is_read INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            related_pr_id INTEGER,
            action_url TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """)
        
        # VENDORS - UPDATED dengan kolom tambahan
        conn.execute("""
        CREATE TABLE IF NOT EXISTS vendors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_code TEXT UNIQUE NOT NULL,
            vendor_name TEXT NOT NULL,
            vendor_type TEXT,
            registration_date TEXT,
            tax_id TEXT,
            address TEXT,
            contact_person TEXT,
            contact_email TEXT,
            contact_phone TEXT,
            bank_name TEXT,
            bank_account TEXT,
            bank_address TEXT,
            bank_code TEXT,
            swift_code TEXT,
            payment_terms TEXT DEFAULT 'NET30',
            fax_no TEXT,
            incoterms TEXT,
            order_currency TEXT DEFAULT 'MYR',
            year_established TEXT,
            created_status TEXT DEFAULT 'New Vendor',
            rating INTEGER DEFAULT 5,
            is_active INTEGER DEFAULT 1,
            notes TEXT
        )
        """)
        
        # INSERT DEFAULT BUDGET DATA
        current_year = datetime.now().year
        budgets = [
            ('IT', 'Hardware', str(current_year), 500000),
            ('IT', 'Software', str(current_year), 300000),
            ('HR', 'Training', str(current_year), 200000),
            ('Finance', 'Office Supplies', str(current_year), 100000),
            ('Operations', 'Maintenance', str(current_year), 400000),
        ]
        
        for dept, category, year, amount in budgets:
            conn.execute("""
            INSERT OR IGNORE INTO budget_categories 
            (department, category, fiscal_year, allocated_amount)
            VALUES (?, ?, ?, ?)
            """, (dept, category, year, amount))
        
        conn.close()
        print("✅ Database initialized successfully with WAL mode")
        
        # Run migration untuk kolom tambahan
        migrate_vendor_columns()
        
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
        raise

# ==================================================
# HELPER FUNCTIONS
# ==================================================
def login_required(fn):
    @wraps(fn)
    def wrap(*args, **kwargs):
        if not session.get("user_id"):
            flash("Please login first", "warning")
            return redirect("/")
        return fn(*args, **kwargs)
    return wrap

def role_required(*roles):
    def deco(fn):
        @wraps(fn)
        def wrap(*args, **kwargs):
            if session.get("role") not in roles:
                abort(403, description="Insufficient permissions")
            return fn(*args, **kwargs)
        return wrap
    return deco

def generate_pr_no(dept):
    """Generate PR Number dengan format: PR-YYYY-DEPT-001"""
    year = datetime.now().strftime("%Y")
    try:
        with db() as conn:
            count = conn.execute("""
            SELECT COUNT(*) FROM pr 
            WHERE department=? AND strftime('%Y', created_at)=?
            """, (dept.upper(), year)).fetchone()[0]
        
        seq = count + 1
        return f"PR-{year}-{dept.upper()}-{seq:03d}"
    except:
        # Fallback jika ada error
        timestamp = int(datetime.now().timestamp())
        return f"PR-{year}-{dept.upper()}-{timestamp}"

def check_budget_availability(department, category, amount, fiscal_year):
    """Cek apakah ada budget yang cukup"""
    try:
        with db() as conn:
            budget = conn.execute("""
            SELECT allocated_amount, spent_amount, remaining_amount
            FROM budget_categories
            WHERE department=? AND category=? AND fiscal_year=?
            """, (department, category, fiscal_year)).fetchone()
        
        if not budget:
            return {'available': False, 'remaining': 0, 'message': 'Budget category not found'}
        
        if budget['remaining_amount'] >= amount:
            return {
                'available': True,
                'remaining': budget['remaining_amount'],
                'message': 'Within budget'
            }
        else:
            return {
                'available': False,
                'remaining': budget['remaining_amount'],
                'message': f'Insufficient budget. Remaining: {budget["remaining_amount"]:.2f}'
            }
    except Exception as e:
        return {'available': False, 'remaining': 0, 'message': f'Error checking budget: {str(e)}'}

def get_approval_path(amount, budget_status):
    if budget_status == BUDGET_STATUS['OUT_OF_BUDGET']:
        return ['approver1']
    
    if amount <= APPROVAL_THRESHOLDS['LEVEL_1']:
        return ['approver1']
    elif amount <= APPROVAL_THRESHOLDS['LEVEL_2']:
        return ['approver2', 'approver3']
    else:
        return ['approver4']

def create_notification(user_id, title, message, notif_type='INFO', pr_id=None):
    """Buat notifikasi untuk user"""
    try:
        with db() as conn:
            conn.execute("""
            INSERT INTO notifications 
            (user_id, title, message, notification_type, created_at, related_pr_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (
                user_id, title, message, notif_type,
                datetime.now().isoformat(), pr_id
            ))
    except Exception as e:
        print(f"⚠️ Failed to create notification: {e}")

def log_approval_action(pr_id, action, comments='', approver_id=None):
    """Log setiap action approval"""
    try:
        with db() as conn:
            conn.execute("""
            INSERT INTO approval_history 
            (pr_id, approver_role, approver_id, action, action_date, comments, ip_address)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                pr_id,
                session.get('role'),
                approver_id or session.get('user_id'),
                action,
                datetime.now().isoformat(),
                comments,
                request.remote_addr if request else 'N/A'
            ))
    except Exception as e:
        print(f"⚠️ Failed to log approval action: {e}")

# ==================================================
# AUTHENTICATION
# ==================================================
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        
        try:
            with db() as conn:
                user = conn.execute("""
                SELECT * FROM users 
                WHERE username=? AND active=1
                """, (username,)).fetchone()
                
                if user and check_password_hash(user["password_hash"], password):
                    # Update last login
                    conn.execute("""
                    UPDATE users SET last_login=?
                    WHERE id=?
                    """, (datetime.now().isoformat(), user["id"]))
                    
                    # Set session
                    session["user_id"] = user["id"]
                    session["role"] = user["role"]
                    session["name"] = user["full_name"]
                    session["department"] = user["department"] or ""

                    session.permanent = True
                    
                    # Create login notification
                    create_notification(
                        user["id"],
                        "Login Successful",
                        f"You logged in successfully at {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                        "SUCCESS"
                    )
                    
                    flash("Login successful!", "success")
                    return redirect("/dashboard")
            
            flash("Invalid username or password", "danger")
            
        except Exception as e:
            print(f"⚠️ Login error: {e}")
            flash("System error. Please try again.", "danger")
    
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out successfully", "info")
    return redirect("/")

# ==================================================
# DASHBOARD
# ==================================================
@app.route("/dashboard")
@login_required
def dashboard():
    try:
        user_id = session["user_id"]
        role = session["role"]
        department = session.get("department", "")
        
        with db() as conn:
            # Get notifications
            notifications = conn.execute("""
            SELECT * FROM notifications 
            WHERE user_id=? AND is_read=0
            ORDER BY created_at DESC
            LIMIT 10
            """, (user_id,)).fetchall()
            
            # Get dashboard stats based on role
            stats = {}
            my_pr = []
            
            if role == "user":
                # User's PR statistics
                stats['total_pr'] = conn.execute("""
                SELECT COUNT(*) FROM pr WHERE created_by=?
                """, (user_id,)).fetchone()[0]
                
                stats['pending_pr'] = conn.execute("""
                SELECT COUNT(*) FROM pr 
                WHERE created_by=? AND status IN ('PENDING_APPROVAL', 'BUDGET_EXCEPTION_PENDING')
                """, (user_id,)).fetchone()[0]
                
                stats['approved_pr'] = conn.execute("""
                SELECT COUNT(*) FROM pr 
                WHERE created_by=? AND status='APPROVED'
                """, (user_id,)).fetchone()[0]
                
                # Get user's PRs
                my_pr = conn.execute("""
                SELECT p.*, 
                       (SELECT COUNT(*) FROM pr_items WHERE pr_id=p.id) as item_count
                FROM pr p
                WHERE p.created_by=?
                ORDER BY p.created_at DESC
                LIMIT 10
                """, (user_id,)).fetchall()
            
            elif role in ['approver1', 'approver2', 'approver3', 'approver4']:
                # Approver's pending count
                stats['pending_approvals'] = conn.execute("""
                SELECT COUNT(*) FROM pr
                WHERE status='PENDING_APPROVAL' 
                AND current_approver_role=?
                """, (role,)).fetchone()[0]
                
                # Get PRs pending approval
                my_pr = conn.execute("""
                SELECT p.*, u.full_name as requester_name_full
                FROM pr p
                JOIN users u ON p.created_by = u.id
                WHERE p.status='PENDING_APPROVAL' 
                AND p.current_approver_role=?
                ORDER BY p.created_at DESC
                LIMIT 10
                """, (role,)).fetchall()
            
            elif role == 'procurement':
                # Procurement dashboard
                stats['total_approved'] = conn.execute("""
                SELECT COUNT(*) FROM pr WHERE status='APPROVED'
                """).fetchone()[0]
                
                stats['received_count'] = conn.execute("""
                SELECT COUNT(*) FROM pr WHERE procurement_received_date IS NOT NULL
                """).fetchone()[0]
                
                stats['pending_receipt'] = stats['total_approved'] - stats['received_count']
                
                my_pr = conn.execute("""
                SELECT p.*, u.full_name as requester_name_full
                FROM pr p
                JOIN users u ON p.created_by = u.id
                WHERE p.status='APPROVED'
                ORDER BY p.created_at DESC
                LIMIT 10
                """).fetchall()
            
            elif role == 'superadmin':
                # Admin dashboard
                stats['total_users'] = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
                stats['total_prs'] = conn.execute("SELECT COUNT(*) FROM pr").fetchone()[0]
                stats['total_vendors'] = conn.execute("SELECT COUNT(*) FROM vendors").fetchone()[0]
                
                my_pr = conn.execute("""
                SELECT p.*, u.full_name as requester_name_full
                FROM pr p
                JOIN users u ON p.created_by = u.id
                ORDER BY p.created_at DESC
                LIMIT 10
                """).fetchall()
            
            # Get budget overview for user's department
            budget_overview = None
            if department:
                budget_overview = conn.execute("""
                SELECT category, allocated_amount, spent_amount, remaining_amount
                FROM budget_categories
                WHERE department=?
                AND fiscal_year=?
                """, (department, str(datetime.now().year))).fetchall()
        
        return render_template(
            "dashboard.html",
            role=role,
            name=session["name"],
            notifications=notifications,
            stats=stats,
            my_pr=my_pr,
            budget_overview=budget_overview,
            BUDGET_STATUS=BUDGET_STATUS
        )
        
    except Exception as e:
        print(f"⚠️ Dashboard error: {e}")
        flash("Error loading dashboard", "danger")
        return redirect("/")

# ==================================================
# CREATE PR
# ==================================================
@app.route("/pr/new", methods=["GET", "POST"])
@login_required
@role_required("user")
def pr_new():
    if request.method == "POST":
        try:
            # Parse form data
            department = request.form["department"]
            budget_category = request.form.get("budget_category")
            fiscal_year = request.form.get("fiscal_year", str(datetime.now().year))
            
            # Parse items from JSON
            items = json.loads(request.form.get("items", "[]"))
            
            # Calculate totals
            total_amount = sum(float(item.get('total_price', 0)) for item in items)
            
            # Check budget availability
            budget_check = None
            if budget_category:
                budget_check = check_budget_availability(
                    department, budget_category, total_amount, fiscal_year
                )
            
            # Generate PR number
            pr_no = generate_pr_no(department)
            
            # Determine initial status based on budget check
            if budget_check and budget_check['available']:
                initial_status = "DRAFT"
                budget_status = BUDGET_STATUS['IN_BUDGET']
            else:
                initial_status = "BUDGET_EXCEPTION_PENDING"
                budget_status = BUDGET_STATUS['OUT_OF_BUDGET']
            
            with db() as conn:
                # Validate vendor code if provided
                vendor_code = request.form.get("vendor_code")
                if vendor_code:
                    vendor = conn.execute("""
                    SELECT vendor_name FROM vendors 
                    WHERE vendor_code=? AND is_active=1
                    """, (vendor_code,)).fetchone()
                    
                    if not vendor:
                        flash("Invalid or inactive vendor code", "danger")
                        return redirect("/pr/new")
                
                # Insert PR header
                cursor = conn.execute("""
                INSERT INTO pr (
                    pr_no, fiscal_year,
                    created_at, created_by, 
                    requester_name, department,
                    budget_category, budget_status,
                    purpose, priority,
                    vendor_name, vendor_code, vendor_contact,
                    total_amount, currency, tax_amount,
                    status, current_approver_role,
                    last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    pr_no, fiscal_year,
                    datetime.now().isoformat(), session["user_id"],
                    request.form.get("requester_name", session["name"]), department,
                    budget_category, budget_status,
                    request.form["purpose"], request.form.get("priority", "NORMAL"),
                    request.form["vendor_name"], vendor_code,
                    request.form.get("vendor_contact"),
                    total_amount, request.form.get("currency", "MYR"),
                    float(request.form.get("tax_amount", 0)),
                    initial_status, "",
                    datetime.now().isoformat()
                ))
                
                pr_id = cursor.lastrowid
                
                # Insert PR items
                for idx, item in enumerate(items, 1):
                    conn.execute("""
                    INSERT INTO pr_items (
                        pr_id, item_no, item_description,
                        quantity, unit_of_measure, unit_price, total_price,
                        catalog_number, specifications, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        pr_id, idx, item.get('description', ''),
                        int(item.get('quantity', 1)), item.get('uom', 'UNIT'),
                        float(item.get('unit_price', 0)), float(item.get('total_price', 0)),
                        item.get('catalog_number'), item.get('specifications'),
                        item.get('notes')
                    ))
                
                # Update budget if within budget
                if budget_check and budget_check['available'] and budget_category:
                    conn.execute("""
                    UPDATE budget_categories 
                    SET spent_amount = spent_amount + ?
                    WHERE department=? AND category=? AND fiscal_year=?
                    """, (total_amount, department, budget_category, fiscal_year))
                
                # Create notification
                create_notification(
                    session["user_id"],
                    "PR Created",
                    f"PR {pr_no} has been created successfully.",
                    "SUCCESS",
                    pr_id
                )
            
            flash(f"PR {pr_no} created successfully!", "success")
            
            if initial_status == "BUDGET_EXCEPTION_PENDING":
                flash("This PR requires budget exception approval before submission.", "warning")
                return redirect(f"/pr/{pr_id}/budget-exception")
            
            return redirect("/dashboard")
            
        except Exception as e:
            print(f"❌ Error creating PR: {e}")
            flash(f"Error creating PR: {str(e)}", "danger")
            return redirect("/pr/new")
    
    # GET request - show form
    try:
        with db() as conn:
            departments = conn.execute("""
            SELECT DISTINCT department FROM users WHERE department IS NOT NULL
            """).fetchall()
            
            budget_categories = conn.execute("""
            SELECT DISTINCT category FROM budget_categories
            WHERE department=?
            """, (session.get("department", ""),)).fetchall()
            
            vendors = conn.execute("""
            SELECT vendor_code, vendor_name FROM vendors WHERE is_active=1
            LIMIT 50
            """).fetchall()
        
        return render_template(
            "pr_new_enhanced.html",
            departments=[d['department'] for d in departments],
            budget_categories=[bc['category'] for bc in budget_categories],
            vendors=vendors,
            fiscal_year=datetime.now().year
        )
    except Exception as e:
        print(f"⚠️ Error loading PR form: {e}")
        flash("Error loading form", "danger")
        return redirect("/dashboard")

# ==================================================
# BUDGET EXCEPTION APPROVAL
# ==================================================
@app.route("/pr/<int:pr_id>/budget-exception", methods=["GET", "POST"])
@login_required
@role_required("approver1", "approver2", "approver3", "approver4", "superadmin")
def budget_exception_approval(pr_id):
    """Handle budget exception approval"""
    try:
        with db() as conn:
            pr = conn.execute("""
            SELECT p.*, u.full_name as creator_name
            FROM pr p
            JOIN users u ON p.created_by = u.id
            WHERE p.id=?
            """, (pr_id,)).fetchone()
            
            if not pr or pr['budget_status'] != BUDGET_STATUS['OUT_OF_BUDGET']:
                abort(404, description="PR not found or doesn't require budget exception")
            
            if request.method == "POST":
                action = request.form.get("action")
                comments = request.form.get("comments", "")
                
                if action == "approve":
                    # Update PR status
                    conn.execute("""
                    UPDATE pr SET
                        budget_status=?,
                        budget_exception_approver=?,
                        budget_exception_date=?,
                        budget_exception_notes=?,
                        status='DRAFT',
                        last_updated=?
                    WHERE id=?
                    """, (
                        BUDGET_STATUS['EXCEPTION_APPROVED'],
                        session["user_id"],
                        datetime.now().isoformat(),
                        comments,
                        datetime.now().isoformat(),
                        pr_id
                    ))
                    
                    # Log approval
                    log_approval_action(pr_id, "BUDGET_EXCEPTION_APPROVE", comments)
                    
                    # Create notifications
                    create_notification(
                        pr['created_by'],
                        "Budget Exception Approved",
                        f"Budget exception for PR {pr['pr_no']} has been approved.",
                        "SUCCESS",
                        pr_id
                    )
                    
                    flash("Budget exception approved! PR is now in DRAFT status.", "success")
                    
                elif action == "reject":
                    conn.execute("""
                    UPDATE pr SET
                        budget_status=?,
                        status='REJECTED',
                        rejection_reason=?,
                        last_updated=?
                    WHERE id=?
                    """, (
                        BUDGET_STATUS['OUT_OF_BUDGET'],
                        f"Budget exception rejected: {comments}",
                        datetime.now().isoformat(),
                        pr_id
                    ))
                    
                    log_approval_action(pr_id, "BUDGET_EXCEPTION_REJECT", comments)
                    
                    create_notification(
                        pr['created_by'],
                        "Budget Exception Rejected",
                        f"Budget exception for PR {pr['pr_no']} has been rejected.",
                        "DANGER",
                        pr_id
                    )
                    
                    flash("Budget exception rejected.", "danger")
                
                return redirect("/dashboard")
            
            # GET request - show budget exception details
            items = conn.execute("""
            SELECT * FROM pr_items WHERE pr_id=?
            ORDER BY item_no
            """, (pr_id,)).fetchall()
            
            budget_info = None
            if pr['budget_category']:
                budget_info = conn.execute("""
                SELECT * FROM budget_categories
                WHERE department=? AND category=? AND fiscal_year=?
                """, (pr['department'], pr['budget_category'], pr['fiscal_year'])).fetchone()
        
        return render_template(
            "budget_exception.html",
            pr=pr,
            items=items,
            budget_info=budget_info
        )
        
    except Exception as e:
        print(f"⚠️ Budget exception error: {e}")
        flash("Error loading budget exception page", "danger")
        return redirect("/dashboard")

# ==================================================
# SUBMIT PR FOR APPROVAL
# ==================================================
@app.route("/pr/<int:pr_id>/submit", methods=["GET", "POST"])
@login_required
@role_required("user")
def submit_pr(pr_id):
    try:
        with db() as conn:
            pr = conn.execute("""
            SELECT * FROM pr 
            WHERE id=? AND created_by=? 
            AND status IN ('DRAFT', 'BUDGET_EXCEPTION_APPROVED')
            """, (pr_id, session["user_id"])).fetchone()
            
            if not pr:
                abort(403, description="PR not found or cannot be submitted")
            
            # Determine approval path
            path = get_approval_path(pr['total_amount'], pr['budget_status'])
            
            # Update PR status
            conn.execute("""
            UPDATE pr SET
                status='PENDING_APPROVAL',
                current_approver_role=?,
                last_updated=?
            WHERE id=?
            """, (
                path[0] if path[0] != 'budget_exception' else 'approver1',
                datetime.now().isoformat(),
                pr_id
            ))
            
            # Create notification for first approver
            approvers = conn.execute("""
            SELECT id FROM users WHERE role=? AND active=1
            LIMIT 1
            """, (path[0] if path[0] != 'budget_exception' else 'approver1',)).fetchone()
            
            if approvers:
                create_notification(
                    approvers['id'],
                    "PR Pending Approval",
                    f"PR {pr['pr_no']} requires your approval.",
                    "WARNING",
                    pr_id
                )
            
            # Log submission
            log_approval_action(pr_id, "SUBMIT_FOR_APPROVAL")
            
            flash("PR submitted for approval successfully!", "success")
        
        return redirect("/dashboard")
        
    except Exception as e:
        print(f"⚠️ Submit PR error: {e}")
        flash("Error submitting PR", "danger")
        return redirect("/dashboard")

# ==================================================
# APPROVAL SYSTEM
# ==================================================
@app.route("/approve")
@login_required
@role_required("approver1", "approver2", "approver3", "approver4")
def approve_list():
    try:
        role = session["role"]
        
        with db() as conn:
            # Get PRs pending current approver's approval
            prs = conn.execute("""
            SELECT p.*, 
                   u.full_name as requester_name_full,
                   (SELECT COUNT(*) FROM pr_items WHERE pr_id=p.id) as item_count
            FROM pr p
            JOIN users u ON p.created_by = u.id
            WHERE p.status='PENDING_APPROVAL'
            AND p.current_approver_role=?
            ORDER BY p.created_at DESC
            """, (role,)).fetchall()
            
            # Also get budget exception requests for approver1 and above
            budget_exceptions = []
            if role in ['approver1', 'approver2', 'approver3', 'approver4', 'superadmin']:
                budget_exceptions = conn.execute("""
                SELECT p.*, u.full_name as requester_name_full
                FROM pr p
                JOIN users u ON p.created_by = u.id
                WHERE p.status='BUDGET_EXCEPTION_PENDING'
                ORDER BY p.created_at DESC
                """).fetchall()
        
        return render_template(
            "approve_list_enhanced.html",
            prs=prs,
            budget_exceptions=budget_exceptions,
            role=role
        )
        
    except Exception as e:
        print(f"⚠️ Approve list error: {e}")
        flash("Error loading approvals", "danger")
        return redirect("/dashboard")

@app.route("/approve/<int:pr_id>/<action>", methods=["POST"])
@login_required
@role_required("approver1", "approver2", "approver3", "approver4")
def approve_action(pr_id, action):
    """Handle approval/rejection"""
    comments = request.form.get("comments", "")
    
    try:
        with db() as conn:
            pr = conn.execute("""
            SELECT * FROM pr 
            WHERE id=? AND status='PENDING_APPROVAL' 
            AND current_approver_role=?
            """, (pr_id, session["role"])).fetchone()
            
            if not pr:
                abort(404, description="PR not found or not pending your approval")
            
            if action == "approve":
                # Determine approval path
                path = get_approval_path(pr['total_amount'], pr['budget_status'])
                current_idx = path.index(session["role"]) if session["role"] in path else -1
                
                # Update approver status
                approver_field = f"{session['role']}_status"
                approver_date_field = f"{session['role']}_date"
                approver_notes_field = f"{session['role']}_notes"
                approver_id_field = f"{session['role']}_id"
                
                conn.execute(f"""
                UPDATE pr SET
                    {approver_field}=?,
                    {approver_date_field}=?,
                    {approver_notes_field}=?,
                    {approver_id_field}=?,
                    last_updated=?
                WHERE id=?
                """, (
                    "APPROVED",
                    datetime.now().isoformat(),
                    comments,
                    session["user_id"],
                    datetime.now().isoformat(),
                    pr_id
                ))
                
                # Check if there's next approver
                if current_idx + 1 < len(path):
                    next_role = path[current_idx + 1]
                    conn.execute("""
                    UPDATE pr SET current_approver_role=?
                    WHERE id=?
                    """, (next_role, pr_id))
                    
                    # Notify next approver
                    next_approvers = conn.execute("""
                    SELECT id FROM users WHERE role=? AND active=1
                    LIMIT 1
                    """, (next_role,)).fetchone()
                    
                    if next_approvers:
                        create_notification(
                            next_approvers['id'],
                            "PR Pending Your Approval",
                            f"PR {pr['pr_no']} has been approved by {session['role']} and now requires your approval.",
                            "WARNING",
                            pr_id
                        )
                    
                    flash(f"Approved! Moved to {next_role} for next approval.", "success")
                    
                else:
                    # Final approval
                    conn.execute("""
                    UPDATE pr SET 
                        status='APPROVED',
                        current_approver_role='',
                        last_updated=?
                    WHERE id=?
                    """, (datetime.now().isoformat(), pr_id))
                    
                    # Create notification for procurement
                    procurement_users = conn.execute("""
                    SELECT id FROM users WHERE role='procurement' AND active=1
                    LIMIT 1
                    """).fetchone()
                    
                    if procurement_users:
                        create_notification(
                            procurement_users['id'],
                            "PR Approved - Ready for Procurement",
                            f"PR {pr['pr_no']} has been fully approved and is ready for procurement processing.",
                            "SUCCESS",
                            pr_id
                        )
                    
                    # Notify requester
                    create_notification(
                        pr['created_by'],
                        "PR Fully Approved",
                        f"Your PR {pr['pr_no']} has been fully approved!",
                        "SUCCESS",
                        pr_id
                    )
                    
                    flash("PR fully approved! Sent to procurement.", "success")
                
                # Log approval action
                log_approval_action(pr_id, "APPROVE", comments)
                
            elif action == "reject":
                # Reject the PR
                conn.execute("""
                UPDATE pr SET
                    status='REJECTED',
                    current_approver_role='',
                    rejection_reason=?,
                    last_updated=?
                WHERE id=?
                """, (
                    f"Rejected by {session['role']}: {comments}",
                    datetime.now().isoformat(),
                    pr_id
                ))
                
                # Notify requester
                create_notification(
                    pr['created_by'],
                    "PR Rejected",
                    f"Your PR {pr['pr_no']} has been rejected by {session['role']}.",
                    "DANGER",
                    pr_id
                )
                
                # Log rejection
                log_approval_action(pr_id, "REJECT", comments)
                
                flash("PR rejected.", "danger")
            
            elif action == "return":
                # Return to requester for revision
                conn.execute("""
                UPDATE pr SET
                    status='DRAFT',
                    current_approver_role='',
                    rejection_reason=?,
                    last_updated=?
                WHERE id=?
                """, (
                    f"Returned by {session['role']} for revision: {comments}",
                    datetime.now().isoformat(),
                    pr_id
                ))
                
                # Notify requester
                create_notification(
                    pr['created_by'],
                    "PR Returned for Revision",
                    f"Your PR {pr['pr_no']} has been returned for revision by {session['role']}.",
                    "WARNING",
                    pr_id
                )
                
                # Log return action
                log_approval_action(pr_id, "RETURN", comments)
                
                flash("PR returned to requester for revision.", "warning")
        
        return redirect("/approve")
        
    except Exception as e:
        print(f"⚠️ Approval action error: {e}")
        flash("Error processing approval", "danger")
        return redirect("/approve")

# ==================================================
# VIEW PR DETAILS
# ==================================================
@app.route("/pr/<int:pr_id>")
@login_required
def view_pr(pr_id):
    try:
        with db() as conn:
            # Get PR details
            pr = conn.execute("""
            SELECT p.*, 
                   u.full_name as creator_full_name,
                   u.department as creator_dept,
                   (SELECT COUNT(*) FROM pr_items WHERE pr_id=p.id) as item_count
            FROM pr p
            JOIN users u ON p.created_by = u.id
            WHERE p.id=?
            """, (pr_id,)).fetchone()
            
            if not pr:
                abort(404, description="PR not found")
            
            # Check permissions
            if session["role"] == "user" and pr["created_by"] != session["user_id"]:
                abort(403, description="You can only view your own PRs")
            
            # Get items
            items = conn.execute("""
            SELECT * FROM pr_items WHERE pr_id=?
            ORDER BY item_no
            """, (pr_id,)).fetchall()
            
            # Get approval history
            history = conn.execute("""
            SELECT * FROM approval_history 
            WHERE pr_id=?
            ORDER BY action_date DESC
            """, (pr_id,)).fetchall()
            
            # Get budget info if applicable
            budget_info = None
            if pr['budget_category']:
                budget_info = conn.execute("""
                SELECT * FROM budget_categories
                WHERE department=? AND category=? AND fiscal_year=?
                """, (pr['department'], pr['budget_category'], pr['fiscal_year'])).fetchone()
        
        return render_template(
            "view_pr.html",
            pr=pr,
            items=items,
            history=history,
            budget_info=budget_info,
            APPROVAL_THRESHOLDS=APPROVAL_THRESHOLDS,
            BUDGET_STATUS=BUDGET_STATUS
        )
        
    except Exception as e:
        print(f"⚠️ View PR error: {e}")
        flash("Error loading PR details", "danger")
        return redirect("/dashboard")

# ==================================================
# PROCUREMENT
# ==================================================
@app.route("/procurement")
@login_required
@role_required("procurement")
def procurement():
    try:
        with db() as conn:
            # Get approved PRs
            prs = conn.execute("""
            SELECT p.*, 
                   u.full_name as requester_name_full,
                   u.department as requester_dept,
                   (SELECT COUNT(*) FROM pr_items WHERE pr_id=p.id) as item_count
            FROM pr p
            JOIN users u ON p.created_by = u.id
            WHERE p.status='APPROVED'
            ORDER BY p.created_at DESC
            """).fetchall()
            
            # Get PRs received by procurement
            received_prs = conn.execute("""
            SELECT p.*, 
                   u.full_name as requester_name_full,
                   (SELECT COUNT(*) FROM pr_items WHERE pr_id=p.id) as item_count
            FROM pr p
            JOIN users u ON p.created_by = u.id
            WHERE p.procurement_received_date IS NOT NULL
            ORDER BY p.procurement_received_date DESC
            LIMIT 10
            """).fetchall()
            
            # Procurement stats
            stats = {
                'total_approved': len(prs),
                'received_count': len(received_prs),
                'pending_receipt': len(prs) - len(received_prs)
            }
        
        return render_template(
            "procurement_enhanced.html",
            prs=prs,
            received_prs=received_prs,
            stats=stats
        )
        
    except Exception as e:
        print(f"⚠️ Procurement error: {e}")
        flash("Error loading procurement page", "danger")
        return redirect("/dashboard")

@app.route("/procurement/receive/<int:pr_id>", methods=["POST"])
@login_required
@role_required("procurement")
def receive_pr(pr_id):
    """Mark PR as received by procurement"""
    try:
        with db() as conn:
            # Verify PR is approved
            pr = conn.execute("""
            SELECT * FROM pr WHERE id=? AND status='APPROVED'
            """, (pr_id,)).fetchone()
            
            if not pr:
                abort(404, description="PR not found or not approved")
            
            # Update as received
            conn.execute("""
            UPDATE pr SET
                procurement_received_date=?,
                procurement_officer_id=?,
                last_updated=?
            WHERE id=?
            """, (
                datetime.now().isoformat(),
                session["user_id"],
                datetime.now().isoformat(),
                pr_id
            ))
            
            # Create notification for requester
            create_notification(
                pr['created_by'],
                "PR Received by Procurement",
                f"Your PR {pr['pr_no']} has been received by the procurement department.",
                "INFO",
                pr_id
            )
            
            flash(f"PR {pr['pr_no']} marked as received by procurement.", "success")
        
        return redirect("/procurement")
        
    except Exception as e:
        print(f"⚠️ Receive PR error: {e}")
        flash("Error receiving PR", "danger")
        return redirect("/procurement")

# ==================================================
# NOTIFICATION SYSTEM
# ==================================================
@app.route("/notifications")
@login_required
def notifications():
    try:
        with db() as conn:
            notifications = conn.execute("""
            SELECT n.*, p.pr_no
            FROM notifications n
            LEFT JOIN pr p ON n.related_pr_id = p.id
            WHERE n.user_id=?
            ORDER BY n.created_at DESC
            LIMIT 50
            """, (session["user_id"],)).fetchall()
        
        return render_template("notifications.html", notifications=notifications)
        
    except Exception as e:
        print(f"⚠️ Notifications error: {e}")
        flash("Error loading notifications", "danger")
        return redirect("/dashboard")

@app.route("/notifications/mark-read/<int:notification_id>", methods=["POST"])
@login_required
def mark_notification_read(notification_id):
    """Mark a notification as read"""
    try:
        with db() as conn:
            conn.execute("""
            UPDATE notifications 
            SET is_read=1 
            WHERE id=? AND user_id=?
            """, (notification_id, session["user_id"]))
        
        return jsonify({"success": True})
        
    except Exception as e:
        print(f"⚠️ Mark notification read error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/notifications/mark-all-read", methods=["POST"])
@login_required
def mark_all_notifications_read():
    """Mark all notifications as read"""
    try:
        with db() as conn:
            conn.execute("""
            UPDATE notifications 
            SET is_read=1 
            WHERE user_id=? AND is_read=0
            """, (session["user_id"],))
        
        flash("All notifications marked as read", "success")
        return redirect("/dashboard")
        
    except Exception as e:
        print(f"⚠️ Mark all notifications read error: {e}")
        flash("Error marking notifications as read", "danger")
        return redirect("/dashboard")

# ==================================================
# VENDOR MANAGEMENT - FULL IMPLEMENTATION
# ==================================================
@app.route("/vendors")
@login_required
@role_required("superadmin", "procurement")
def vendor_list():
    try:
        with db() as conn:
            vendors = conn.execute("""
            SELECT * FROM vendors 
            ORDER BY vendor_name
            """).fetchall()
        
        return render_template("vendors.html", vendors=vendors)
        
    except Exception as e:
        print(f"⚠️ Vendor list error: {e}")
        flash("Error loading vendors", "danger")
        return redirect("/dashboard")

@app.route("/vendors/new", methods=["GET", "POST"])
@login_required
@role_required("superadmin", "procurement")
def new_vendor():
    """Legacy vendor form - redirect to new procurement form"""
    return redirect(url_for("procurement_vendor_form"))

@app.route("/vendors/edit/<string:vendor_code>", methods=["GET", "POST"])
@login_required
@role_required("superadmin", "procurement")
def edit_vendor(vendor_code):
    """Edit existing vendor details"""
    try:
        with db() as conn:
            vendor = conn.execute("""
            SELECT * FROM vendors WHERE vendor_code=?
            """, (vendor_code,)).fetchone()
            
            if not vendor:
                flash("Vendor not found", "danger")
                return redirect("/vendors")
            
            if request.method == "POST":
                # Parse existing notes
                notes_data = {}
                if vendor['notes']:
                    try:
                        notes_data = json.loads(vendor['notes'])
                    except:
                        pass
                
                # Update notes with form data
                notes_data.update({
                    "company_registration_no": request.form.get("company_registration_no"),
                    "sst_reg_no": request.form.get("sst_reg_no"),
                    "tin_no": request.form.get("tin_no"),
                    "msic_no": request.form.get("msic_no"),
                    "goods_services": request.form.get("goods_services_details")
                })
                
                # Format address
                full_address = ", ".join(filter(None, [
                    request.form.get("address"),
                    request.form.get("state"),
                    request.form.get("country"),
                    request.form.get("postal_code")
                ]))
                
                # Update vendor
                conn.execute("""
                UPDATE vendors SET
                    vendor_name=?,
                    vendor_type=?,
                    tax_id=?,
                    address=?,
                    contact_person=?,
                    contact_email=?,
                    contact_phone=?,
                    bank_name=?,
                    bank_account=?,
                    bank_address=?,
                    bank_code=?,
                    swift_code=?,
                    payment_terms=?,
                    fax_no=?,
                    incoterms=?,
                    order_currency=?,
                    year_established=?,
                    created_status=?,
                    is_active=?,
                    notes=?
                WHERE vendor_code=?
                """, (
                    request.form.get("vendor_name"),
                    request.form.get("vendor_type", "Supplier"),
                    request.form.get("tax_id"),
                    full_address,
                    request.form.get("contact_person_sales"),
                    request.form.get("contact_email_sales"),
                    request.form.get("contact_phone_sales"),
                    request.form.get("bank_name"),
                    request.form.get("bank_account"),
                    request.form.get("bank_address"),
                    request.form.get("bank_code"),
                    request.form.get("swift_code"),
                    request.form.get("payment_terms", "NET30"),
                    request.form.get("fax_no"),
                    request.form.get("incoterms"),
                    request.form.get("order_currency", "MYR"),
                    request.form.get("year_established"),
                    request.form.get("created_status", "Amendment"),
                    1 if request.form.get("is_active") == "on" else 0,
                    json.dumps(notes_data),
                    vendor_code
                ))
                
                # Create notification
                create_notification(
                    session["user_id"],
                    "Vendor Updated",
                    f"Vendor {request.form['vendor_name']} ({vendor_code}) has been updated",
                    "INFO"
                )
                
                flash(f"Vendor {vendor_code} updated successfully", "success")
                return redirect("/vendors")
            
            # Parse notes for display
            notes = {}
            if vendor['notes']:
                try:
                    notes = json.loads(vendor['notes'])
                except:
                    pass
            
            return render_template(
                "procurement_vendor_edit.html",
                vendor=vendor,
                notes=notes,
                current_year=datetime.now().year
            )
            
    except Exception as e:
        print(f"⚠️ Edit vendor error: {e}")
        flash("Error editing vendor", "danger")
        return redirect("/vendors")

@app.route("/vendors/view/<string:vendor_code>")
@login_required
@role_required("superadmin", "procurement", "user", "approver1", "approver2", "approver3", "approver4")
def view_vendor(vendor_code):
    """View vendor details"""
    try:
        with db() as conn:
            vendor = conn.execute("""
            SELECT * FROM vendors WHERE vendor_code=?
            """, (vendor_code,)).fetchone()
            
            if not vendor:
                flash("Vendor not found", "danger")
                return redirect("/vendors")
            
            # Parse notes
            notes = {}
            if vendor['notes']:
                try:
                    notes = json.loads(vendor['notes'])
                except:
                    pass
            
            return render_template(
                "vendor_view.html",
                vendor=vendor,
                notes=notes
            )
            
    except Exception as e:
        print(f"⚠️ View vendor error: {e}")
        flash("Error loading vendor details", "danger")
        return redirect("/vendors")

@app.route("/vendors/delete/<string:vendor_code>", methods=["POST"])
@login_required
@role_required("superadmin")
def delete_vendor(vendor_code):
    """Delete a vendor (superadmin only)"""
    try:
        with db() as conn:
            # Check if vendor is used in any PR
            pr_count = conn.execute("""
            SELECT COUNT(*) FROM pr WHERE vendor_code=?
            """, (vendor_code,)).fetchone()[0]
            
            if pr_count > 0:
                flash(f"Cannot delete vendor {vendor_code} - used in {pr_count} PR(s). Deactivate instead.", "warning")
                return redirect("/vendors")
            
            # Delete vendor
            conn.execute("DELETE FROM vendors WHERE vendor_code=?", (vendor_code,))
            
            flash(f"Vendor {vendor_code} deleted successfully", "success")
            
        return redirect("/vendors")
        
    except Exception as e:
        print(f"⚠️ Delete vendor error: {e}")
        flash("Error deleting vendor", "danger")
        return redirect("/vendors")

# ==================================================
# PROCUREMENT VENDOR REGISTRATION FORM - MAIN IMPLEMENTATION
# ==================================================
@app.route("/procurement/vendor/new", methods=["GET", "POST"])
@login_required
@role_required("procurement", "superadmin")
def procurement_vendor_form():
    """
    Form untuk procurement daftar vendor baru
    Sesuai dengan Excel Vendor Registration Form
    """
    if request.method == "POST":
        try:
            # Validate required fields
            required_fields = ['vendor_code', 'vendor_name', 'company_registration_no']
            for field in required_fields:
                if not request.form.get(field):
                    flash(f"{field.replace('_', ' ').title()} is required", "danger")
                    return redirect("/procurement/vendor/new")
            
            # Check if vendor code already exists
            with db() as conn:
                existing = conn.execute("""
                SELECT vendor_code FROM vendors WHERE vendor_code=?
                """, (request.form["vendor_code"],)).fetchone()
                
                if existing:
                    flash(f"Vendor code {request.form['vendor_code']} already exists", "danger")
                    return redirect("/procurement/vendor/new")
            
            # Format full address
            full_address = ", ".join(filter(None, [
                request.form.get("address"),
                request.form.get("state"),
                request.form.get("country"),
                request.form.get("postal_code")
            ]))
            
            # Insert vendor dengan kolom sesuai Excel
            with db() as conn:
                conn.execute("""
                INSERT INTO vendors (
                    vendor_code, vendor_name, vendor_type,
                    registration_date, tax_id, address,
                    contact_person, contact_email, contact_phone,
                    bank_name, bank_account, bank_address,
                    bank_code, swift_code, payment_terms,
                    fax_no, incoterms, order_currency,
                    year_established, created_status,
                    is_active, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    request.form["vendor_code"],
                    request.form["vendor_name"],
                    request.form.get("vendor_type", "Supplier"),
                    request.form.get("registration_date"),
                    request.form.get("tax_id"),
                    full_address,  # Full formatted address
                    
                    # Contact person
                    request.form.get("contact_person_sales"),
                    request.form.get("contact_email_sales"),
                    request.form.get("contact_phone_sales"),
                    
                    # Bank info
                    request.form.get("bank_name"),
                    request.form.get("bank_account"),
                    request.form.get("bank_address"),
                    request.form.get("bank_code"),
                    request.form.get("swift_code"),
                    
                    # Payment terms
                    request.form.get("payment_terms", "NET30"),
                    
                    # Additional fields
                    request.form.get("fax_no"),
                    request.form.get("incoterms"),
                    request.form.get("order_currency", "MYR"),
                    request.form.get("year_established"),
                    request.form.get("created_status", "New Vendor"),
                    
                    # Status & notes
                    1,  # is_active
                    json.dumps({
                        "company_registration_no": request.form.get("company_registration_no"),
                        "sst_reg_no": request.form.get("sst_reg_no"),
                        "tin_no": request.form.get("tin_no"),
                        "msic_no": request.form.get("msic_no"),
                        "goods_services": request.form.get("goods_services_details")
                    })
                ))
            
            # Create notification
            create_notification(
                session["user_id"],
                "New Vendor Registered",
                f"Vendor {request.form['vendor_name']} ({request.form['vendor_code']}) has been registered",
                "SUCCESS"
            )
            
            flash(f"Vendor {request.form['vendor_name']} registered successfully!", "success")
            return redirect("/vendors")
            
        except sqlite3.IntegrityError as e:
            print(f"⚠️ Integrity error: {e}")
            flash("Vendor code already exists or data validation failed", "danger")
            return redirect("/procurement/vendor/new")
            
        except Exception as e:
            print(f"⚠️ Vendor registration error: {e}")
            flash(f"Error registering vendor: {str(e)}", "danger")
            return redirect("/procurement/vendor/new")
    
    # GET request - show form
    return render_template(
        "procurement_vendor_form.html",
        current_year=datetime.now().year
    )

# ==================================================
# VENDOR API ENDPOINTS
# ==================================================
@app.route("/api/vendors/search")
@login_required
def search_vendors():
    """Search vendors for autocomplete"""
    query = request.args.get("q", "")
    
    try:
        with db() as conn:
            vendors = conn.execute("""
            SELECT vendor_code, vendor_name 
            FROM vendors 
            WHERE (vendor_code LIKE ? OR vendor_name LIKE ?) 
            AND is_active=1
            LIMIT 20
            """, (f"%{query}%", f"%{query}%")).fetchall()
        
        return jsonify([dict(v) for v in vendors])
    except Exception as e:
        return jsonify([])

@app.route("/api/vendors/<string:vendor_code>")
@login_required
def get_vendor_details(vendor_code):
    """Get vendor details for PR form"""
    try:
        with db() as conn:
            vendor = conn.execute("""
            SELECT vendor_code, vendor_name, address, 
                   contact_person, contact_email, contact_phone,
                   payment_terms
            FROM vendors 
            WHERE vendor_code=? AND is_active=1
            """, (vendor_code,)).fetchone()
            
            if not vendor:
                return jsonify({"success": False, "error": "Vendor not found"})
            
            return jsonify({"success": True, "vendor": dict(vendor)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

# ==================================================
# USER MANAGEMENT (SUPER ADMIN) - CRUD LENGKAP
# ==================================================
@app.route("/admin/users", methods=["GET", "POST"])
@login_required
@role_required("superadmin")
def manage_users():
    if request.method == "POST":
        try:
            with db() as conn:
                conn.execute("""
                INSERT INTO users (
                    username, password_hash, full_name, 
                    email, department, role, 
                    approval_limit, active, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
                """, (
                    request.form["username"],
                    generate_password_hash(request.form["password"]),
                    request.form["full_name"],
                    request.form.get("email"),
                    request.form.get("department"),
                    request.form["role"],
                    float(request.form.get("approval_limit", 0)),
                    datetime.now().isoformat()
                ))
            flash("User created successfully!", "success")
            return redirect("/admin/users")
        except sqlite3.IntegrityError:
            flash("Username already exists!", "danger")
        except Exception as e:
            print(f"⚠️ Create user error: {e}")
            flash("Error creating user", "danger")
    
    try:
        with db() as conn:
            users = conn.execute("SELECT * FROM users ORDER BY id").fetchall()
        
        return render_template("users.html", users=users)
    except Exception as e:
        print(f"⚠️ Manage users error: {e}")
        flash("Error loading users", "danger")
        return redirect("/dashboard")

# ==================================================
# USER API ROUTES (CRUD OPERATIONS)
# ==================================================
@app.route("/api/users/<int:user_id>", methods=["GET"])
@login_required
@role_required("superadmin")
def get_user(user_id):
    """Get user details"""
    try:
        with db() as conn:
            user = conn.execute("""
            SELECT id, username, full_name, email, department, 
                   role, approval_limit, active, created_at, last_login
            FROM users WHERE id=?
            """, (user_id,)).fetchone()
            
            if not user:
                return jsonify({"success": False, "error": "User not found"}), 404
            
            user_dict = dict(user)
            return jsonify({"success": True, "user": user_dict})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/users/<int:user_id>", methods=["PUT"])
@login_required
@role_required("superadmin")
def update_user(user_id):
    """Update user details"""
    try:
        data = request.get_json()
        
        # Validate data
        if not data:
            return jsonify({"success": False, "error": "No data provided"}), 400
        
        with db() as conn:
            conn.execute("""
            UPDATE users SET
                full_name=?,
                email=?,
                department=?,
                role=?,
                approval_limit=?
            WHERE id=?
            """, (
                data.get("full_name"),
                data.get("email"),
                data.get("department"),
                data.get("role"),
                float(data.get("approval_limit", 0)),
                user_id
            ))
        
        return jsonify({"success": True, "message": "User updated successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/users/<int:user_id>", methods=["DELETE"])
@login_required
@role_required("superadmin")
def delete_user(user_id):
    """Delete a user account"""
    try:
        # Cannot delete yourself
        if user_id == session["user_id"]:
            return jsonify({"success": False, "error": "Cannot delete yourself"}), 400
        
        with db() as conn:
            # Check if user exists
            user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
            if not user:
                return jsonify({"success": False, "error": "User not found"}), 404
            
            # Delete user
            conn.execute("DELETE FROM users WHERE id=?", (user_id,))
        
        return jsonify({"success": True, "message": "User deleted successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/users/<int:user_id>/activate", methods=["POST"])
@login_required
@role_required("superadmin")
def activate_user(user_id):
    """Activate a user account"""
    try:
        with db() as conn:
            conn.execute("""
            UPDATE users SET active=1 WHERE id=?
            """, (user_id,))
        
        return jsonify({"success": True, "message": "User activated successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/users/<int:user_id>/deactivate", methods=["POST"])
@login_required
@role_required("superadmin")
def deactivate_user(user_id):
    """Deactivate a user account"""
    try:
        # Cannot deactivate yourself
        if user_id == session["user_id"]:
            return jsonify({"success": False, "error": "Cannot deactivate yourself"}), 400
        
        with db() as conn:
            conn.execute("""
            UPDATE users SET active=0 WHERE id=?
            """, (user_id,))
        
        return jsonify({"success": True, "message": "User deactivated successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/users/<int:user_id>/reset-password", methods=["POST"])
@login_required
@role_required("superadmin")
def reset_user_password(user_id):
    """Reset user password"""
    try:
        data = request.get_json()
        new_password = data.get("new_password")
        
        if not new_password:
            return jsonify({"success": False, "error": "New password required"}), 400
        
        if len(new_password) < 6:
            return jsonify({"success": False, "error": "Password must be at least 6 characters"}), 400
        
        with db() as conn:
            conn.execute("""
            UPDATE users SET password_hash=?
            WHERE id=?
            """, (
                generate_password_hash(new_password),
                user_id
            ))
        
        return jsonify({"success": True, "message": "Password reset successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/profile/update", methods=["POST"])
@login_required
def update_profile():
    """Update current user's profile"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"success": False, "error": "No data provided"}), 400
        
        with db() as conn:
            conn.execute("""
            UPDATE users SET
                full_name=?,
                email=?,
                department=?
            WHERE id=?
            """, (
                data.get("full_name"),
                data.get("email"),
                data.get("department"),
                session["user_id"]
            ))
        
        # Update session
        session["name"] = data.get("full_name", session["name"])
        if data.get("department"):
            session["department"] = data.get("department")
        
        return jsonify({"success": True, "message": "Profile updated successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/profile/change-password", methods=["POST"])
@login_required
def change_password():
    """Change current user's password"""
    try:
        data = request.get_json()
        current_password = data.get("current_password")
        new_password = data.get("new_password")
        
        if not current_password or not new_password:
            return jsonify({"success": False, "error": "Both passwords are required"}), 400
        
        if len(new_password) < 6:
            return jsonify({"success": False, "error": "New password must be at least 6 characters"}), 400
        
        with db() as conn:
            # Verify current password
            user = conn.execute("""
            SELECT password_hash FROM users WHERE id=?
            """, (session["user_id"],)).fetchone()
            
            if not user or not check_password_hash(user["password_hash"], current_password):
                return jsonify({"success": False, "error": "Current password is incorrect"}), 400
            
            # Update password
            conn.execute("""
            UPDATE users SET password_hash=?
            WHERE id=?
            """, (
                generate_password_hash(new_password),
                session["user_id"]
            ))
        
        return jsonify({"success": True, "message": "Password changed successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ==================================================
# INITIAL DATA POPULATION
# ==================================================
def create_initial_users():
    """Create initial users for testing"""
    try:
        with db() as conn:
            # Check if users already exist
            existing = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            
            if existing == 0:
                users = [
                    # Super Admin
                    ("admin", "Admin User", "admin@company.com", "superadmin", "IT", 0),
                    # Regular Users
                    ("user1", "John Doe", "john@company.com", "user", "IT", 0),
                    ("user2", "Jane Smith", "jane@company.com", "user", "HR", 0),
                    ("user3", "Bob Johnson", "bob@company.com", "user", "Finance", 0),
                    # Approvers
                    ("approver1", "Director IT", "director.it@company.com", "approver1", "IT", 10000),
                    ("approver2", "Group CFO", "cfo@company.com", "approver2", "Finance", 50000),
                    ("approver3", "Group CEO", "ceo@company.com", "approver3", "Executive", 100000),
                    ("approver4", "Group MD", "md@company.com", "approver4", "Executive", 200000),
                    # Procurement
                    ("procurement1", "Procurement Officer", "procurement@company.com", "procurement", "Procurement", 0),
                ]
                
                for username, full_name, email, role, department, approval_limit in users:
                    password = f"{username}123"
                    conn.execute("""
                    INSERT INTO users (username, password_hash, full_name, email, role, department, approval_limit, active, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
                    """, (
                        username,
                        generate_password_hash(password),
                        full_name,
                        email,
                        role,
                        department,
                        approval_limit,
                        datetime.now().isoformat()
                    ))
                
                # Create sample vendors
                sample_vendors = [
                    ("V001", "Tech Supplies Sdn Bhd", "Supplier", "2020-01-15", 
                     "123456789012", "123 Tech Street, KL", "Ali", "ali@techsupplies.com", 
                     "03-12345678", "Maybank", "1234567890", "Maybank HQ", "MBBEMYKL", 
                     "MBBEMYKLXXX", "NET30", "03-12345679", "FOB", "MYR", "2015"),
                    ("V002", "Office Mart Bhd", "Supplier", "2019-05-20",
                     "987654321098", "456 Office Ave, PJ", "Siti", "siti@officemart.com",
                     "03-98765432", "CIMB", "0987654321", "CIMB PJ", "CIBBMYKL",
                     "CIBBMYKLXXX", "NET45", "03-98765433", "EXW", "MYR", "2010"),
                ]
                
                for vcode, vname, vtype, regdate, taxid, addr, contact, email, phone, 
                    bank, baccount, baddr, bcode, swift, pterms, fax, incoterms, currency, year in sample_vendors:
                    
                    conn.execute("""
                    INSERT OR IGNORE INTO vendors (
                        vendor_code, vendor_name, vendor_type, registration_date,
                        tax_id, address, contact_person, contact_email, contact_phone,
                        bank_name, bank_account, bank_address, bank_code, swift_code,
                        payment_terms, fax_no, incoterms, order_currency, year_established,
                        is_active, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        vcode, vname, vtype, regdate, taxid, addr, contact, email, phone,
                        bank, baccount, baddr, bcode, swift, pterms, fax, incoterms, currency, year,
                        1, json.dumps({"company_registration_no": f"COMP-{vcode}"})
                    ))
                
                print("✅ Initial users and vendors created")
                print("👉 Test credentials:")
                for username, _, _, _, _, _ in users:
                    print(f"   {username} / {username}123")
    except Exception as e:
        print(f"⚠️ Error creating initial data: {e}")

# ==================================================
# ERROR HANDLERS
# ==================================================
@app.errorhandler(403)
def forbidden(error):
    return render_template("error.html", 
                         error_code=403,
                         error_message="Access Forbidden",
                         error_description="You don't have permission to access this page.",
                         datetime=datetime), 403

@app.errorhandler(404)
def not_found(error):
    return render_template("error.html",
                         error_code=404,
                         error_message="Page Not Found",
                         error_description="The page you're looking for doesn't exist.",
                         datetime=datetime), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template("error.html",
                         error_code=500,
                         error_message="Internal Server Error",
                         error_description="Something went wrong on our end.",
                         datetime=datetime), 500

# ==================================================
# APPLICATION SHUTDOWN HANDLER
# ==================================================
@app.teardown_appcontext
def teardown_db(exception=None):
    """Close database connections on app shutdown"""
    close_db_connections()

# ==================================================
# RUN APPLICATION
# ==================================================
if __name__ == "__main__":
    try:
        init_db()
        create_initial_users()

        app.run(
            debug=True,
            host="0.0.0.0",
            port=5000,
            threaded=True
        )
    except Exception as e:
        print(f"❌ Failed to start application: {e}")
        close_db_connections()

# ==================================================
# RENDER / GUNICORN SAFE STARTUP
# ==================================================
@app.before_request
def _render_startup_guard():
    """
    Initialize DB once per process (Render/Gunicorn safe)
    """
    if not getattr(app, "_db_initialized", False):
        init_db()
        create_initial_users()
        app._db_initialized = True