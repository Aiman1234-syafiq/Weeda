import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime
import json
from functools import wraps
from contextlib import contextmanager
from werkzeug.utils import secure_filename

from flask import (
    Flask, render_template, request, redirect,
    url_for, session, abort, flash, jsonify,
    send_from_directory
)
from werkzeug.security import generate_password_hash, check_password_hash

# ==================================================
# BASIC CONFIG
# ==================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "pr_enterprise.db"))

# File upload config
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
QUOTATION_FOLDER = os.path.join(UPLOAD_FOLDER, 'quotations')
ALLOWED_EXTENSIONS = {'pdf', 'jpg', 'jpeg', 'png', 'doc', 'docx'}
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB

# Create upload folders if not exist
os.makedirs(QUOTATION_FOLDER, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get(
    "SECRET_KEY",
    "change-this-in-production-32-char-secret"
)

# Upload config
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

# SESSION CONFIG (DEV SAFE)
app.config['SESSION_COOKIE_SECURE'] = os.environ.get("SESSION_COOKIE_SECURE", "false").lower() == "true"
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['PERMANENT_SESSION_LIFETIME'] = 1800

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
def migrate_po_table():
    """
    Create PO table for tracking PO numbers from procurement
    """
    try:
        with db() as conn:
            # Create PO table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS po (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pr_id INTEGER NOT NULL UNIQUE,
                po_no TEXT NOT NULL UNIQUE,
                po_date TEXT NOT NULL,
                vendor_name TEXT,
                total_amount REAL,
                created_by INTEGER,
                created_at TEXT,
                status TEXT DEFAULT 'ACTIVE',
                notes TEXT,
                FOREIGN KEY (pr_id) REFERENCES pr(id) ON DELETE CASCADE
            )
            """)
            
            # Create index untuk performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_po_pr_id ON po(pr_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_po_number ON po(po_no)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_po_date ON po(po_date)")
            
            print("✅ PO table created successfully")
            
    except Exception as e:
        print(f"⚠️ PO table migration error: {e}")

def migrate_quotation_table():
    """
    Create quotation table for storing uploaded quotation files
    """
    try:
        with db() as conn:
            # Create quotation table
            conn.execute("""
            CREATE TABLE IF NOT EXISTS pr_quotation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pr_id INTEGER NOT NULL,
                filename TEXT NOT NULL,
                file_path TEXT NOT NULL,
                uploaded_by INTEGER NOT NULL,
                uploaded_at TEXT NOT NULL,
                file_size INTEGER,
                mime_type TEXT,
                FOREIGN KEY (pr_id) REFERENCES pr(id) ON DELETE CASCADE,
                FOREIGN KEY (uploaded_by) REFERENCES users(id)
            )
            """)
            
            # Create index
            conn.execute("CREATE INDEX IF NOT EXISTS idx_quotation_pr_id ON pr_quotation(pr_id)")
            
            print("✅ Quotation table created successfully")
            
    except Exception as e:
        print(f"⚠️ Quotation table migration error: {e}")

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

def migrate_pr_columns():
    """
    Migration untuk column quotation_filename dan quotation_uploaded_at
    """
    try:
        with db() as conn:
            # Check if columns already exist
            try:
                conn.execute("""
                ALTER TABLE pr ADD COLUMN quotation_filename TEXT
                """)
                print("✅ Added column: quotation_filename")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e):
                    raise
            
            try:
                conn.execute("""
                ALTER TABLE pr ADD COLUMN quotation_uploaded_at TEXT
                """)
                print("✅ Added column: quotation_uploaded_at")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e):
                    raise
                    
    except Exception as e:
        print(f"⚠️ PR columns migration error: {e}")

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
        
        # PR - DIPERMUDAHKAN (NO APPROVAL SYSTEM)
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
            
            -- Status - DIPERMUDAHKAN
            status TEXT DEFAULT 'SUBMITTED',
            
            -- Procurement (optional)
            procurement_received_date TEXT,
            procurement_officer_id INTEGER,
            
            -- Audit Trail
            last_updated TEXT NOT NULL,
            
            -- Quotation (link to uploaded file)
            quotation_filename TEXT,
            quotation_uploaded_at TEXT,
            
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
        
        # APPROVAL HISTORY - DISIMPAN TAPI TAK DIGUNAKAN
        conn.execute("""
        CREATE TABLE IF NOT EXISTS approval_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pr_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            action_date TEXT NOT NULL,
            comments TEXT,
            ip_address TEXT,
            user_agent TEXT,
            FOREIGN KEY (pr_id) REFERENCES pr(id)
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
        
        # AUDIT LOG - untuk production tracking
        conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            user_id INTEGER,
            action TEXT NOT NULL,
            entity_type TEXT,
            entity_id INTEGER,
            ip_address TEXT,
            user_agent TEXT,
            details TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
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
        migrate_po_table()
        migrate_quotation_table()
        
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
        raise

# ==================================================
# GLOBAL TEMPLATE CONTEXT
# ==================================================
@app.context_processor
def inject_globals():
    """Inject global variables into all templates"""
    return {
        'datetime': datetime,
        'now': datetime.now,
        'current_year': datetime.now().year,
        'format_datetime': lambda dt: datetime.fromisoformat(dt).strftime("%d %b %Y %H:%M") if dt else "N/A"
    }

# ==================================================
# CONSTANTS - DIPERMUDAHKAN
# ==================================================
BUDGET_STATUS = {
    'IN_BUDGET': 'IN_BUDGET',
    'OUT_OF_BUDGET': 'OUT_OF_BUDGET',
    'EXCEPTION_APPROVED': 'EXCEPTION_APPROVED',
    'EXCEPTION_PENDING': 'BUDGET_EXCEPTION_PENDING'
}

PR_STATUS = {
    'SUBMITTED': 'SUBMITTED',        # User create PR
    'PO_CREATED': 'PO_CREATED',      # Procurement isi PO
    'CLOSED': 'CLOSED',              # Selesai
    'REJECTED': 'REJECTED'           # Ditolak
}

# ==================================================
# HELPER FUNCTIONS
# ==================================================
def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def save_quotation_file(file, pr_id):
    """Save quotation file and return filename"""
    if not file or file.filename == '':
        return None
    
    if not allowed_file(file.filename):
        raise ValueError("File type not allowed. Allowed: PDF, JPG, PNG, DOC, DOCX")
    
    # Generate unique filename with pr_id
    original_filename = secure_filename(file.filename)
    file_ext = original_filename.rsplit('.', 1)[1].lower()
    unique_filename = f"quotation_{pr_id}_{uuid.uuid4().hex[:8]}.{file_ext}"
    
    # Save file
    file_path = os.path.join(QUOTATION_FOLDER, unique_filename)
    file.save(file_path)
    
    return unique_filename

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

def log_action(pr_id, action, comments='', user_id=None):
    """Log setiap action"""
    try:
        with db() as conn:
            conn.execute("""
            INSERT INTO approval_history 
            (pr_id, action, action_date, comments, ip_address, user_agent)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (
                pr_id,
                action,
                datetime.now().isoformat(),
                comments,
                request.remote_addr if request else 'N/A',
                request.headers.get('User-Agent') if request else 'N/A'
            ))
    except Exception as e:
        print(f"⚠️ Failed to log action: {e}")

def audit_log(user_id, action, entity_type=None, entity_id=None, details=None):
    """Create audit log for production tracking"""
    try:
        with db() as conn:
            conn.execute("""
            INSERT INTO audit_log 
            (timestamp, user_id, action, entity_type, entity_id, ip_address, user_agent, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now().isoformat(),
                user_id,
                action,
                entity_type,
                entity_id,
                request.remote_addr if request else 'N/A',
                request.headers.get('User-Agent') if request else 'N/A',
                json.dumps(details) if details else None
            ))
    except Exception as e:
        print(f"⚠️ Failed to create audit log: {e}")

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
                    session["email"] = user["email"] or ""
                    session["last_login"] = user["last_login"] or ""

                    session.permanent = True
                    
                    # Create login notification
                    create_notification(
                        user["id"],
                        "Login Successful",
                        f"You logged in successfully at {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                        "SUCCESS"
                    )
                    
                    # Audit log
                    audit_log(
                        user["id"],
                        "LOGIN",
                        "user",
                        user["id"],
                        {"username": username, "ip": request.remote_addr}
                    )
                    
                    flash("Login successful!", "success")
                    return redirect("/dashboard")
            
            # Audit log failed login
            audit_log(
                None,
                "LOGIN_FAILED",
                details={"username": username, "ip": request.remote_addr}
            )
            
            flash("Invalid username or password", "danger")
            
        except Exception as e:
            print(f"⚠️ Login error: {e}")
            flash("System error. Please try again.", "danger")
    
    return render_template("login.html")

@app.route("/logout")
def logout():
    # Audit log
    audit_log(
        session.get("user_id"),
        "LOGOUT",
        "user",
        session.get("user_id")
    )
    
    session.clear()
    flash("Logged out successfully", "info")
    return redirect("/")

# ==================================================
# DASHBOARD - UPDATED DENGAN PO LOGIC
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
                # User's PR statistics - HANYA YANG BELUM ADA PO
                stats['total_pr'] = conn.execute("""
                SELECT COUNT(*) FROM pr WHERE created_by=?
                AND id NOT IN (SELECT pr_id FROM po)
                """, (user_id,)).fetchone()[0]
                
                stats['po_created'] = conn.execute("""
                SELECT COUNT(*) FROM po 
                WHERE pr_id IN (SELECT id FROM pr WHERE created_by=?)
                """, (user_id,)).fetchone()[0]
                
                stats['with_quotation'] = conn.execute("""
                SELECT COUNT(*) FROM pr 
                WHERE created_by=? AND quotation_filename IS NOT NULL
                """, (user_id,)).fetchone()[0]
                
                # Get user's PRs - HANYA YANG BELUM ADA PO
                my_pr = conn.execute("""
                SELECT p.*, 
                       (SELECT COUNT(*) FROM pr_items WHERE pr_id=p.id) as item_count
                FROM pr p
                WHERE p.created_by=?
                AND p.id NOT IN (SELECT pr_id FROM po)
                ORDER BY p.created_at DESC
                LIMIT 10
                """, (user_id,)).fetchall()
            
            elif role == 'procurement':
                # Procurement dashboard
                stats['total_submitted'] = conn.execute("""
                SELECT COUNT(*) FROM pr WHERE status IN ('SUBMITTED', 'BUDGET_EXCEPTION_PENDING')
                AND id NOT IN (SELECT pr_id FROM po)
                """).fetchone()[0]
                
                stats['total_po'] = conn.execute("""
                SELECT COUNT(*) FROM po
                """).fetchone()[0]
                
                stats['pending_po'] = conn.execute("""
                SELECT COUNT(*) FROM pr 
                WHERE status IN ('SUBMITTED', 'BUDGET_EXCEPTION_PENDING')
                AND id NOT IN (SELECT pr_id FROM po)
                """).fetchone()[0]
                
                stats['without_quotation'] = conn.execute("""
                SELECT COUNT(*) FROM pr 
                WHERE status IN ('SUBMITTED', 'BUDGET_EXCEPTION_PENDING')
                AND quotation_filename IS NULL
                AND id NOT IN (SELECT pr_id FROM po)
                """).fetchone()[0]
                
                # Get PRs yang belum ada PO untuk procurement
                my_pr = conn.execute("""
                SELECT p.*, u.full_name as requester_name_full
                FROM pr p
                JOIN users u ON p.created_by = u.id
                WHERE p.status IN ('SUBMITTED', 'BUDGET_EXCEPTION_PENDING')
                AND p.id NOT IN (SELECT pr_id FROM po)
                ORDER BY p.created_at DESC
                LIMIT 10
                """).fetchall()
            
            elif role == 'superadmin':
                # Admin dashboard
                stats['total_users'] = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
                stats['total_prs'] = conn.execute("SELECT COUNT(*) FROM pr").fetchone()[0]
                stats['total_vendors'] = conn.execute("SELECT COUNT(*) FROM vendors").fetchone()[0]
                stats['total_pos'] = conn.execute("SELECT COUNT(*) FROM po").fetchone()[0]
                stats['total_quotations'] = conn.execute("""
                SELECT COUNT(*) FROM pr WHERE quotation_filename IS NOT NULL
                """).fetchone()[0]
                
                # Budget exception stats for superadmin
                stats['pending_budget_exceptions'] = conn.execute("""
                SELECT COUNT(*) FROM pr 
                WHERE budget_status = 'OUT_OF_BUDGET' 
                AND status = 'BUDGET_EXCEPTION_PENDING'
                """).fetchone()[0]
                
                # Get semua PR
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
        
        # Audit log
        audit_log(
            user_id,
            "VIEW_DASHBOARD",
            details={"role": role}
        )
        
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
# CREATE PR - DENGAN QUOTATION UPLOAD (FIXED)
# ==================================================
@app.route("/pr/new", methods=["GET", "POST"])
@login_required
@role_required("user")
def pr_new():
    if request.method == "POST":
        try:
            # Check quotation file
            if 'quotation' not in request.files:
                flash("Quotation file is required", "danger")
                return redirect("/pr/new")
            
            quotation_file = request.files['quotation']
            
            if quotation_file.filename == '':
                flash("No quotation file selected", "danger")
                return redirect("/pr/new")
            
            # Parse form data
            department = request.form["department"]
            budget_category = request.form.get("budget_category")
            fiscal_year = request.form.get("fiscal_year", str(datetime.now().year))
            
            # Parse items from JSON
            items = json.loads(request.form.get("items", "[]"))
            
            if len(items) == 0:
                flash("At least one item is required", "danger")
                return redirect("/pr/new")
            
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
            
            # Determine initial status
            initial_status = "SUBMITTED"  # SELALU SUBMITTED (tanpa approval)
            
            # Budget status
            if budget_check and budget_check['available']:
                budget_status = BUDGET_STATUS['IN_BUDGET']
            else:
                budget_status = BUDGET_STATUS['OUT_OF_BUDGET']
                initial_status = "BUDGET_EXCEPTION_PENDING"
            
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
                
                # Insert PR header TANPA quotation dulu
                cursor = conn.execute("""
                INSERT INTO pr (
                    pr_no, fiscal_year,
                    created_at, created_by, 
                    requester_name, department,
                    budget_category, budget_status,
                    purpose, priority,
                    vendor_name, vendor_code, vendor_contact,
                    total_amount, currency, tax_amount,
                    status, last_updated,
                    quotation_filename, quotation_uploaded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
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
                    initial_status,
                    datetime.now().isoformat()
                ))
                
                pr_id = cursor.lastrowid
                
                # SIMPAN FILE QUOTATION SELEPAS DAPAT pr_id
                try:
                    quotation_filename = save_quotation_file(quotation_file, pr_id)
                    
                    if not quotation_filename:
                        flash("Failed to save quotation file", "danger")
                        # Rollback PR creation
                        conn.execute("DELETE FROM pr WHERE id=?", (pr_id,))
                        return redirect("/pr/new")
                except ValueError as e:
                    flash(str(e), "danger")
                    conn.execute("DELETE FROM pr WHERE id=?", (pr_id,))
                    return redirect("/pr/new")
                
                # Update PR dengan quotation filename
                conn.execute("""
                UPDATE pr SET 
                    quotation_filename=?,
                    quotation_uploaded_at=?
                WHERE id=?
                """, (
                    quotation_filename,
                    datetime.now().isoformat(),
                    pr_id
                ))
                
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
                
                # Save quotation record dengan relative path
                conn.execute("""
                INSERT INTO pr_quotation (
                    pr_id, filename, file_path,
                    uploaded_by, uploaded_at,
                    file_size, mime_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    pr_id,
                    quotation_filename,
                    f"quotations/{quotation_filename}",  # Relative path
                    session["user_id"],
                    datetime.now().isoformat(),
                    os.path.getsize(os.path.join(QUOTATION_FOLDER, quotation_filename)),
                    quotation_file.content_type
                ))
                
                # Update budget if within budget
                if budget_check and budget_check['available'] and budget_category:
                    conn.execute("""
                    UPDATE budget_categories 
                    SET spent_amount = spent_amount + ?
                    WHERE department=? AND category=? AND fiscal_year=?
                    """, (total_amount, department, budget_category, fiscal_year))
                
                # Log action
                log_action(pr_id, "PR_CREATED", "PR created with quotation")
                
                # Audit log
                audit_log(
                    session["user_id"],
                    "CREATE_PR",
                    "pr",
                    pr_id,
                    {
                        "pr_no": pr_no,
                        "total_amount": total_amount,
                        "has_quotation": True,
                        "quotation_file": quotation_filename
                    }
                )
                
                # Create notification
                create_notification(
                    session["user_id"],
                    "PR Created",
                    f"PR {pr_no} has been created successfully with quotation.",
                    "SUCCESS",
                    pr_id
                )
            
            flash(f"PR {pr_no} created and submitted successfully with quotation!", "success")
            
            if initial_status == "BUDGET_EXCEPTION_PENDING":
                flash("This PR requires budget exception approval.", "warning")
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
            fiscal_year=datetime.now().year,
            allowed_extensions=list(ALLOWED_EXTENSIONS)
        )
    except Exception as e:
        print(f"⚠️ Error loading PR form: {e}")
        flash("Error loading form", "danger")
        return redirect("/dashboard")

# ==================================================
# VIEW PR DETAILS WITH QUOTATION
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
            
            # Get action history
            history = conn.execute("""
            SELECT * FROM approval_history 
            WHERE pr_id=?
            ORDER BY action_date DESC
            """, (pr_id,)).fetchall()
            
            # Check if PO exists
            po = conn.execute("""
            SELECT * FROM po WHERE pr_id=?
            """, (pr_id,)).fetchone()
            
            # Get quotation info
            quotation = conn.execute("""
            SELECT * FROM pr_quotation WHERE pr_id=?
            """, (pr_id,)).fetchone()
            
            # Get budget info if applicable
            budget_info = None
            if pr['budget_category']:
                budget_info = conn.execute("""
                SELECT * FROM budget_categories
                WHERE department=? AND category=? AND fiscal_year=?
                """, (pr['department'], pr['budget_category'], pr['fiscal_year'])).fetchone()
            
            # Audit log
            audit_log(
                session["user_id"],
                "VIEW_PR",
                "pr",
                pr_id,
                {"pr_no": pr['pr_no']}
            )
        
        return render_template(
            "view_pr.html",
            pr=pr,
            items=items,
            history=history,
            po=po,
            quotation=quotation,
            budget_info=budget_info,
            BUDGET_STATUS=BUDGET_STATUS
        )
        
    except Exception as e:
        print(f"⚠️ View PR error: {e}")
        flash("Error loading PR details", "danger")
        return redirect("/dashboard")

# ==================================================
# DOWNLOAD QUOTATION
# ==================================================
@app.route("/pr/<int:pr_id>/quotation")
@login_required
def download_quotation(pr_id):
    try:
        with db() as conn:
            # Get PR and quotation info
            pr = conn.execute("""
            SELECT p.*, u.full_name as creator_name 
            FROM pr p
            JOIN users u ON p.created_by = u.id
            WHERE p.id=?
            """, (pr_id,)).fetchone()
            
            if not pr:
                abort(404, description="PR not found")
            
            # Check permissions
            if session["role"] == "user" and pr["created_by"] != session["user_id"]:
                abort(403, description="You can only download quotations for your own PRs")
            
            # Check if quotation exists
            if not pr['quotation_filename']:
                flash("No quotation file found for this PR", "warning")
                return redirect(f"/pr/{pr_id}")
            
            quotation_path = os.path.join(QUOTATION_FOLDER, pr['quotation_filename'])
            
            if not os.path.exists(quotation_path):
                flash("Quotation file not found on server", "danger")
                return redirect(f"/pr/{pr_id}")
            
            # Audit log
            audit_log(
                session["user_id"],
                "DOWNLOAD_QUOTATION",
                "pr",
                pr_id,
                {"pr_no": pr['pr_no'], "filename": pr['quotation_filename']}
            )
            
            return send_from_directory(
                QUOTATION_FOLDER,
                pr['quotation_filename'],
                as_attachment=True,
                download_name=f"quotation_{pr['pr_no']}.{pr['quotation_filename'].rsplit('.', 1)[1]}"
            )
            
    except Exception as e:
        print(f"⚠️ Download quotation error: {e}")
        flash("Error downloading quotation", "danger")
        return redirect(f"/pr/{pr_id}")

# ==================================================
# BUDGET EXCEPTION APPROVAL (RECORD-ONLY)
# ==================================================
@app.route("/pr/<int:pr_id>/budget-exception", methods=["GET", "POST"])
@login_required
@role_required("superadmin")
def budget_exception_approval(pr_id):
    """Handle budget exception approval - SUPERADMIN ONLY"""
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
                        status='SUBMITTED',
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
                    log_action(pr_id, "BUDGET_EXCEPTION_APPROVE", comments)
                    
                    # Audit log
                    audit_log(
                        session["user_id"],
                        "APPROVE_BUDGET_EXCEPTION",
                        "pr",
                        pr_id,
                        {"action": "approve", "comments": comments, "pr_no": pr['pr_no']}
                    )
                    
                    # Create notifications
                    create_notification(
                        pr['created_by'],
                        "Budget Exception Approved",
                        f"Budget exception for PR {pr['pr_no']} has been approved by {session['name']}.",
                        "SUCCESS",
                        pr_id
                    )
                    
                    flash("Budget exception approved! PR is now ready for PO.", "success")
                    
                elif action == "reject":
                    conn.execute("""
                    UPDATE pr SET
                        status='REJECTED',
                        last_updated=?
                    WHERE id=?
                    """, (
                        datetime.now().isoformat(),
                        pr_id
                    ))
                    
                    log_action(pr_id, "BUDGET_EXCEPTION_REJECT", comments)
                    
                    # Audit log
                    audit_log(
                        session["user_id"],
                        "REJECT_BUDGET_EXCEPTION",
                        "pr",
                        pr_id,
                        {"action": "reject", "comments": comments, "pr_no": pr['pr_no']}
                    )
                    
                    create_notification(
                        pr['created_by'],
                        "Budget Exception Rejected",
                        f"Budget exception for PR {pr['pr_no']} has been rejected by {session['name']}.",
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

@app.route("/budget-exceptions")
@login_required
@role_required("superadmin")
def budget_exceptions_list():
    """List semua PR yang perlu budget exception approval"""
    try:
        with db() as conn:
            prs = conn.execute("""
            SELECT p.*, u.full_name as requester_name
            FROM pr p
            JOIN users u ON p.created_by = u.id
            WHERE budget_status = 'OUT_OF_BUDGET' 
            AND status = 'BUDGET_EXCEPTION_PENDING'
            ORDER BY p.created_at DESC
            """).fetchall()
        
        return render_template(
            "budget_exceptions_list.html",
            prs=prs,
            BUDGET_STATUS=BUDGET_STATUS
        )
        
    except Exception as e:
        print(f"⚠️ Budget exceptions list error: {e}")
        flash("Error loading budget exceptions", "danger")
        return redirect("/dashboard")

# ==================================================
# PROCUREMENT - UPDATED DENGAN PO LOGIC
# ==================================================
@app.route("/procurement")
@login_required
@role_required("procurement", "superadmin")
def procurement():
    try:
        with db() as conn:
            # Get submitted PRs yang belum ada PO (termasuk budget exception pending)
            prs = conn.execute("""
            SELECT p.*, 
                   u.full_name as requester_name_full,
                   u.department as requester_dept,
                   (SELECT COUNT(*) FROM pr_items WHERE pr_id=p.id) as item_count
            FROM pr p
            JOIN users u ON p.created_by = u.id
            WHERE p.status IN ('SUBMITTED', 'BUDGET_EXCEPTION_PENDING')
            AND p.id NOT IN (SELECT pr_id FROM po)  # KEY LOGIC - HANYA YANG BELUM ADA PO
            ORDER BY p.created_at DESC
            """).fetchall()
            
            # Get PO list (terbaru)
            pos = conn.execute("""
            SELECT po.*, pr.pr_no, u.full_name as requester_name
            FROM po
            JOIN pr ON po.pr_id = pr.id
            JOIN users u ON pr.created_by = u.id
            ORDER BY po.created_at DESC
            LIMIT 10
            """).fetchall()
            
            # Procurement stats
            stats = {
                'total_submitted': len(prs),
                'total_pos': conn.execute("SELECT COUNT(*) FROM po").fetchone()[0],
                'pending_po': len(prs),
                'without_quotation': conn.execute("""
                    SELECT COUNT(*) FROM pr 
                    WHERE status IN ('SUBMITTED', 'BUDGET_EXCEPTION_PENDING')
                    AND quotation_filename IS NULL
                    AND id NOT IN (SELECT pr_id FROM po)
                """).fetchone()[0]
            }
        
        # Audit log
        audit_log(
            session["user_id"],
            "VIEW_PROCUREMENT",
            details={"stats": stats}
        )
        
        return render_template(
            "procurement_enhanced.html",
            prs=prs,
            pos=pos,
            stats=stats
        )
        
    except Exception as e:
        print(f"⚠️ Procurement error: {e}")
        flash("Error loading procurement page", "danger")
        return redirect("/dashboard")

# ==================================================
# PO MANAGEMENT - PROCUREMENT ENTER PO NUMBER (FIXED)
# ==================================================
@app.route("/procurement/po/new/<int:pr_id>", methods=["GET", "POST"])
@login_required
@role_required("procurement", "superadmin")
def create_po(pr_id):
    """
    Form untuk procurement isi PO number untuk PR yang sudah submitted
    """
    try:
        with db() as conn:
            # Check jika PR sudah ada PO
            existing_po = conn.execute("""
            SELECT * FROM po WHERE pr_id=?
            """, (pr_id,)).fetchone()
            
            if existing_po:
                flash(f"PR ini sudah ada PO: {existing_po['po_no']}", "warning")
                return redirect("/procurement")
            
            # Get PR details
            pr = conn.execute("""
            SELECT p.*, u.full_name as requester_name_full
            FROM pr p
            JOIN users u ON p.created_by = u.id
            WHERE p.id=? AND p.status IN ('SUBMITTED', 'BUDGET_EXCEPTION_PENDING')
            """, (pr_id,)).fetchone()
            
            if not pr:
                flash("PR tidak ditemukan atau belum dalam status SUBMITTED/BUDGET_EXCEPTION_PENDING", "danger")
                return redirect("/procurement")
            
            # Check if PR has quotation - ENFORCE RULE
            if not pr['quotation_filename']:
                flash(f"Cannot create PO. PR {pr['pr_no']} must have quotation file.", "danger")
                return redirect("/procurement")
            
            if request.method == "POST":
                # Validasi PO number
                po_no = request.form.get("po_no", "").strip()
                po_date = request.form.get("po_date")
                
                if not po_no:
                    flash("PO Number is required", "danger")
                    return redirect(f"/procurement/po/new/{pr_id}")
                
                # Check jika PO number sudah ada
                existing_po_no = conn.execute("""
                SELECT po_no FROM po WHERE po_no=?
                """, (po_no,)).fetchone()
                
                if existing_po_no:
                    flash(f"PO Number {po_no} sudah digunakan", "danger")
                    return redirect(f"/procurement/po/new/{pr_id}")
                
                # Create PO record
                cursor = conn.execute("""
                INSERT INTO po (
                    pr_id, po_no, po_date,
                    vendor_name, total_amount,
                    created_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    pr_id,
                    po_no,
                    po_date or datetime.now().isoformat(),
                    pr['vendor_name'],
                    pr['total_amount'],
                    session["user_id"],
                    datetime.now().isoformat()
                ))
                
                po_id = cursor.lastrowid
                
                # Update PR status
                conn.execute("""
                UPDATE pr SET 
                    status='PO_CREATED',
                    last_updated=?
                WHERE id=?
                """, (datetime.now().isoformat(), pr_id))
                
                # Log action
                log_action(pr_id, "PO_CREATED", f"PO {po_no} created")
                
                # Audit log
                audit_log(
                    session["user_id"],
                    "CREATE_PO",
                    "po",
                    po_id,
                    {
                        "po_no": po_no,
                        "pr_no": pr['pr_no'],
                        "vendor": pr['vendor_name'],
                        "amount": pr['total_amount']
                    }
                )
                
                # Create notifications
                create_notification(
                    session["user_id"],
                    "PO Created",
                    f"PO {po_no} telah dibuat untuk PR {pr['pr_no']}",
                    "SUCCESS",
                    pr_id
                )
                
                create_notification(
                    pr['created_by'],
                    "PO Created for Your PR",
                    f"PO {po_no} telah dibuat untuk PR Anda: {pr['pr_no']}",
                    "INFO",
                    pr_id
                )
                
                flash(f"PO {po_no} berhasil dibuat untuk PR {pr['pr_no']}", "success")
                return redirect(f"/procurement/po/{po_id}?print=1")
            
            # GET request - show form
            items = conn.execute("""
            SELECT * FROM pr_items WHERE pr_id=?
            ORDER BY item_no
            """, (pr_id,)).fetchall()
            
            return render_template(
                "procurement_po_form.html",
                pr=pr,
                items=items,
                default_date=datetime.now().strftime("%Y-%m-%d")
            )
            
    except Exception as e:
        print(f"⚠️ Create PO error: {e}")
        flash("Error creating PO", "danger")
        return redirect("/procurement")

@app.route("/procurement/po/list")
@login_required
@role_required("procurement", "superadmin")
def po_list():
    """
    List semua PO yang sudah dibuat
    """
    try:
        with db() as conn:
            # Get all POs dengan detail PR
            pos = conn.execute("""
            SELECT 
                po.id as po_id,
                po.po_no,
                po.po_date,
                po.vendor_name,
                po.total_amount,
                po.created_at as po_created,
                po.status as po_status,
                
                pr.id as pr_id,
                pr.pr_no,
                pr.department,
                pr.created_at as pr_created,
                pr.quotation_filename,
                
                u.full_name as requester_name,
                po_user.full_name as po_creator_name
                
            FROM po
            JOIN pr ON po.pr_id = pr.id
            JOIN users u ON pr.created_by = u.id
            LEFT JOIN users po_user ON po.created_by = po_user.id
            WHERE po.status='ACTIVE'
            ORDER BY po.created_at DESC
            """).fetchall()
            
            # Get stats
            stats = conn.execute("""
            SELECT 
                COUNT(*) as total_po,
                COUNT(DISTINCT vendor_name) as total_vendors,
                SUM(total_amount) as total_amount,
                AVG(total_amount) as avg_amount
            FROM po
            WHERE status='ACTIVE'
            """).fetchone()
        
        # Audit log
        audit_log(
            session["user_id"],
            "VIEW_PO_LIST",
            details={"total_po": stats['total_po'] if stats else 0}
        )
        
        return render_template(
            "procurement_po_list.html",
            pos=pos,
            stats=stats
        )
            
    except Exception as e:
        print(f"⚠️ PO list error: {e}")
        flash("Error loading PO list", "danger")
        return redirect("/procurement")

@app.route("/procurement/po/<int:po_id>")
@login_required
def view_po(po_id):
    """
    View detail PO (accessible by multiple roles)
    """
    try:
        with db() as conn:
            # Get PO details dengan PR info
            po = conn.execute("""
            SELECT 
                po.*,
                pr.pr_no,
                pr.department,
                pr.purpose,
                pr.created_at as pr_created,
                pr.quotation_filename,
                
                u.full_name as requester_name,
                u.department as requester_dept,
                u.email as requester_email,
                
                po_user.full_name as po_creator_name
                
            FROM po
            JOIN pr ON po.pr_id = pr.id
            JOIN users u ON pr.created_by = u.id
            LEFT JOIN users po_user ON po.created_by = po_user.id
            WHERE po.id=?
            """, (po_id,)).fetchone()
            
            if not po:
                flash("PO not found", "danger")
                return redirect("/procurement/po/list")
            
            # Get PR items
            items = conn.execute("""
            SELECT * FROM pr_items WHERE pr_id=?
            ORDER BY item_no
            """, (po['pr_id'],)).fetchall()
            
            # Check permission for users (hanya bisa lihat PO mereka sendiri)
            if session["role"] == "user":
                # Verify user ID match
                requester_id = conn.execute("""
                SELECT created_by FROM pr WHERE id=?
                """, (po['pr_id'],)).fetchone()
                
                if not requester_id or requester_id['created_by'] != session["user_id"]:
                    abort(403, description="You can only view POs for your own PRs")
            
            # Audit log
            audit_log(
                session["user_id"],
                "VIEW_PO",
                "po",
                po_id,
                {"po_no": po['po_no'], "pr_no": po['pr_no']}
            )
            
            return render_template(
                "procurement_po_view.html",
                po=po,
                items=items
            )
            
    except Exception as e:
        print(f"⚠️ View PO error: {e}")
        flash("Error loading PO details", "danger")
        return redirect("/procurement/po/list")

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
        
        # Audit log
        audit_log(
            session["user_id"],
            "VIEW_NOTIFICATIONS"
        )
        
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
# VENDOR MANAGEMENT
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
        
        # Audit log
        audit_log(
            session["user_id"],
            "VIEW_VENDORS"
        )
        
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
                
                # Audit log
                audit_log(
                    session["user_id"],
                    "UPDATE_VENDOR",
                    "vendor",
                    vendor['id'],
                    {
                        "vendor_code": vendor_code,
                        "vendor_name": request.form['vendor_name']
                    }
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
@role_required("superadmin", "procurement", "user")
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
            
            # Get vendor details for audit log
            vendor = conn.execute("SELECT id, vendor_name FROM vendors WHERE vendor_code=?", (vendor_code,)).fetchone()
            
            # Delete vendor
            conn.execute("DELETE FROM vendors WHERE vendor_code=?", (vendor_code,))
            
            # Audit log
            audit_log(
                session["user_id"],
                "DELETE_VENDOR",
                "vendor",
                vendor['id'] if vendor else None,
                {"vendor_code": vendor_code, "vendor_name": vendor['vendor_name'] if vendor else "Unknown"}
            )
            
            flash(f"Vendor {vendor_code} deleted successfully", "success")
            
        return redirect("/vendors")
        
    except Exception as e:
        print(f"⚠️ Delete vendor error: {e}")
        flash("Error deleting vendor", "danger")
        return redirect("/vendors")

# ==================================================
# PROCUREMENT VENDOR REGISTRATION FORM
# ==================================================
@app.route("/procurement/vendor/new", methods=["GET", "POST"])
@login_required
@role_required("procurement", "superadmin")
def procurement_vendor_form():
    """
    Form untuk procurement daftar vendor baru
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
            
            # Audit log
            audit_log(
                session["user_id"],
                "CREATE_VENDOR",
                "vendor",
                None,
                {
                    "vendor_code": request.form['vendor_code'],
                    "vendor_name": request.form['vendor_name']
                }
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
# SEARCH API
# ==================================================
@app.route("/api/search")
@login_required
def global_search():
    query = request.args.get("q", "").strip()
    like = f"%{query}%"

    try:
        with db() as conn:
            prs = conn.execute("""
                SELECT pr_no, purpose, vendor_name, department, status
                FROM pr
                WHERE (pr_no LIKE ? OR purpose LIKE ? OR vendor_name LIKE ?)
                ORDER BY created_at DESC
                LIMIT 10
            """, (like, like, like)).fetchall()

            vendors = conn.execute("""
                SELECT vendor_code, vendor_name
                FROM vendors
                WHERE (vendor_code LIKE ? OR vendor_name LIKE ?)
                  AND is_active=1
                LIMIT 10
            """, (like, like)).fetchall()

        return jsonify({
            "prs": [dict(p) for p in prs],
            "vendors": [dict(v) for v in vendors]
        })
    except Exception as e:
        print(f"⚠️ Global search error: {e}")
        return jsonify({"prs": [], "vendors": []})


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
# PO SEARCH API
# ==================================================
@app.route("/api/po/search")
@login_required
def search_po():
    """Search POs for autocomplete"""
    query = request.args.get("q", "").strip()
    like = f"%{query}%"

    try:
        with db() as conn:
            pos = conn.execute("""
                SELECT po.po_no, pr.pr_no, po.vendor_name
                FROM po
                JOIN pr ON po.pr_id = pr.id
                WHERE (
                    po.po_no LIKE ? 
                    OR pr.pr_no LIKE ? 
                    OR po.vendor_name LIKE ?
                )
                AND po.status='ACTIVE'
                LIMIT 20
            """, (like, like, like)).fetchall()

        return jsonify([dict(p) for p in pos])
    except Exception as e:
        print(f"⚠️ PO search error: {e}")
        return jsonify([])

# ==================================================
# USER MANAGEMENT (SUPER ADMIN)
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
            
            # Audit log
            audit_log(
                session["user_id"],
                "CREATE_USER",
                "user",
                None,
                {"username": request.form["username"], "role": request.form["role"]}
            )
            
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
# USER API ROUTES
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
        
        # Audit log
        audit_log(
            session["user_id"],
            "UPDATE_USER",
            "user",
            user_id,
            data
        )
        
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
        
        # Audit log
        audit_log(
            session["user_id"],
            "DELETE_USER",
            "user",
            user_id,
            {"username": user['username']}
        )
        
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
        
        # Audit log
        audit_log(
            session["user_id"],
            "ACTIVATE_USER",
            "user",
            user_id
        )
        
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
        
        # Audit log
        audit_log(
            session["user_id"],
            "DEACTIVATE_USER",
            "user",
            user_id
        )
        
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
        
        # Audit log
        audit_log(
            session["user_id"],
            "RESET_PASSWORD",
            "user",
            user_id,
            {"action": "password_reset"}
        )
        
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
        
        # Audit log
        audit_log(
            session["user_id"],
            "UPDATE_PROFILE",
            "user",
            session["user_id"],
            data
        )
        
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
        
        # Audit log
        audit_log(
            session["user_id"],
            "CHANGE_PASSWORD",
            "user",
            session["user_id"],
            {"action": "password_change"}
        )
        
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
                
                for (
                    vcode, vname, vtype, regdate, taxid,
                    addr, contact, email, phone,
                    bank, baccount, baddr, bcode, swift,
                    pterms, fax, incoterms, currency, year
                ) in sample_vendors:
                    
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
    # Log the error
    print(f"❌ Server Error: {error}")
    return render_template("error.html",
                         error_code=500,
                         error_message="Internal Server Error",
                         error_description="Something went wrong on our end. Our team has been notified.",
                         datetime=datetime), 500

# ==================================================
# APPLICATION STARTUP (RENDER / GUNICORN SAFE)
# ==================================================
with app.app_context():
    try:
        print("🚀 Initializing application...")
        init_db()
        migrate_pr_columns()
        create_initial_users()
        print("✅ Application initialized successfully")
    except Exception as e:
        print(f"❌ Application initialization failed: {e}")
        # Don't crash on initialization failure
        # Let the health check endpoint handle it

# ==================================================
# HEALTH CHECK ENDPOINT
# ==================================================
@app.route("/health")
def health_check():
    """Health check endpoint for monitoring"""
    try:
        with db() as conn:
            # Test database connection
            conn.execute("SELECT 1").fetchone()
        
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": "connected"
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }), 500

# ==================================================
# MAIN ENTRY POINT
# ==================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)