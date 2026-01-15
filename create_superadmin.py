import sqlite3
from datetime import datetime
from werkzeug.security import generate_password_hash

DB_PATH = "pr.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        full_name TEXT NOT NULL,
        role TEXT NOT NULL,
        active INTEGER NOT NULL,
        created_at TEXT NOT NULL
    )
    """)
    conn.commit()
    conn.close()

def create_superadmin():
    conn = sqlite3.connect(DB_PATH)

    username = "admin"
    password = "admin123"
    full_name = "Super Admin"
    role = "superadmin"

    try:
        conn.execute("""
        INSERT INTO users (username, password_hash, full_name, role, active, created_at)
        VALUES (?, ?, ?, ?, 1, ?)
        """, (
            username,
            generate_password_hash(password),
            full_name,
            role,
            datetime.now().isoformat()
        ))
        conn.commit()
        print("✅ Super Admin created")
        print("   Username: admin")
        print("   Password: admin123")
    except sqlite3.IntegrityError:
        print("ℹ️ Super Admin already exists")
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
    create_superadmin()
