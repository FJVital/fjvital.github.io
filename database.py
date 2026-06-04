import sqlite3
import os
import uuid

DB_FILE = os.path.join(os.getenv("DB_PATH", "/data"), "schema_sync.db")

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes the database and creates tables if they don't exist."""
    conn = get_db_connection()

    # Create Users Table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            hashed_password TEXT,
            stripe_customer_id TEXT,
            credits INTEGER DEFAULT 3
        )
    ''')

    # Create Jobs Table (Updated with the missing columns Claude forgot)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            username TEXT,
            input_path TEXT,
            output_path TEXT,
            original_filename TEXT,
            price INTEGER,
            paid BOOLEAN DEFAULT 0,
            status TEXT DEFAULT 'processing',
            preview_data TEXT,
            download_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Safe migration: Add missing columns if your database already exists
    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN status TEXT DEFAULT 'processing'")
        conn.execute("ALTER TABLE jobs ADD COLUMN preview_data TEXT")
        conn.execute("ALTER TABLE jobs ADD COLUMN download_url TEXT")
    except sqlite3.OperationalError:
        pass # Columns already exist, which is fine

    conn.commit()
    conn.close()

# Run database initialization on startup
init_db()

# --- USER FUNCTIONS ---
def get_user(username: str):
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
    conn.close()
    return dict(user) if user else None

def create_user(username: str, hashed_password: str):
    # The Lock: Guests (auto-generated @flashfix.io emails) get 0 credits.
    # Real users who register with their actual email get 3 credits.
    initial_credits = 0 if username.endswith("@flashfix.io") else 3
    
    conn = get_db_connection()
    conn.execute(
        'INSERT INTO users (username, hashed_password, stripe_customer_id, credits) VALUES (?, ?, ?, ?)',
        (username, hashed_password, None, initial_credits)
    )
    conn.commit()
    conn.close()
    return True


def update_stripe_customer_id(username: str, customer_id: str):
    conn = get_db_connection()
    conn.execute('UPDATE users SET stripe_customer_id = ? WHERE username = ?', (customer_id, username))
    conn.commit()
    conn.close()
    return True

def decrement_credit(username: str):
    """Subtracts exactly one credit from the user's account safely."""
    conn = get_db_connection()
    conn.execute('UPDATE users SET credits = credits - 1 WHERE username = ? AND credits > 0', (username,))
    conn.commit()
    conn.close()
    return True

# --- JOB FUNCTIONS ---
def create_job(job_id: str, username: str, input_path: str, output_path: str, price: int, original_filename: str = "file", preview_data: str = None, download_url: str = None):
    """FIX: Now accepts all 8 arguments passed by app.py without crashing."""
    conn = get_db_connection()
    conn.execute(
        'INSERT INTO jobs (job_id, username, input_path, output_path, original_filename, price, paid, status, preview_data, download_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (job_id, username, input_path, output_path, original_filename, price, 0, 'ready', preview_data, download_url)
    )
    conn.commit()
    conn.close()

def create_new_job(username: str, file_url: str):
    """FIX: Added the wrapper function requested by app.py."""
    job_id = str(uuid.uuid4())
    conn = get_db_connection()
    conn.execute(
        'INSERT INTO jobs (job_id, username, input_path, output_path, original_filename, price, paid, status, preview_data, download_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (job_id, username, file_url, '', 'remote_file', 500, 0, 'processing', None, None)
    )
    conn.commit()
    conn.close()
    return job_id

def get_job(job_id: str):
    conn = get_db_connection()
    job = conn.execute('SELECT * FROM jobs WHERE job_id = ?', (job_id,)).fetchone()
    conn.close()
    return dict(job) if job else None

def mark_job_paid(job_id: str):
    conn = get_db_connection()
    conn.execute('UPDATE jobs SET paid = 1, status = "paid" WHERE job_id = ?', (job_id,))
    conn.commit()
    conn.close()

def get_user_history(username: str):
    conn = get_db_connection()
    jobs = conn.execute('SELECT * FROM jobs WHERE username = ? ORDER BY created_at DESC', (username,)).fetchall()
    conn.close()
    return [dict(job) for job in jobs]