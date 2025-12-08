from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from datetime import datetime, timedelta
from decimal import Decimal
import mysql.connector
from mysql.connector import Error
import json
import os 
from flask import request, jsonify, send_file
from functools import wraps
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
import io
from datetime import datetime
# ============================================
# FLASK APPLICATION CONFIGURATION
# ============================================

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-change-in-production-12345'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=24)

# MySQL Configuration - Hardcoded
DB_CONFIG = {
    'host': 'localhost',
    'user': 'flaskuser',
    'password': 'Flask@123',
    'database': 'hardware_inventory'
}

# Context processor for templates
@app.context_processor
def inject_globals():
    return {
        'app_name': 'Hardware Store Inventory',
        'currency': 'Rs '
    }

# ============================================
# DATABASE SETUP AND INITIALIZATION
# ============================================

def create_database():
    """Create the database if it doesn't exist"""
    try:
        config = {
            'host': DB_CONFIG['host'],
            'user': DB_CONFIG['user'],
            'password': DB_CONFIG['password']
        }
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        db_name = DB_CONFIG['database']
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"✓ Database '{db_name}' created/verified successfully!")
        
        cursor.close()
        connection.close()
        return True
    except Error as e:
        print(f"✗ Error creating database: {e}")
        return False

def initialize_database():
    """Initialize database with all tables"""
    
    tables_sql = """
    CREATE TABLE IF NOT EXISTS stores (
        store_id INT PRIMARY KEY AUTO_INCREMENT,
        store_name VARCHAR(100) NOT NULL,
        address TEXT,
        contact VARCHAR(15),
        email VARCHAR(100),
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_store_active (is_active)
    );

    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY AUTO_INCREMENT,
        username VARCHAR(50) NOT NULL UNIQUE,
        password_hash VARCHAR(255) NOT NULL,
        full_name VARCHAR(100) NOT NULL,
        email VARCHAR(100),
        contact VARCHAR(15),
        role ENUM('admin', 'staff') NOT NULL,
        store_id INT NULL,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE SET NULL,
        INDEX idx_username (username),
        INDEX idx_role (role),
        INDEX idx_store_user (store_id, role)
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id INT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(200) NOT NULL,
        brand VARCHAR(100),
        category VARCHAR(100),
        unit VARCHAR(50) DEFAULT 'pcs',
        description TEXT,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_product_name (name),
        INDEX idx_brand (brand),
        INDEX idx_category (category),
        INDEX idx_active (is_active)
    );

    CREATE TABLE IF NOT EXISTS inventory (
        inventory_id INT PRIMARY KEY AUTO_INCREMENT,
        store_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity DECIMAL(10, 2) DEFAULT 0,
        min_stock_level DECIMAL(10, 2) DEFAULT 0,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        last_modified_by INT,
        notes TEXT,
        FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
        FOREIGN KEY (last_modified_by) REFERENCES users(user_id) ON DELETE SET NULL,
        UNIQUE KEY unique_store_product (store_id, product_id),
        INDEX idx_store_product (store_id, product_id),
        INDEX idx_low_stock (store_id, quantity)
    );

    CREATE TABLE IF NOT EXISTS inventory_movements (
        movement_id INT PRIMARY KEY AUTO_INCREMENT,
        store_id INT NOT NULL,
        product_id INT NOT NULL,
        movement_type ENUM('in', 'out', 'adjustment') NOT NULL,
        quantity DECIMAL(10, 2) NOT NULL,
        previous_stock DECIMAL(10, 2) DEFAULT 0,
        new_stock DECIMAL(10, 2) DEFAULT 0,
        reference_type VARCHAR(50),
        reference_id INT,
        notes TEXT,
        created_by INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
        FOREIGN KEY (created_by) REFERENCES users(user_id) ON DELETE SET NULL,
        INDEX idx_store_movement (store_id, created_at),
        INDEX idx_reference (reference_type, reference_id)
    );

    CREATE TABLE IF NOT EXISTS customers (
        customer_id INT PRIMARY KEY AUTO_INCREMENT,
        customer_name VARCHAR(100) NOT NULL,
        mobile VARCHAR(15) UNIQUE,
        email VARCHAR(100),
        address TEXT,
        notes TEXT,
        total_credit DECIMAL(10, 2) DEFAULT 0,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_customer_mobile (mobile),
        INDEX idx_customer_name (customer_name)
    );

    CREATE TABLE IF NOT EXISTS bills (
        bill_id INT PRIMARY KEY AUTO_INCREMENT,
        bill_number VARCHAR(50) NOT NULL UNIQUE,
        store_id INT NOT NULL,
        staff_id INT NOT NULL,
        customer_id INT NULL,
        customer_name VARCHAR(100),
        customer_contact VARCHAR(15),
        subtotal DECIMAL(10, 2) NOT NULL,
        discount_type ENUM('flat', 'percentage') DEFAULT 'flat',
        discount_value DECIMAL(10, 2) DEFAULT 0,
        discount_amount DECIMAL(10, 2) DEFAULT 0,
        total_amount DECIMAL(10, 2) NOT NULL,
        payment_split JSON,
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE CASCADE,
        FOREIGN KEY (staff_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE SET NULL,
        INDEX idx_bill_number (bill_number),
        INDEX idx_store_bill (store_id, created_at),
        INDEX idx_staff_bill (staff_id, created_at),
        INDEX idx_customer_bills (customer_id, created_at)
    );

    CREATE TABLE IF NOT EXISTS bill_items (
        bill_item_id INT PRIMARY KEY AUTO_INCREMENT,
        bill_id INT NOT NULL,
        product_id INT NOT NULL,
        product_name VARCHAR(200) NOT NULL,
        quantity DECIMAL(10, 2) NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL,
        item_discount DECIMAL(10, 2) DEFAULT 0,
        total DECIMAL(10, 2) NOT NULL,
        FOREIGN KEY (bill_id) REFERENCES bills(bill_id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
        INDEX idx_bill_items (bill_id)
    );

    CREATE TABLE IF NOT EXISTS credit_payments (
        payment_id INT PRIMARY KEY AUTO_INCREMENT,
        payment_number VARCHAR(50) NOT NULL UNIQUE,
        customer_id INT NOT NULL,
        bill_id INT NOT NULL,
        store_id INT NOT NULL,
        original_credit_amount DECIMAL(10, 2) NOT NULL,
        payment_amount DECIMAL(10, 2) NOT NULL,
        remaining_credit DECIMAL(10, 2) NOT NULL,
        payment_method ENUM('cash', 'upi', 'card', 'bank_transfer') NOT NULL,
        payment_reference VARCHAR(100),
        notes TEXT,
        recorded_by INT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE,
        FOREIGN KEY (bill_id) REFERENCES bills(bill_id) ON DELETE CASCADE,
        FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE CASCADE,
        FOREIGN KEY (recorded_by) REFERENCES users(user_id) ON DELETE CASCADE,
        INDEX idx_customer_payment (customer_id, created_at),
        INDEX idx_bill_payment (bill_id),
        INDEX idx_store_payment (store_id, created_at),
        INDEX idx_payment_number (payment_number)
    );

    CREATE TABLE IF NOT EXISTS credit_notes (
        credit_id INT PRIMARY KEY AUTO_INCREMENT,
        credit_number VARCHAR(50) NOT NULL UNIQUE,
        bill_id INT NOT NULL,
        store_id INT NOT NULL,
        staff_id INT NOT NULL,
        customer_id INT NULL,
        total_amount DECIMAL(10, 2) NOT NULL,
        remaining_balance DECIMAL(10, 2) NOT NULL,
        status ENUM('active', 'fully_used', 'expired') DEFAULT 'active',
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (bill_id) REFERENCES bills(bill_id) ON DELETE CASCADE,
        FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE CASCADE,
        FOREIGN KEY (staff_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE SET NULL,
        INDEX idx_credit_number (credit_number),
        INDEX idx_store_credit (store_id, status),
        INDEX idx_bill_credit (bill_id),
        INDEX idx_customer_credit_notes (customer_id, status)
    );

    CREATE TABLE IF NOT EXISTS return_items (
        return_id INT PRIMARY KEY AUTO_INCREMENT,
        credit_id INT NOT NULL,
        product_id INT NOT NULL,
        product_name VARCHAR(200) NOT NULL,
        quantity DECIMAL(10, 2) NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL,
        refund_amount DECIMAL(10, 2) NOT NULL,
        date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (credit_id) REFERENCES credit_notes(credit_id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
        INDEX idx_credit_returns (credit_id)
    );

    CREATE TABLE IF NOT EXISTS credit_note_usage (
        usage_id INT PRIMARY KEY AUTO_INCREMENT,
        credit_id INT NOT NULL,
        bill_id INT NOT NULL,
        amount_used DECIMAL(10, 2) NOT NULL,
        used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (credit_id) REFERENCES credit_notes(credit_id) ON DELETE CASCADE,
        FOREIGN KEY (bill_id) REFERENCES bills(bill_id) ON DELETE CASCADE,
        INDEX idx_credit_usage (credit_id),
        INDEX idx_bill_usage (bill_id)
    );

    CREATE TABLE IF NOT EXISTS quotations (
        quote_id INT PRIMARY KEY AUTO_INCREMENT,
        quote_number VARCHAR(50) NOT NULL UNIQUE,
        store_id INT NOT NULL,
        staff_id INT NOT NULL,
        customer_name VARCHAR(100),
        customer_contact VARCHAR(15),
        subtotal DECIMAL(10, 2) NOT NULL,
        discount_type ENUM('flat', 'percentage') DEFAULT 'flat',
        discount_value DECIMAL(10, 2) DEFAULT 0,
        discount_amount DECIMAL(10, 2) DEFAULT 0,
        total_amount DECIMAL(10, 2) NOT NULL,
        status ENUM('pending', 'converted', 'expired') DEFAULT 'pending',
        converted_to_bill_id INT NULL,
        valid_until DATE,
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE CASCADE,
        FOREIGN KEY (staff_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (converted_to_bill_id) REFERENCES bills(bill_id) ON DELETE SET NULL,
        INDEX idx_quote_number (quote_number),
        INDEX idx_store_quote (store_id, created_at),
        INDEX idx_staff_quote (staff_id, created_at)
    );

    CREATE TABLE IF NOT EXISTS quotation_items (
        quote_item_id INT PRIMARY KEY AUTO_INCREMENT,
        quote_id INT NOT NULL,
        product_id INT NOT NULL,
        product_name VARCHAR(200) NOT NULL,
        quantity DECIMAL(10, 2) NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL,
        item_discount DECIMAL(10, 2) DEFAULT 0,
        total DECIMAL(10, 2) NOT NULL,
        FOREIGN KEY (quote_id) REFERENCES quotations(quote_id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
        INDEX idx_quote_items (quote_id)
    );
    """
    
    try:
        connection = get_db_connection()
        if not connection:
            return False
        
        cursor = connection.cursor()
        
        # Execute multi-statement SQL
        for result in cursor.execute(tables_sql, multi=True):
            pass
        
        connection.commit()
        cursor.close()
        connection.close()
        print("✓ All tables created successfully!")
        return True
    except Error as e:
        print(f"✗ Error creating tables: {e}")
        return False

def seed_initial_data():
    """Seed initial data (admin user and sample store)"""
    try:
        connection = get_db_connection()
        if not connection:
            return False
        
        cursor = connection.cursor()
        
        # Check if admin exists
        cursor.execute("SELECT user_id FROM users WHERE username = 'admin'")
        admin_exists = cursor.fetchone()
        
        if not admin_exists:
            # Create admin user with plain text password
            cursor.execute("""
                INSERT INTO users (username, password_hash, full_name, email, role, store_id)
                VALUES ('admin', 'admin123', 'System Administrator', 'admin@hardwarestore.com', 'admin', NULL)
            """)
            print("✓ Default admin user created!")
            print("  Username: admin")
            print("  Password: admin123")
            print("  ⚠️  WARNING: Using plain text passwords - NOT SECURE!")
        else:
            print("✓ Admin user already exists")
        
        # Check if sample store exists
        cursor.execute("SELECT store_id FROM stores WHERE store_name = 'Main Store'")
        store_exists = cursor.fetchone()
        
        if not store_exists:
            cursor.execute("""
                INSERT INTO stores (store_name, address, contact, email)
                VALUES ('Main Store', '123 Hardware Street, City', '1234567890', 'mainstore@hardwarestore.com')
            """)
            print("✓ Sample store created!")
        else:
            print("✓ Sample store already exists")
        
        connection.commit()
        cursor.close()
        connection.close()
        return True
    except Error as e:
        print(f"✗ Error seeding data: {e}")
        return False

def setup_database():
    """Complete database setup"""
    print("="*60)
    print("Hardware Store Inventory System - Database Setup")
    print("="*60)
    print()
    
    print("Step 1: Creating database...")
    if not create_database():
        print("Failed to create database!")
        return False
    print()
    
    print("Step 2: Creating tables...")
    if not initialize_database():
        print("Failed to create tables!")
        return False
    print()
    
    print("Step 3: Seeding initial data...")
    if not seed_initial_data():
        print("Failed to seed data!")
        return False
    print()
    
    print("="*60)
    print("✓ Database setup completed successfully!")
    print("="*60)
    print()
    print("You can now login with:")
    print("  Username: admin")
    print("  Password: admin123")
    print()
    return True

# ============================================
# DATABASE CONNECTION HELPER
# ============================================

def get_db_connection():
    """Create and return a database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def execute_query(query, params=None, fetch_one=False, fetch_all=False, commit=False):
    """Execute a database query"""
    connection = get_db_connection()
    if not connection:
        return None
    
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params or ())
        
        if commit:
            connection.commit()
            last_id = cursor.lastrowid
            cursor.close()
            connection.close()
            return last_id
        elif fetch_one:
            result = cursor.fetchone()
            cursor.close()
            connection.close()
            return result
        elif fetch_all:
            result = cursor.fetchall()
            cursor.close()
            connection.close()
            return result
        else:
            cursor.close()
            connection.close()
            return True
    except Error as e:
        print(f"Database error: {e}")
        if connection:
            connection.rollback()
            connection.close()
        return None

# ============================================
# SESSION HELPER FUNCTIONS
# ============================================

def is_logged_in():
    """Check if user is logged in"""
    return 'user_id' in session and 'username' in session

def is_admin():
    """Check if logged in user is admin"""
    return session.get('role') == 'admin'

def is_staff():
    """Check if logged in user is staff"""
    return session.get('role') == 'staff'

def login_required(f):
    """Decorator to require login"""
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_logged_in():
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    """Decorator to require admin role"""
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_logged_in():
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('login'))
        if not is_admin():
            flash('Access denied. Admin privileges required.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

def staff_required(f):
    """Decorator to require staff role"""
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_logged_in():
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('login'))
        if not is_staff():
            flash('Access denied. Staff privileges required.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

def admin_or_staff_required(f):
    """Allow access to both admin and staff roles (handle missing store_id safely)"""
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_logged_in():
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('login'))

        role = session.get('role')
        if role not in ['admin', 'staff']:
            flash('Access denied. Admin or staff privileges required.', 'danger')
            return redirect(url_for('index'))

        # 🧩 Optional: If the page depends on store_id (like product/inventory pages)
        # and admin has no store assigned, you can assign a default one
        if not session.get('store_id'):
            # Auto-assign first active store for admin to avoid errors
            store = execute_query("SELECT store_id FROM stores WHERE is_active = TRUE LIMIT 1", fetch_one=True)
            if store:
                session['store_id'] = store['store_id']
            else:
                flash('No active store found. Please create one first.', 'danger')
                return redirect(url_for('admin_stores'))

        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page"""
    if is_logged_in():
        if is_admin():
            return redirect(url_for('admin_dashboard'))
        return redirect(url_for('staff_dashboard'))
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        remember = request.form.get('remember', False)
        
        if not username or not password:
            flash('Please enter both username and password.', 'danger')
            return redirect(url_for('login'))
        
        # Authenticate user
        query = "SELECT * FROM users WHERE username = %s AND is_active = TRUE"
        user_data = execute_query(query, (username,), fetch_one=True)
        
        # Plain text password comparison (NOT SECURE - for development only)
        if user_data and user_data['password_hash'] == password:
            # Set session data
            session.permanent = bool(remember)
            session['user_id'] = user_data['user_id']
            session['username'] = user_data['username']
            session['full_name'] = user_data['full_name']
            session['email'] = user_data['email']
            session['role'] = user_data['role']
            session['store_id'] = user_data['store_id']
            
            next_page = request.args.get('next')
            
            if not next_page:
                # Check role from user_data to determine redirect
                if user_data['role'] == 'admin':
                    next_page = url_for('admin_dashboard')
                else:
                    next_page = url_for('staff_dashboard')
            
            flash(f'Welcome back, {user_data["full_name"]}!', 'success')
            return redirect(next_page)
        else:
            flash('Invalid username or password.', 'danger')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    """Logout user"""
    session.clear()
    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('login'))

#________________DASHBOARD API'S__________________________________________

@app.route('/staff/dashboard')
@staff_required
def staff_dashboard():
    """Staff dashboard"""
    store_id = session.get('store_id')
    
    # Today's sales
    today_sales_query = """
        SELECT COALESCE(SUM(total_amount), 0) as total 
        FROM bills 
        WHERE store_id = %s AND DATE(created_at) = CURDATE()
    """
    today_sales = execute_query(today_sales_query, (store_id,), fetch_one=True)
    today_sales = today_sales['total'] if today_sales else 0
    
    # This month's sales
    month_sales_query = """
        SELECT COALESCE(SUM(total_amount), 0) as total 
        FROM bills 
        WHERE store_id = %s AND MONTH(created_at) = MONTH(CURDATE()) AND YEAR(created_at) = YEAR(CURDATE())
    """
    month_sales = execute_query(month_sales_query, (store_id,), fetch_one=True)
    month_sales = month_sales['total'] if month_sales else 0
    
    # Today's bills count
    today_bills_query = """
        SELECT COUNT(*) as count 
        FROM bills 
        WHERE store_id = %s AND DATE(created_at) = CURDATE()
    """
    today_bills = execute_query(today_bills_query, (store_id,), fetch_one=True)
    today_bills = today_bills['count'] if today_bills else 0
    
    # Low stock items
    low_stock_query = """
        SELECT i.*, p.name as product_name, p.unit
        FROM inventory i
        JOIN products p ON i.product_id = p.product_id
        WHERE i.store_id = %s AND i.quantity < i.min_stock_level AND p.is_active = TRUE
        ORDER BY i.quantity
        LIMIT 5
    """
    low_stock = execute_query(low_stock_query, (store_id,), fetch_all=True) or []
    
    # Active credit notes
    credit_notes_query = """
        SELECT COUNT(*) as count, COALESCE(SUM(remaining_balance), 0) as total
        FROM credit_notes
        WHERE store_id = %s AND status = 'active'
    """
    credit_notes = execute_query(credit_notes_query, (store_id,), fetch_one=True)
    credit_notes_count = credit_notes['count'] if credit_notes else 0
    credit_notes_total = credit_notes['total'] if credit_notes else 0
    
    # Recent bills
    recent_bills_query = """
        SELECT * FROM bills
        WHERE store_id = %s
        ORDER BY created_at DESC
        LIMIT 5
    """
    recent_bills = execute_query(recent_bills_query, (store_id,), fetch_all=True) or []
    
    return render_template('staff_dashboard.html',
                         today_sales=today_sales,
                         month_sales=month_sales,
                         today_bills=today_bills,
                         low_stock=low_stock,
                         credit_notes_count=credit_notes_count,
                         credit_notes_total=credit_notes_total,
                         recent_bills=recent_bills)

@app.route('/api/dashboard/stats')
@staff_required
def api_dashboard_stats():
    """Optimized API endpoint for staff dashboard (1 connection, batched queries)"""
    try:
        store_id = session.get('store_id')
        if not store_id:
            return jsonify({'error': 'Store not assigned to user'}), 400

        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(dictionary=True)

        # Run all subqueries in a single trip to MySQL
        query = f"""
        SELECT 
            -- 1️⃣ Today's bills count
            (SELECT COUNT(*) FROM bills WHERE store_id = %s AND DATE(created_at) = CURDATE()) AS today_bills,

            -- 2️⃣ Today's sales
            (SELECT COALESCE(SUM(total_amount), 0) FROM bills WHERE store_id = %s AND DATE(created_at) = CURDATE()) AS today_sales,

            -- 3️⃣ Low stock count
            (SELECT COUNT(*) FROM inventory i 
                JOIN products p ON i.product_id = p.product_id
                WHERE i.store_id = %s 
                  AND i.quantity < i.min_stock_level 
                  AND p.is_active = TRUE) AS low_stock_count,

            -- 4️⃣ Total dues
            (SELECT COALESCE(SUM(remaining_balance), 0) 
             FROM credit_notes 
             WHERE store_id = %s AND status = 'active') AS total_dues,

            -- 5️⃣ Total customer credits
            (
                SELECT 
                    COALESCE(SUM(
                        CAST(JSON_UNQUOTE(JSON_EXTRACT(b.payment_split, '$.credit')) AS DECIMAL(10,2))
                    ), 0)
                    - COALESCE(SUM(cp.total_paid), 0)
                FROM bills b
                LEFT JOIN (
                    SELECT bill_id, SUM(payment_amount) AS total_paid
                    FROM credit_payments
                    GROUP BY bill_id
                ) cp ON b.bill_id = cp.bill_id
                WHERE b.store_id = %s
                  AND b.payment_split IS NOT NULL
                  AND JSON_EXTRACT(b.payment_split, '$.credit') IS NOT NULL
                  AND CAST(JSON_UNQUOTE(JSON_EXTRACT(b.payment_split, '$.credit')) AS DECIMAL(10,2)) > 0
            ) AS total_customer_credits;
        """

        cursor.execute(query, (store_id, store_id, store_id, store_id, store_id))
        result = cursor.fetchone()

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'today_bills': int(result['today_bills'] or 0),
            'today_sales': float(result['today_sales'] or 0),
            'low_stock_count': int(result['low_stock_count'] or 0),
            'total_dues': float(result['total_dues'] or 0),
            'customer_credits': float(result['total_customer_credits'] or 0)
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Failed to load statistics: {str(e)}'}), 500

#________________BILLING API'S__________________________________________
@app.route('/api/products')
@staff_required
def api_get_products():
    """Get all products with current stock for the staff's store"""
    try:
        store_id = session.get('store_id')
        search = request.args.get('search', '')
        low_stock = request.args.get('low_stock', '')
        
        if search:
            query = """
                SELECT p.product_id, p.name as product_name, p.brand, p.category, p.unit,
                       COALESCE(i.quantity, 0) as current_stock,
                       COALESCE(i.min_stock_level, 10) as low_stock_threshold
                FROM products p
                LEFT JOIN inventory i ON p.product_id = i.product_id AND i.store_id = %s
                WHERE p.is_active = TRUE AND p.name LIKE %s
                ORDER BY p.name
                LIMIT 50
            """
            products = execute_query(query, (store_id, f'%{search}%'), fetch_all=True) or []
        else:
            query = """
                SELECT p.product_id, p.name as product_name, p.brand, p.category, p.unit,
                       COALESCE(i.quantity, 0) as current_stock,
                       COALESCE(i.min_stock_level, 10) as low_stock_threshold
                FROM products p
                LEFT JOIN inventory i ON p.product_id = i.product_id AND i.store_id = %s
                WHERE p.is_active = TRUE
                ORDER BY p.brand, p.name
            """
            products = execute_query(query, (store_id,), fetch_all=True) or []
        
        # Filter for low stock if requested
        if low_stock == 'true':
            products = [p for p in products if float(p.get('current_stock', 0)) <= float(p.get('low_stock_threshold', 10))]
        
        # Convert Decimal to float for JSON serialization
        for product in products:
            product['current_stock'] = float(product['current_stock']) if product['current_stock'] is not None else 0.0
            product['low_stock_threshold'] = float(product['low_stock_threshold']) if product['low_stock_threshold'] is not None else 10.0
        
        return jsonify(products)
    except Exception as e:
        print(f"Error in api_get_products: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
        
@app.route('/api/sales', methods=['POST'])
@staff_required
def api_create_sale():
    """Create new sale/bill"""
    try:
        data = request.get_json()
        store_id = session.get('store_id')
        staff_id = session.get('user_id')
        
        customer_id = data.get('customer_id')
        items = data.get('items', [])
        payment_split = data.get('payment_split', {})
        credit_notes_used = data.get('credit_notes_used', [])
        
        # Validate customer details are provided
        if not customer_id:
            return jsonify({'success': False, 'message': 'Customer details are required to complete the sale'}), 400
        
        if not items:
            return jsonify({'success': False, 'message': 'No items in cart'}), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'success': False, 'message': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Calculate totals
            subtotal = sum(item['quantity'] * item['rate'] for item in items)
            item_discounts = sum(item.get('discount', 0) for item in items)
            credit_note_amount = payment_split.get('credit_note', 0)
            total_amount = subtotal - item_discounts - credit_note_amount
            
            # Generate bill number
            cursor.execute("SELECT COUNT(*) as count FROM bills WHERE store_id = %s", (store_id,))
            count = cursor.fetchone()['count']
            bill_number = f"BILL-{store_id}-{count + 1:06d}"
            
            # Get customer details if provided
            customer_name = None
            customer_contact = None
            if customer_id:
                cursor.execute("SELECT customer_name, mobile FROM customers WHERE customer_id = %s", (customer_id,))
                customer = cursor.fetchone()
                if customer:
                    customer_name = customer['customer_name']
                    customer_contact = customer['mobile']
            
            # Insert bill
            bill_query = """
                INSERT INTO bills (bill_number, store_id, staff_id, customer_id, customer_name, customer_contact,
                                   subtotal, discount_amount, total_amount, payment_split, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """
            cursor.execute(bill_query, (
                bill_number, store_id, staff_id, customer_id, customer_name, customer_contact,
                subtotal, item_discounts, total_amount, json.dumps(payment_split)
            ))
            bill_id = cursor.lastrowid
            
            # Validate stock availability before processing sale
            for item in items:
                cursor.execute("""
                    SELECT p.name, i.quantity 
                    FROM inventory i
                    JOIN products p ON i.product_id = p.product_id
                    WHERE i.store_id = %s AND i.product_id = %s
                """, (store_id, item['product_id']))
                stock_info = cursor.fetchone()
                
                if not stock_info:
                    connection.rollback()
                    cursor.close()
                    connection.close()
                    return jsonify({
                        'success': False, 
                        'message': f'Product "{item["product_name"]}" not found in inventory'
                    }), 400
                
                available_stock = float(stock_info['quantity'])
                
                if available_stock <= 0:
                    connection.rollback()
                    cursor.close()
                    connection.close()
                    return jsonify({
                        'success': False, 
                        'message': f'Product "{item["product_name"]}" is out of stock'
                    }), 400
                
                if item['quantity'] > available_stock:
                    connection.rollback()
                    cursor.close()
                    connection.close()
                    return jsonify({
                        'success': False, 
                        'message': f'Insufficient stock for "{item["product_name"]}". Available: {available_stock}, Requested: {item["quantity"]}'
                    }), 400
            
            # Insert bill items and update inventory
            for item in items:
                # Insert bill item
                item_query = """
                    INSERT INTO bill_items (bill_id, product_id, product_name, quantity, 
                                            unit_price, item_discount, total)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                item_total = (item['quantity'] * item['rate']) - item.get('discount', 0)
                cursor.execute(item_query, (
                    bill_id, item['product_id'], item['product_name'],
                    item['quantity'], item['rate'], item.get('discount', 0), item_total
                ))
                
                # Get current stock
                cursor.execute("""
                    SELECT quantity FROM inventory 
                    WHERE store_id = %s AND product_id = %s
                """, (store_id, item['product_id']))
                inv = cursor.fetchone()
                current_stock = float(inv['quantity']) if inv else 0
                
                # Update inventory
                new_stock = current_stock - item['quantity']
                cursor.execute("""
                    UPDATE inventory 
                    SET quantity = %s, last_modified_by = %s
                    WHERE store_id = %s AND product_id = %s
                """, (new_stock, staff_id, store_id, item['product_id']))
                
                # Log inventory movement
                cursor.execute("""
                    INSERT INTO inventory_movements 
                    (store_id, product_id, movement_type, quantity, previous_stock, new_stock,
                     reference_type, reference_id, notes, created_by, created_at)
                    VALUES (%s, %s, 'out', %s, %s, %s, 'bill', %s, %s, %s, NOW())
                """, (store_id, item['product_id'], item['quantity'], current_stock, 
                      new_stock, bill_id, f"Sale - Bill #{bill_number}", staff_id))
            
            # Handle credit note usage
            if credit_note_amount > 0 and credit_notes_used:
                remaining_to_use = credit_note_amount
                
                for cn in credit_notes_used:
                    if remaining_to_use <= 0:
                        break
                    
                    cn_id = cn['credit_id']
                    cn_balance = float(cn['remaining_balance'])
                    
                    amount_to_use = min(remaining_to_use, cn_balance)
                    new_balance = cn_balance - amount_to_use
                    
                    # Update credit note balance
                    cursor.execute("""
                        UPDATE credit_notes 
                        SET remaining_balance = %s,
                            status = CASE WHEN %s <= 0.01 THEN 'fully_used' ELSE 'active' END
                        WHERE credit_id = %s
                    """, (new_balance, new_balance, cn_id))
                    
                    # Log credit note usage
                    cursor.execute("""
                        INSERT INTO credit_note_usage (credit_id, bill_id, amount_used, used_at)
                        VALUES (%s, %s, %s, NOW())
                    """, (cn_id, bill_id, amount_to_use))
                    
                    remaining_to_use -= amount_to_use
            
            connection.commit()
            cursor.close()
            connection.close()
            
            return jsonify({
                'success': True,
                'sale_id': bill_id,
                'bill_number': bill_number,
                'total_amount': total_amount
            })
            
        except Exception as e:
            connection.rollback()
            cursor.close()
            connection.close()
            print(f"Error creating sale: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_create_sale: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/admin/customers-with-credits', methods=['GET'])
@staff_required
def api_get_customers_with_credits():
    """Get all customers who have outstanding credit bills for staff's store"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get staff's store_id
        user_store_id = session.get('store_id')
        
        if not user_store_id:
            cursor.close()
            connection.close()
            return jsonify({
                'success': False,
                'message': 'Store not assigned to user'
            }), 400
        
        # Query to get customers with outstanding credit bills in this store
        customer_query = """
            SELECT DISTINCT
                c.customer_id,
                c.customer_name,
                c.mobile,
                c.address
            FROM customers c
            INNER JOIN bills b ON c.customer_id = b.customer_id
            WHERE b.store_id = %s
            AND b.payment_split IS NOT NULL
            AND JSON_EXTRACT(b.payment_split, '$.credit') IS NOT NULL
            AND CAST(JSON_UNQUOTE(JSON_EXTRACT(b.payment_split, '$.credit')) AS DECIMAL(10,2)) > 0
            ORDER BY c.customer_name
        """
        
        cursor.execute(customer_query, (user_store_id,))
        customers = cursor.fetchall()
        
        # For each customer, get their outstanding bills from this store
        for customer in customers:
            bills_query = """
                SELECT 
                    b.bill_id,
                    b.bill_number,
                    b.store_id,
                    s.store_name,
                    b.total_amount,
                    b.created_at,
                    CAST(JSON_UNQUOTE(JSON_EXTRACT(b.payment_split, '$.credit')) AS DECIMAL(10,2)) as original_credit_amount,
                    COALESCE(
                        (SELECT SUM(cp.payment_amount)
                         FROM credit_payments cp
                         WHERE cp.bill_id = b.bill_id),
                        0
                    ) as paid_amount
                FROM bills b
                INNER JOIN stores s ON b.store_id = s.store_id
                WHERE b.customer_id = %s
                AND b.store_id = %s
                AND b.payment_split IS NOT NULL
                AND JSON_EXTRACT(b.payment_split, '$.credit') IS NOT NULL
                AND CAST(JSON_UNQUOTE(JSON_EXTRACT(b.payment_split, '$.credit')) AS DECIMAL(10,2)) > 0
                ORDER BY b.created_at DESC
            """
            
            cursor.execute(bills_query, (customer['customer_id'], user_store_id))
            bills = cursor.fetchall()
            
            # Calculate remaining credit and filter
            customer['bills'] = []
            for bill in bills:
                remaining = float(bill['original_credit_amount']) - float(bill['paid_amount'])
                if remaining > 0:
                    bill['remaining_credit'] = remaining
                    customer['bills'].append(bill)
        
        # Filter out customers with no outstanding bills
        customers = [c for c in customers if len(c.get('bills', [])) > 0]
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'customers': customers
        })
        
    except Exception as e:
        print(f"Error fetching customers with credits: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/credit-settlement/outstanding/<int:customer_id>', methods=['GET'])
@login_required
def api_get_outstanding_credit(customer_id):
    """Get all outstanding credit bills for a customer"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get user's store_id
        user_store_id = session.get('store_id')
        
        # Get customer details
        cursor.execute("""
            SELECT customer_id, customer_name, mobile, address
            FROM customers
            WHERE customer_id = %s
        """, (customer_id,))
        customer = cursor.fetchone()
        
        if not customer:
            cursor.close()
            connection.close()
            return jsonify({
                'success': False,
                'message': 'Customer not found'
            }), 404
        
        # Build query based on user role
        query = """
            SELECT 
                b.bill_id,
                b.bill_number,
                b.store_id,
                s.store_name,
                b.total_amount,
                b.payment_split,
                COALESCE(
                    (SELECT SUM(cp.payment_amount)
                     FROM credit_payments cp
                     WHERE cp.bill_id = b.bill_id),
                    0
                ) as total_paid,
                b.created_at,
                u.full_name as staff_name
            FROM bills b
            INNER JOIN stores s ON b.store_id = s.store_id
            LEFT JOIN users u ON b.staff_id = u.user_id
            WHERE b.customer_id = %s
            AND b.payment_split IS NOT NULL
        """
        
        params = [customer_id]
        
        # If staff user, filter by their store
        if user_store_id:
            query += " AND b.store_id = %s"
            params.append(user_store_id)
        
        query += " ORDER BY b.created_at ASC"
        
        cursor.execute(query, params)
        bills = cursor.fetchall()
        
        # Calculate remaining credit for each bill
        outstanding_bills = []
        for bill in bills:
            try:
                # Parse payment_split JSON
                if bill['payment_split']:
                    if isinstance(bill['payment_split'], str):
                        payment_split = json.loads(bill['payment_split'])
                    else:
                        payment_split = bill['payment_split']
                    
                    # Extract credit amount
                    credit_amount = payment_split.get('credit', 0)
                    if credit_amount:
                        # Convert to Decimal for precision
                        original_credit = Decimal(str(credit_amount))
                        # Convert total_paid to Decimal (it comes from DB as Decimal)
                        total_paid = Decimal(str(bill['total_paid'])) if bill['total_paid'] else Decimal('0')
                        remaining_credit = original_credit - total_paid
                        
                        if remaining_credit > Decimal('0.01'):
                            bill['original_credit_amount'] = float(original_credit)
                            bill['remaining_credit'] = float(remaining_credit)
                            # Remove payment_split from response
                            del bill['payment_split']
                            outstanding_bills.append(bill)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError, AttributeError) as e:
                print(f"Error parsing payment_split for bill {bill['bill_id']}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
        
        # Calculate total outstanding
        total_outstanding = sum(bill['remaining_credit'] for bill in outstanding_bills)
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'customer': customer,
            'outstanding_bills': outstanding_bills,
            'total_outstanding': float(total_outstanding)
        })
        
    except Exception as e:
        print(f"Error fetching outstanding credit: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/credit-payments/<int:payment_id>/print', methods=['GET'])
@login_required
def print_credit_payment_receipt(payment_id):
    """Generate PDF receipt for a credit payment"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get user's store_id
        user_store_id = session.get('store_id')
        
        # Get payment details with related information
        query = """
            SELECT 
                cp.payment_id,
                cp.payment_number,
                cp.original_credit_amount,
                cp.payment_amount,
                cp.remaining_credit,
                cp.payment_method,
                cp.payment_reference,
                cp.notes,
                cp.created_at,
                c.customer_name,
                c.mobile,
                c.address as customer_address,
                b.bill_number,
                b.bill_id,
                b.created_at as bill_date,
                b.total_amount as bill_total,
                s.store_name,
                s.address as store_address,
                s.contact as store_contact,
                u.full_name as recorded_by_name
            FROM credit_payments cp
            INNER JOIN customers c ON cp.customer_id = c.customer_id
            INNER JOIN bills b ON cp.bill_id = b.bill_id
            INNER JOIN stores s ON cp.store_id = s.store_id
            INNER JOIN users u ON cp.recorded_by = u.user_id
            WHERE cp.payment_id = %s
        """
        
        params = [payment_id]
        
        # If staff user, filter by their store
        if user_store_id:
            query += " AND cp.store_id = %s"
            params.append(user_store_id)
        
        cursor.execute(query, params)
        payment = cursor.fetchone()
        
        if not payment:
            cursor.close()
            connection.close()
            return jsonify({
                'success': False,
                'message': 'Payment record not found or access denied'
            }), 404
        
        cursor.close()
        connection.close()
        
        # Generate PDF
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=0.5*inch, bottomMargin=0.5*inch)
        elements = []
        
        styles = getSampleStyleSheet()
        
        # Custom styles
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#2563eb'),
            spaceAfter=6,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        
        receipt_title_style = ParagraphStyle(
            'ReceiptTitle',
            parent=styles['Heading1'],
            fontSize=20,
            textColor=colors.HexColor('#16a34a'),
            spaceAfter=12,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=14,
            textColor=colors.HexColor('#374151'),
            spaceAfter=12,
            spaceBefore=12,
            fontName='Helvetica-Bold'
        )
        
        normal_style = ParagraphStyle(
            'CustomNormal',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.HexColor('#1f2937'),
            spaceAfter=6
        )
        
        # Header - Store Info
        elements.append(Paragraph(payment['store_name'], title_style))
        
        if payment['store_address']:
            store_info = f"{payment['store_address']}"
            if payment['store_contact']:
                store_info += f" | Contact: {payment['store_contact']}"
            elements.append(Paragraph(store_info, normal_style))
        
        elements.append(Spacer(1, 0.15*inch))
        
        # Receipt Title
        elements.append(Paragraph("CREDIT PAYMENT RECEIPT", receipt_title_style))
        elements.append(Spacer(1, 0.15*inch))
        
        # Payment Info and Customer Info (Two columns)
        payment_info_data = [
            ['Payment Receipt #:', payment['payment_number']],
            ['Payment Date:', payment['created_at'].strftime('%d-%b-%Y %I:%M %p')],
            ['Recorded By:', payment['recorded_by_name']],
            ['Payment Method:', payment['payment_method'].upper().replace('_', ' ')],
        ]
        
        if payment['payment_reference']:
            payment_info_data.append(['Reference #:', payment['payment_reference']])
        
        customer_info_data = [
            ['Customer:', payment['customer_name']],
            ['Mobile:', payment['mobile'] or 'N/A'],
            ['Address:', payment['customer_address'] or 'N/A'],
            ['Original Bill #:', payment['bill_number']],
        ]
        
        # Two-column layout
        info_table_data = []
        max_rows = max(len(payment_info_data), len(customer_info_data))
        for i in range(max_rows):
            row = []
            if i < len(payment_info_data):
                row.extend(payment_info_data[i])
            else:
                row.extend(['', ''])
            
            row.append('')  # Spacer
            
            if i < len(customer_info_data):
                row.extend(customer_info_data[i])
            else:
                row.extend(['', ''])
            
            info_table_data.append(row)
        
        info_table = Table(info_table_data, colWidths=[1.5*inch, 1.8*inch, 0.3*inch, 1.5*inch, 1.8*inch])
        info_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (3, 0), (3, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#374151')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        elements.append(info_table)
        elements.append(Spacer(1, 0.25*inch))
        
        # Payment Details Section
        elements.append(Paragraph("Payment Details", heading_style))
        
        payment_details_data = [
            ['Description', 'Amount'],
            ['Original Credit Amount (from Bill)', f"Rs {float(payment['original_credit_amount']):.2f}"],
            ['Payment Received', f"Rs {float(payment['payment_amount']):.2f}"],
            ['Remaining Credit Balance', f"Rs {float(payment['remaining_credit']):.2f}"],
        ]
        
        payment_details_table = Table(payment_details_data, colWidths=[4.5*inch, 2.4*inch])
        payment_details_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#16a34a')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 11),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('ALIGN', (1, 1), (1, -1), 'RIGHT'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9fafb')]),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ]))
        elements.append(payment_details_table)
        elements.append(Spacer(1, 0.2*inch))
        
        # Payment Summary (Highlight)
        summary_data = [
            ['Total Payment Received:', f"Rs {float(payment['payment_amount']):.2f}"],
        ]
        
        summary_table = Table(summary_data, colWidths=[4.5*inch, 2.4*inch])
        summary_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 14),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('LINEABOVE', (0, 0), (-1, 0), 2, colors.HexColor('#16a34a')),
            ('LINEBELOW', (0, -1), (-1, -1), 2, colors.HexColor('#16a34a')),
            ('TOPPADDING', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f0fdf4')),
        ]))
        elements.append(summary_table)
        elements.append(Spacer(1, 0.2*inch))
        
        # Notes (if any)
        if payment['notes']:
            elements.append(Paragraph("Notes", heading_style))
            elements.append(Paragraph(payment['notes'], normal_style))
            elements.append(Spacer(1, 0.2*inch))
        
        # Status Badge
        if float(payment['remaining_credit']) <= 0:
            status_text = "STATUS: CREDIT FULLY PAID"
            status_color = colors.HexColor('#16a34a')
        else:
            status_text = f"STATUS: REMAINING CREDIT - Rs {float(payment['remaining_credit']):.2f}"
            status_color = colors.HexColor('#ea580c')
        
        status_style = ParagraphStyle(
            'Status',
            parent=styles['Normal'],
            fontSize=12,
            textColor=status_color,
            fontName='Helvetica-Bold',
            alignment=TA_CENTER
        )
        elements.append(Paragraph(status_text, status_style))
        
        # Footer
        elements.append(Spacer(1, 0.3*inch))
        footer_style = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#6b7280'),
            alignment=TA_CENTER
        )
        
        elements.append(Paragraph("Thank you for your payment!", footer_style))
        elements.append(Paragraph("This is a computer-generated receipt", footer_style))
        
        # Build PDF
        doc.build(elements)
        
        # Get PDF data
        pdf_data = buffer.getvalue()
        buffer.close()
        
        # Return PDF
        return send_file(
            io.BytesIO(pdf_data),
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'Payment_Receipt_{payment["payment_number"]}.pdf'
        )
        
    except Exception as e:
        print(f"Error generating payment receipt PDF: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/bills/<int:bill_id>/print', methods=['GET'])
@staff_required
def api_print_bill(bill_id):
    """Generate printable PDF for a bill - Using Unified PDF Format"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT 
                b.bill_id, b.bill_number, b.subtotal, b.discount_type, b.discount_value,
                b.discount_amount, b.total_amount, b.payment_split, b.notes, b.created_at,
                b.customer_name, b.customer_contact, s.store_name, s.address as store_address,
                s.contact as store_contact, s.email as store_email, u.full_name as staff_name,
                c.address as customer_address
            FROM bills b
            INNER JOIN stores s ON b.store_id = s.store_id
            INNER JOIN users u ON b.staff_id = u.user_id
            LEFT JOIN customers c ON b.customer_id = c.customer_id
            WHERE b.bill_id = %s
        """, (bill_id,))
        
        bill = cursor.fetchone()
        if not bill:
            cursor.close()
            connection.close()
            return jsonify({'success': False, 'message': 'Bill not found'}), 404
        
        cursor.execute("""
            SELECT product_name, quantity, unit_price, item_discount, total as line_total
            FROM bill_items WHERE bill_id = %s ORDER BY bill_item_id
        """, (bill_id,))
        items = cursor.fetchall()
        cursor.close()
        connection.close()
        
        meta_info = [
            ['Bill Number:', bill['bill_number'], '', 'Customer:', bill['customer_name'] or 'Walk-in Customer'],
            ['Date:', bill['created_at'].strftime('%d-%b-%Y %I:%M %p'), '', 'Mobile:', bill['customer_contact'] or 'N/A'],
            ['Staff:', bill['staff_name'], '', 'Address:', bill['customer_address'] or 'N/A'],
        ]
        
        items_data = [['#', 'Item Description', 'Qty', 'Unit Price', 'Discount', 'Line Total']]
        for idx, item in enumerate(items, 1):
            items_data.append([str(idx), item['product_name'], f"{float(item['quantity']):.2f}",
                f"Rs {float(item['unit_price']):.2f}", f"Rs {float(item['item_discount'] or 0):.2f}",
                f"Rs {float(item['line_total']):.2f}"])
        
        summary_data = [['Subtotal:', f"Rs {float(bill['subtotal']):.2f}"]]
        if bill['discount_amount'] and float(bill['discount_amount']) > 0:
            summary_data.append([f"Discount ({bill['discount_type']} - {bill['discount_value']}):", 
                f"- Rs {float(bill['discount_amount']):.2f}"])
        summary_data.extend([['', ''], ['Total Amount:', f"Rs {float(bill['total_amount']):.2f}"]])
        
        payment_details = None
        if bill['payment_split']:
            payment_split = json.loads(bill['payment_split'])
            payment_data = [['Payment Method', 'Amount']]
            payment_methods = {'cash': 'Cash', 'upi': 'UPI', 'card': 'Card', 'bank_transfer': 'Bank Transfer',
                'cheque': 'Cheque', 'credit': 'Credit', 'credit_note': 'Credit Note'}
            for key, label in payment_methods.items():
                if key in payment_split and float(payment_split[key]) > 0:
                    payment_data.append([label, f"Rs {float(payment_split[key]):.2f}"])
            if len(payment_data) > 1:
                payment_details = payment_data
        
        pdf_data = {
            'store_name': bill['store_name'], 'store_address': bill['store_address'] or '',
            'store_contact': bill['store_contact'] or '', 'store_email': bill['store_email'] or '',
            'meta_info': meta_info, 'items': items_data, 'summary': summary_data,
            'payment_details': payment_details, 'notes': bill['notes'],
            'footer_text': 'Thank you for your business!'
        }
        
        pdf_buffer = generate_unified_pdf(pdf_data, pdf_type="INVOICE")
        return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True,
            download_name=f'Invoice_{bill["bill_number"]}.pdf')
        
    except Exception as e:
        print(f"Error generating bill PDF: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500
        
@app.route('/api/outstanding/<int:customer_id>/print', methods=['GET'])
@staff_required
def print_outstanding_bills(customer_id):
    """Generate unified outstanding bills PDF with store details"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get user's store_id from session
        user_store_id = session.get('store_id')
        
        # Fetch customer details along with store details
        cursor.execute("""
            SELECT 
                c.customer_id,
                c.customer_name,
                c.mobile,
                c.address as customer_address,
                s.store_id,
                s.store_name,
                s.address as store_address,
                s.contact as store_contact,
                s.email as store_email
            FROM customers c
            CROSS JOIN stores s
            WHERE c.customer_id = %s
            AND s.store_id = %s
        """, (customer_id, user_store_id))
        
        customer = cursor.fetchone()
        
        if not customer:
            cursor.close()
            connection.close()
            return jsonify({'success': False, 'message': 'Customer not found'}), 404
        
        # Fetch outstanding bills with remaining credit
        query = """
            SELECT 
                b.bill_id,
                b.bill_number,
                b.total_amount,
                b.created_at,
                b.payment_split,
                COALESCE(
                    (SELECT SUM(cp.payment_amount)
                     FROM credit_payments cp
                     WHERE cp.bill_id = b.bill_id),
                    0
                ) as total_paid
            FROM bills b
            WHERE b.customer_id = %s
            AND b.store_id = %s
            AND b.payment_split IS NOT NULL
        """
        
        cursor.execute(query, (customer_id, user_store_id))
        bills = cursor.fetchall()
        
        cursor.close()
        connection.close()

        # Calculate outstanding bills
        outstanding_bills = []
        total_outstanding = 0
        
        for bill in bills:
            payment_split = json.loads(bill['payment_split']) if bill['payment_split'] else {}
            original_credit = float(payment_split.get('credit', 0))
            
            if original_credit > 0:
                remaining_credit = original_credit - float(bill['total_paid'])
                
                if remaining_credit > 0:
                    outstanding_bills.append({
                        'bill_number': bill['bill_number'],
                        'date': bill['created_at'],
                        'total_amount': bill['total_amount'],
                        'original_credit': original_credit,
                        'remaining': remaining_credit
                    })
                    total_outstanding += remaining_credit

        # Prepare items table
        items = [['#', 'Bill No', 'Date', 'Total Amount', 'Original Credit', 'Remaining']]
        
        for idx, bill in enumerate(outstanding_bills, 1):
            items.append([
                str(idx),
                bill['bill_number'],
                bill['date'].strftime('%d-%b-%Y'),
                f"Rs {float(bill['total_amount']):.2f}",
                f"Rs {float(bill['original_credit']):.2f}",
                f"Rs {float(bill['remaining']):.2f}"
            ])

        # Prepare summary
        summary = [['Total Outstanding:', f"Rs {total_outstanding:.2f}"]]
        
        # Prepare PDF data with FULL store details
        pdf_data = {
            'store_name': customer['store_name'],
            'store_address': customer['store_address'],
            'store_contact': customer['store_contact'],
            'store_email': customer['store_email'],
            'meta_info': [[
                'Customer:', customer['customer_name'], '', 
                'Report Date:', datetime.now().strftime('%d-%b-%Y'),
                '', 'Mobile:', customer.get('mobile', 'N/A')
            ]],
            'items': items,
            'summary': summary,
            'footer_text': 'Please clear outstanding balances promptly.'
        }

        # Generate unified PDF
        pdf_buffer = generate_unified_pdf(pdf_data, pdf_type="OUTSTANDING BILLS")
        
        return send_file(
            pdf_buffer, 
            mimetype='application/pdf', 
            as_attachment=True, 
            download_name=f'Outstanding_Bills_{customer["customer_name"]}.pdf'
        )
        
    except Exception as e:
        print(f"Error generating outstanding bills PDF: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/credit-settlement/record-payment', methods=['POST'])
@login_required
def api_record_credit_payment():
    """Record a payment made by customer to settle their credit"""
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['customer_id', 'bill_id', 'payment_amount', 'payment_method']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'success': False,
                    'message': f'Missing required field: {field}'
                }), 400
        
        customer_id = data['customer_id']
        bill_id = data['bill_id']
        payment_amount = float(data['payment_amount'])
        payment_method = data['payment_method']
        payment_reference = data.get('payment_reference', '')
        notes = data.get('notes', '')
        
        if payment_amount <= 0:
            return jsonify({
                'success': False,
                'message': 'Payment amount must be greater than 0'
            }), 400
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get user details
        user_id = session.get('user_id')
        user_store_id = session.get('store_id')
        
        # Get bill details and verify outstanding credit
        cursor.execute("""
            SELECT 
                b.bill_id,
                b.bill_number,
                b.store_id,
                b.customer_id,
                b.total_amount,
                b.payment_split,
                COALESCE(
                    (SELECT SUM(cp.payment_amount)
                     FROM credit_payments cp
                     WHERE cp.bill_id = b.bill_id),
                    0
                ) as total_paid
            FROM bills b
            WHERE b.bill_id = %s AND b.customer_id = %s
        """, (bill_id, customer_id))
        
        bill = cursor.fetchone()
        
        if not bill:
            cursor.close()
            connection.close()
            return jsonify({
                'success': False,
                'message': 'Bill not found or does not belong to this customer'
            }), 404
        
        # Parse payment_split to get original credit amount
        try:
            if bill['payment_split']:
                if isinstance(bill['payment_split'], str):
                    payment_split = json.loads(bill['payment_split'])
                else:
                    payment_split = bill['payment_split']
                
                credit_amount = payment_split.get('credit', 0)
                if not credit_amount or float(credit_amount) <= 0:
                    cursor.close()
                    connection.close()
                    return jsonify({
                        'success': False,
                        'message': 'This bill does not have any credit amount'
                    }), 400
                
                original_credit_amount = Decimal(str(credit_amount))
            else:
                cursor.close()
                connection.close()
                return jsonify({
                    'success': False,
                    'message': 'This bill does not have payment split information'
                }), 400
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            cursor.close()
            connection.close()
            return jsonify({
                'success': False,
                'message': f'Error parsing bill payment data: {str(e)}'
            }), 400
        
        # Verify user has access to this store
        if user_store_id and bill['store_id'] != user_store_id:
            cursor.close()
            connection.close()
            return jsonify({
                'success': False,
                'message': 'You do not have access to this bill'
            }), 403
        
        # Calculate outstanding credit using Decimal
        total_paid = Decimal(str(bill['total_paid'])) if bill['total_paid'] else Decimal('0')
        outstanding_credit = original_credit_amount - total_paid
        
        if outstanding_credit <= Decimal('0'):
            cursor.close()
            connection.close()
            return jsonify({
                'success': False,
                'message': 'This bill has no outstanding credit'
            }), 400
        
        # Check if payment amount exceeds outstanding
        payment_amount_decimal = Decimal(str(payment_amount))
        if payment_amount_decimal > outstanding_credit:
            cursor.close()
            connection.close()
            return jsonify({
                'success': False,
                'message': f'Payment amount (Rs {payment_amount:.2f}) exceeds outstanding credit (Rs {float(outstanding_credit):.2f})'
            }), 400
        
        # Calculate remaining credit after this payment
        remaining_credit = outstanding_credit - payment_amount_decimal
        
        # Generate payment number
        cursor.execute("""
            SELECT payment_number 
            FROM credit_payments 
            WHERE payment_number LIKE CONCAT('CP', DATE_FORMAT(NOW(), '%Y%m%d'), '%')
            ORDER BY payment_number DESC 
            LIMIT 1
        """)
        last_payment = cursor.fetchone()
        
        if last_payment:
            last_num = int(last_payment['payment_number'][-4:])
            payment_number = f"CP{datetime.now().strftime('%Y%m%d')}{str(last_num + 1).zfill(4)}"
        else:
            payment_number = f"CP{datetime.now().strftime('%Y%m%d')}0001"
        
        # Insert credit payment record
        cursor.execute("""
            INSERT INTO credit_payments (
                payment_number,
                customer_id,
                bill_id,
                store_id,
                original_credit_amount,
                payment_amount,
                remaining_credit,
                payment_method,
                payment_reference,
                notes,
                recorded_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            payment_number,
            customer_id,
            bill_id,
            bill['store_id'],
            float(original_credit_amount),
            payment_amount,
            float(remaining_credit),
            payment_method,
            payment_reference,
            notes,
            user_id
        ))
        
        payment_id = cursor.lastrowid
        
        # ============================================
        # UPDATE BILLS TABLE PAYMENT_SPLIT
        # ============================================
        
        # Update the payment_split in the bills table to reflect the credit payment
        # The updated payment_split will show credit paid with the payment method
        
        # Calculate how much credit has been paid in total
        new_credit_paid = float(total_paid) + payment_amount
        
        # Update the payment_split JSON
        updated_payment_split = dict(payment_split)  # Make a copy
        
        # Remove the original 'credit' entry
        updated_payment_split.pop('credit', None)
        
        # If there's still remaining credit, keep 'credit' with remaining amount
        if remaining_credit > 0:
            updated_payment_split['credit'] = float(remaining_credit)
        
        # Add the paid amount with the payment method
        # Accumulate if payment method already exists
        if payment_method in updated_payment_split:
            updated_payment_split[payment_method] = float(updated_payment_split[payment_method]) + payment_amount
        else:
            updated_payment_split[payment_method] = payment_amount
        
        # Update the bills table with new payment_split
        cursor.execute("""
            UPDATE bills 
            SET payment_split = %s
            WHERE bill_id = %s
        """, (json.dumps(updated_payment_split), bill_id))
        
        # ============================================
        # END OF UPDATE
        # ============================================
        
        connection.commit()
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'message': 'Credit payment recorded successfully',
            'payment_id': payment_id,
            'payment_number': payment_number,
            'remaining_credit': float(remaining_credit)
        })
        
    except Exception as e:
        print(f"Error recording credit payment: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/customers/<int:customer_id>/bills')
@staff_required
def api_get_customer_bills(customer_id):
    """Get all bills for a specific customer"""
    try:
        store_id = session.get('store_id')
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        # Get all bills for the customer
        cursor.execute("""
            SELECT bill_id as sale_id, bill_number, total_amount, 
                   subtotal, discount_amount, payment_split, created_at
            FROM bills
            WHERE customer_id = %s AND store_id = %s
            ORDER BY created_at DESC
        """, (customer_id, store_id))
        
        bills = cursor.fetchall()
        
        # Convert Decimal to float and parse payment_split for JSON serialization
        for bill in bills:
            bill['total_amount'] = float(bill['total_amount'])
            bill['subtotal'] = float(bill['subtotal'])
            bill['discount_amount'] = float(bill['discount_amount'])
            
            # Parse payment_split JSON
            if bill.get('payment_split'):
                try:
                    import json
                    bill['payment_split'] = json.loads(bill['payment_split'])
                except:
                    bill['payment_split'] = {}
            else:
                bill['payment_split'] = {}
        
        cursor.close()
        connection.close()
        
        return jsonify(bills)
        
    except Exception as e:
        print(f"Error in api_get_customer_bills: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/sales/<int:sale_id>/with-returns')
@staff_required
def api_get_bill_with_returns(sale_id):
    """Get bill details with returned quantities calculated"""
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        # Get bill details
        cursor.execute("""
            SELECT b.bill_id, b.bill_number, b.total_amount, b.subtotal, 
                   b.discount_amount, b.customer_id, b.payment_split, b.created_at,
                   c.customer_name, c.mobile
            FROM bills b
            LEFT JOIN customers c ON b.customer_id = c.customer_id
            WHERE b.bill_id = %s
        """, (sale_id,))
        
        bill = cursor.fetchone()
        
        if not bill:
            cursor.close()
            connection.close()
            return jsonify({'error': 'Bill not found'}), 404
        
        # Get bill items with product details
        cursor.execute("""
            SELECT bi.bill_item_id, bi.product_id, bi.product_name, bi.quantity, 
                   bi.unit_price as price, bi.item_discount, bi.total,
                   p.brand, p.unit
            FROM bill_items bi
            JOIN products p ON bi.product_id = p.product_id
            WHERE bi.bill_id = %s
        """, (sale_id,))
        
        items = cursor.fetchall()
        
        # For each item, calculate already returned quantity
        for item in items:
            product_id = item['product_id']
            
            # Get sum of already returned quantities for this product from this bill
            cursor.execute("""
                SELECT COALESCE(SUM(ri.quantity), 0) as returned_qty
                FROM return_items ri
                JOIN credit_notes cn ON ri.credit_id = cn.credit_id
                WHERE cn.bill_id = %s AND ri.product_id = %s
            """, (sale_id, product_id))
            
            returned_result = cursor.fetchone()
            already_returned = float(returned_result['returned_qty']) if returned_result else 0.0
            
            # Calculate remaining returnable quantity
            original_qty = float(item['quantity'])
            returnable_qty = original_qty - already_returned
            
            # Add fields to item
            item['quantity'] = original_qty
            item['already_returned'] = already_returned
            item['returnable_quantity'] = max(0, returnable_qty)  # Ensure non-negative
            item['price'] = float(item['price'])
            item['item_discount'] = float(item['item_discount'])
            item['total'] = float(item['total'])
        
        # Convert bill fields to float
        bill['total_amount'] = float(bill['total_amount'])
        bill['subtotal'] = float(bill['subtotal'])
        bill['discount_amount'] = float(bill['discount_amount'])
        
        # Parse payment_split JSON
        if bill.get('payment_split'):
            try:
                import json
                bill['payment_split'] = json.loads(bill['payment_split'])
            except:
                bill['payment_split'] = {}
        else:
            bill['payment_split'] = {}
        
        bill['items'] = items
        
        cursor.close()
        connection.close()
        
        return jsonify(bill)
        
    except Exception as e:
        print(f"Error in api_get_bill_with_returns: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/credit_notes')
@staff_required
def api_get_credit_notes():
    """Search or list credit notes (by CN# or mobile)"""
    try:
        store_id = session.get('store_id')
        cn_number = request.args.get('cn_number')
        mobile = request.args.get('mobile')

        query = """
            SELECT cn.credit_id, cn.credit_number, cn.customer_id, 
                   cn.total_amount AS amount, cn.remaining_balance, 
                   cn.status, cn.created_at,
                   c.customer_name, c.mobile,
                   cn.bill_id, cn.notes,
                   CASE 
                       WHEN cn.credit_number LIKE 'CN-ADV-%' OR cn.credit_number LIKE '%ADV%' 
                       THEN TRUE 
                       ELSE FALSE 
                   END AS is_advance
            FROM credit_notes cn
            JOIN customers c ON cn.customer_id = c.customer_id
            WHERE cn.store_id = %s
        """
        params = [store_id]

        # Add search filters
        if cn_number:
            query += " AND cn.credit_number LIKE %s"
            params.append(f"%{cn_number}%")
        elif mobile:
            query += " AND c.mobile LIKE %s"
            params.append(f"%{mobile}%")

        # Order by active/remaining first, then newest
        query += """
            ORDER BY 
                CASE 
                    WHEN cn.status = 'active' AND cn.remaining_balance > 0 THEN 0
                    ELSE 1
                END,
                cn.created_at DESC
            LIMIT 50
        """

        credit_notes = execute_query(query, params, fetch_all=True) or []

        # Convert Decimal to float for JSON serialization
        for cn in credit_notes:
            cn['amount'] = float(cn.get('amount', 0))
            cn['remaining_balance'] = float(cn.get('remaining_balance', 0))

        return jsonify(credit_notes)

    except Exception as e:
        print(f"Error in api_get_credit_notes: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/credit_notes', methods=['POST'])
@staff_required
def api_create_credit_note():
    """Create a new credit note for returned items"""
    try:
        data = request.get_json()
        store_id = session.get('store_id')
        staff_id = session.get('user_id')
        
        customer_id = data.get('customer_id')
        sale_id = data.get('sale_id')
        items = data.get('items', [])
        
        if not customer_id or not sale_id or not items:
            return jsonify({'success': False, 'message': 'Missing required fields'}), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'success': False, 'message': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Calculate total credit amount
            total_amount = 0
            for item in items:
                total_amount += item['return_qty'] * item['original_rate']
            
            # Generate credit note number
            cursor.execute("""
                SELECT MAX(CAST(SUBSTRING(credit_number, 3) AS UNSIGNED)) as max_num
                FROM credit_notes
                WHERE store_id = %s
            """, (store_id,))
            result = cursor.fetchone()
            next_num = (result['max_num'] or 0) + 1
            credit_number = f"CN{next_num:06d}"
            
            # Insert credit note
            cursor.execute("""
                INSERT INTO credit_notes 
                (credit_number, bill_id, store_id, staff_id, customer_id, 
                 total_amount, remaining_balance, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'active', NOW())
            """, (credit_number, sale_id, store_id, staff_id, customer_id, 
                  total_amount, total_amount))
            
            credit_note_id = cursor.lastrowid
            
            # Insert return items records
            for item in items:
                product_id = item['product_id']
                return_qty = float(item['return_qty'])
                original_rate = float(item['original_rate'])
                
                # 🔹 Step 1: Fetch Qty Sold for this product in the bill
                cursor.execute("""
                    SELECT quantity 
                    FROM bill_items 
                    WHERE bill_id = %s AND product_id = %s
                """, (sale_id, product_id))
                bill_item = cursor.fetchone()
                qty_sold = float(bill_item['quantity']) if bill_item else 0
                
                # 🔹 Step 2: Validate return quantity
                if return_qty > qty_sold:
                    connection.rollback()
                    cursor.close()
                    connection.close()
                    return jsonify({
                        'success': False,
                        'message': f'Return quantity for product ID {product_id} exceeds sold quantity ({qty_sold})'
                    }), 400
                
                # 🔹 Step 3: Compute refund
                refund_amount = return_qty * original_rate
                
                # Get product name
                cursor.execute("""
                    SELECT name FROM products WHERE product_id = %s
                """, (product_id,))
                product = cursor.fetchone()
                product_name = product['name'] if product else 'Unknown Product'
                
                # Insert into return_items table
                cursor.execute("""
                    INSERT INTO return_items 
                    (credit_id, product_id, product_name, quantity, unit_price, refund_amount, date)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """, (credit_note_id, product_id, product_name, return_qty, original_rate, refund_amount))
            
            # Update inventory for returned items
            for item in items:
                product_id = item['product_id']
                return_qty = item['return_qty']
                
                # Get current stock
                cursor.execute("""
                    SELECT quantity FROM inventory 
                    WHERE store_id = %s AND product_id = %s
                """, (store_id, product_id))
                inv = cursor.fetchone()
                current_stock = float(inv['quantity']) if inv else 0
                
                # Update inventory (add returned items back)
                new_stock = current_stock + return_qty
                cursor.execute("""
                    UPDATE inventory 
                    SET quantity = %s, last_modified_by = %s
                    WHERE store_id = %s AND product_id = %s
                """, (new_stock, staff_id, store_id, product_id))
                
                # Log inventory movement
                cursor.execute("""
                    INSERT INTO inventory_movements 
                    (store_id, product_id, movement_type, quantity, previous_stock, new_stock,
                     reference_type, reference_id, notes, created_by, created_at)
                    VALUES (%s, %s, 'in', %s, %s, %s, 'credit_note', %s, %s, %s, NOW())
                """, (store_id, product_id, return_qty, current_stock, 
                      new_stock, credit_note_id, f"Return - Credit Note #{credit_number}", staff_id))
            
            connection.commit()
            cursor.close()
            connection.close()
            
            return jsonify({
                'success': True,
                'credit_note_id': credit_note_id,
                'credit_number': credit_number,
                'amount': total_amount
            })
            
        except Exception as e:
            connection.rollback()
            cursor.close()
            connection.close()
            print(f"Error creating credit note: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_create_credit_note: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500

        return jsonify({'error': str(e)}), 500

@app.route('/api/credit-notes/advance-payment-v2', methods=['POST'])
@staff_required
def api_create_advance_payment_credit_note_v2():
    """
    Create a credit note for advance payment without generating a bill.
    Fixed version that does NOT insert into bills table.
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Invalid input data'}), 400

        store_id = session.get('store_id')
        staff_id = session.get('user_id')
        customer_id = data.get('customer_id')
        amount = Decimal(str(data.get('amount', 0)))
        notes = data.get('notes', '')

        # Validate amount
        if amount <= 0:
            return jsonify({'error': 'Advance amount must be greater than zero'}), 400

        # Step 1: Generate next credit note number (ADV prefix)
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT credit_number FROM credit_notes WHERE credit_number LIKE 'ADV-%' ORDER BY credit_id DESC LIMIT 1")
        last_cn = cursor.fetchone()

        if last_cn:
            last_num = int(last_cn['credit_number'].split('-')[1])
            next_num = last_num + 1
        else:
            next_num = 1

        new_credit_number = f"ADV-{next_num:05d}"

        # Step 2: Insert into credit_notes (no bill insert)
        insert_cn_query = """
            INSERT INTO credit_notes (
                credit_number,
                bill_id,
                store_id,
                staff_id,
                customer_id,
                total_amount,
                remaining_balance,
                status,
                notes,
                created_at
            ) VALUES (%s, NULL, %s, %s, %s, %s, %s, 'active', %s, NOW())
        """
        cursor.execute(insert_cn_query, (
            new_credit_number,
            store_id,
            staff_id,
            customer_id,
            amount,
            amount,
            notes
        ))
        conn.commit()

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'Advance payment credit note created successfully!',
            'credit_number': new_credit_number
        }), 201

    except Exception as e:
        print(f"Error creating advance payment credit note: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/credit-notes/<int:credit_id>/print', methods=['GET'])
@staff_required
def api_print_credit_note(credit_id):
    """Generate printable PDF for a credit note - Using Unified PDF Format"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT cn.credit_id, cn.credit_number, cn.total_amount, cn.remaining_balance,
                cn.status, cn.notes, cn.created_at, c.customer_name, c.mobile,
                c.address as customer_address, b.bill_number, s.store_name,
                s.address as store_address, s.contact as store_contact, s.email as store_email,
                u.full_name as staff_name
            FROM credit_notes cn
            INNER JOIN stores s ON cn.store_id = s.store_id
            INNER JOIN users u ON cn.staff_id = u.user_id
            LEFT JOIN customers c ON cn.customer_id = c.customer_id
            LEFT JOIN bills b ON cn.bill_id = b.bill_id
            WHERE cn.credit_id = %s
        """, (credit_id,))
        
        credit_note = cursor.fetchone()
        if not credit_note:
            cursor.close()
            connection.close()
            return jsonify({'success': False, 'message': 'Credit note not found'}), 404
        
        is_advance = credit_note['bill_number'] and credit_note['bill_number'].startswith('ADV-')
        return_items = []
        
        if not is_advance and credit_note['bill_number']:
            cursor.execute("""
                SELECT product_name, quantity, unit_price, refund_amount
                FROM return_items WHERE credit_id = %s ORDER BY return_id
            """, (credit_id,))
            return_items = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        meta_info = [
            ['Credit Note #:', credit_note['credit_number'], '', 'Customer:', credit_note['customer_name'] or 'N/A'],
            ['Date:', credit_note['created_at'].strftime('%d-%b-%Y %I:%M %p'), '', 'Mobile:', credit_note['mobile'] or 'N/A'],
            ['Staff:', credit_note['staff_name'], '', 'Address:', credit_note['customer_address'] or 'N/A'],
        ]
        
        if not is_advance:
            meta_info.append(['Original Bill #:', credit_note['bill_number'] or 'N/A', '', '', ''])
        
        items_data = None
        if return_items:
            items_data = [['#', 'Item Description', 'Quantity', 'Unit Price', '', 'Refund Amount']]
            for idx, item in enumerate(return_items, 1):
                items_data.append([str(idx), item['product_name'], f"{float(item['quantity']):.2f}",
                    f"Rs {float(item['unit_price']):.2f}", '', f"Rs {float(item['refund_amount']):.2f}"])
        
        summary_data = [
            ['Total Credit Amount:', f"Rs {float(credit_note['total_amount']):.2f}"],
            ['Amount Used:', f"Rs {float(credit_note['total_amount']) - float(credit_note['remaining_balance']):.2f}"],
            ['', ''],
            ['Remaining Balance:', f"Rs {float(credit_note['remaining_balance']):.2f}"],
        ]
        
        status_text = f"Status: {credit_note['status'].upper().replace('_', ' ')}"
        footer_msg = "This credit note can be redeemed for future purchases" if not is_advance else "This advance payment can be used for future purchases"
        
        pdf_data = {
            'store_name': credit_note['store_name'], 'store_address': credit_note['store_address'] or '',
            'store_contact': credit_note['store_contact'] or '', 'store_email': credit_note['store_email'] or '',
            'meta_info': meta_info, 'items': items_data, 'summary': summary_data,
            'notes': credit_note['notes'], 'additional_info': [status_text], 'footer_text': footer_msg
        }
        
        pdf_buffer = generate_unified_pdf(pdf_data, pdf_type="CREDIT NOTE")
        return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True,
            download_name=f'CreditNote_{credit_note["credit_number"]}.pdf')
        
    except Exception as e:
        print(f"Error generating credit note PDF: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/quotations/generate', methods=['POST'])
@admin_or_staff_required
def generate_quotation_pdf():
    """Generate a quotation PDF - Using Unified PDF Format"""
    try:
        data = request.json
        customer_name = data.get('customer_name', '').strip()
        customer_mobile = data.get('customer_mobile', '').strip()
        customer_address = data.get('customer_address', '').strip()
        items = data.get('items', [])
        discount_percentage = float(data.get('discount_percentage', 0))
        
        if not customer_name:
            return jsonify({'error': 'Customer name is required'}), 400
        if not customer_mobile or len(customer_mobile) != 10:
            return jsonify({'error': 'Valid 10-digit mobile number is required'}), 400
        if not items or len(items) == 0:
            return jsonify({'error': 'At least one item is required'}), 400
        
        store_id = session.get('store_id')
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT store_name, address, contact as contact_number FROM stores WHERE store_id = %s", (store_id,))
        store = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not store:
            return jsonify({'error': 'Store not found'}), 400
        
        quotation_number = f"QT-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        quotation_date = datetime.now().strftime('%d %B, %Y')
        
        meta_info = [
            ['Quotation No:', quotation_number, '', 'Date:', quotation_date],
            ['From:', store['store_name'], '', 'To:', customer_name],
            ['', store['address'] or 'N/A', '', 'Mobile:', customer_mobile],
            ['Phone:', store['contact_number'] or 'N/A', '', 'Address:', customer_address or 'N/A'],
        ]
        
        items_data = [['#', 'Product Name', 'Qty', 'Unit Price', 'Disc.%', 'Total']]
        subtotal = 0
        
        for idx, item in enumerate(items, 1):
            quantity = float(item.get('quantity', 0))
            unit_price = float(item.get('unit_price', 0))
            discount = float(item.get('discount', 0))
            discount_percentage_item = float(item.get('discount_percentage', 0))
            item_total = (quantity * unit_price) - discount
            subtotal += item_total
            
            brand = item.get('brand', '').strip()
            product_name = item.get('product_name', '')
            full_product_name = f"{brand} - {product_name}" if brand else product_name
            disc_display = f"{discount_percentage_item:.1f}%" if discount_percentage_item > 0 else '-'
            
            items_data.append([str(idx), full_product_name, f"{quantity:.2f}",
                f"Rs {unit_price:.2f}", disc_display, f"Rs {item_total:.2f}"])
        
        discount_amount = (subtotal * discount_percentage) / 100
        grand_total = subtotal - discount_amount
        
        summary_data = [['Subtotal:', f"Rs {subtotal:.2f}"]]
        if discount_percentage > 0:
            summary_data.append([f'Discount ({discount_percentage}%):', f"Rs {discount_amount:.2f}"])
        summary_data.extend([['', ''], ['Grand Total:', f"Rs {grand_total:.2f}"]])
        
        terms = [
            "Terms & Conditions:",
            "1. This quotation is valid for 30 days from the date of issue.",
            "2. Prices are subject to change without prior notice.",
            "3. Goods once sold will not be taken back or exchanged.",
            "4. Payment terms: As per agreed terms.",
            "5. All disputes are subject to local jurisdiction only."
        ]
        
        pdf_data = {
            'store_name': store['store_name'], 'store_address': store['address'] or '',
            'store_contact': store['contact_number'] or '', 'store_email': '',
            'meta_info': meta_info, 'items': items_data, 'summary': summary_data,
            'additional_info': terms, 'footer_text': 'Thank you for your business!'
        }
        
        pdf_buffer = generate_unified_pdf(pdf_data, pdf_type="QUOTATION")
        pdf_filename = f"Quotation_{customer_name.replace(' ', '_')}_{int(datetime.now().timestamp() * 1000)}.pdf"
        pdf_path = os.path.join(os.getcwd(), pdf_filename)
        
        with open(pdf_path, 'wb') as f:
            f.write(pdf_buffer.getvalue())
        
        return send_file(pdf_path, mimetype='application/pdf', as_attachment=True, download_name=pdf_filename)
        
    except Exception as e:
        print(f"Error generating quotation PDF: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_admin_sales_summary: {str(e)}")
        return jsonify({'error': str(e)}), 500
   
#________________INVENTORY API'S__________________________________________

@app.route('/api/inventory/all')
@staff_required
def api_get_inventory_all():
    """Get all inventory items for the unified inventory page"""
    try:
        store_id = session.get('store_id')
        
        query = """
            SELECT p.product_id, p.name as product_name, p.brand, p.category, p.unit,
                   COALESCE(i.quantity, 0) as current_stock,
                   COALESCE(i.min_stock_level, 10) as low_stock_threshold
            FROM products p
            LEFT JOIN inventory i ON p.product_id = i.product_id AND i.store_id = %s
            WHERE p.is_active = TRUE
            ORDER BY p.brand, p.name
        """
        products = execute_query(query, (store_id,), fetch_all=True) or []
        
        # Convert Decimal to float for JSON serialization
        for product in products:
            product['current_stock'] = float(product.get('current_stock', 0) or 0)
            product['low_stock_threshold'] = float(product.get('low_stock_threshold', 10) or 10)
            product['brand'] = product.get('brand') or ''
            product['category'] = product.get('category') or ''
        
        return jsonify({'success': True, 'products': products})
    except Exception as e:
        print(f"Error in api_get_inventory_all: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
        
@app.route('/api/products/<int:product_id>/history')
@staff_required
def api_get_product_history(product_id):
    """Get stock movement history for a product"""
    try:
        store_id = session.get('store_id')
        
        query = """
            SELECT movement_type as action_type, quantity as qty_change, 
                   notes as note, created_at
            FROM inventory_movements
            WHERE store_id = %s AND product_id = %s
            ORDER BY created_at DESC
            LIMIT 50
        """
        
        history = execute_query(query, (store_id, product_id), fetch_all=True) or []
        
        # Convert qty_change to signed values
        for item in history:
            if item['action_type'] == 'out':
                item['qty_change'] = -abs(float(item['qty_change']))
            else:
                item['qty_change'] = float(item['qty_change'])
        
        return jsonify(history)
    except Exception as e:
        print(f"Error in api_get_product_history: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock/purchase', methods=['POST'])
@staff_required
def api_stock_purchase():
    """Record stock purchase and update inventory"""
    try:
        data = request.get_json()
        print(f"Received stock purchase request: {data}")  # Debug log
        
        store_id = session.get('store_id')
        staff_id = session.get('user_id')
        
        print(f"Store ID: {store_id}, Staff ID: {staff_id}")  # Debug log
        
        supplier_name = data.get('supplier_name', '')
        purchase_date = data.get('purchase_date')
        items = data.get('items', [])
        
        print(f"Purchase date: {purchase_date}, Items count: {len(items)}")  # Debug log
        
        if not purchase_date or not items:
            return jsonify({'success': False, 'message': 'Purchase date and items are required'}), 400
        
        connection = get_db_connection()
        if not connection:
            print("Database connection failed!")  # Debug log
            return jsonify({'success': False, 'message': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Process each item
            for item in items:
                product_id = item['product_id']
                quantity = item['quantity']
                purchase_price = item.get('purchase_price', 0)
                
                # Get current stock
                cursor.execute("""
                    SELECT quantity FROM inventory 
                    WHERE store_id = %s AND product_id = %s
                """, (store_id, product_id))
                inv = cursor.fetchone()
                
                if inv:
                    current_stock = float(inv['quantity'])
                    new_stock = current_stock + quantity
                    
                    # Update inventory
                    cursor.execute("""
                        UPDATE inventory 
                        SET quantity = %s, last_modified_by = %s
                        WHERE store_id = %s AND product_id = %s
                    """, (new_stock, staff_id, store_id, product_id))
                else:
                    # Create inventory record if doesn't exist
                    current_stock = 0
                    new_stock = quantity
                    
                    cursor.execute("""
                        INSERT INTO inventory (store_id, product_id, quantity, last_modified_by)
                        VALUES (%s, %s, %s, %s)
                    """, (store_id, product_id, new_stock, staff_id))
                
                # Log inventory movement
                notes = f"Purchase from {supplier_name if supplier_name else 'Supplier'}"
                if purchase_price > 0:
                    notes += f" @ Rs {purchase_price:.2f}"
                
                # Convert date to datetime format for MySQL
                purchase_datetime = f"{purchase_date} 00:00:00"
                
                cursor.execute("""
                    INSERT INTO inventory_movements 
                    (store_id, product_id, movement_type, quantity, previous_stock, new_stock,
                     reference_type, notes, created_by, created_at)
                    VALUES (%s, %s, 'in', %s, %s, %s, 'purchase', %s, %s, %s)
                """, (store_id, product_id, quantity, current_stock, new_stock, 
                      notes, staff_id, purchase_datetime))
            
            connection.commit()
            cursor.close()
            connection.close()
            
            print(f"✓ Stock purchase recorded successfully for {len(items)} items")  # Debug log
            
            return jsonify({
                'success': True,
                'message': 'Stock purchase recorded successfully'
            })
            
        except Exception as e:
            connection.rollback()
            cursor.close()
            connection.close()
            print(f"Error recording purchase: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'success': False, 'message': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_stock_purchase: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/customers/<int:customer_id>/credit-notes')
@staff_required
def api_get_customer_credit_notess(customer_id):
    """Get all active credit notes for a customer"""
    try:
        store_id = session.get('store_id')
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        # Get only active credit notes with remaining balance
        cursor.execute("""
            SELECT credit_id, credit_number, bill_id, total_amount, remaining_balance, 
                   status, created_at
            FROM credit_notes
            WHERE customer_id = %s 
            AND store_id = %s
            AND status = 'active'
            AND remaining_balance > 0
            ORDER BY created_at ASC
        """, (customer_id, store_id))
        
        credit_notes = cursor.fetchall()
        
        # Convert Decimal to float for JSON serialization and calculate total
        total_available = 0
        for cn in credit_notes:
            cn['total_amount'] = float(cn['total_amount'])
            cn['remaining_balance'] = float(cn['remaining_balance'])
            cn['created_at'] = cn['created_at'].isoformat() if cn['created_at'] else None
            total_available += cn['remaining_balance']
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'credit_notes': credit_notes,
            'total_available': total_available
        })
        
    except Exception as e:
        print(f"Error in api_get_customer_credit_notes: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
        
#====================================CUSTOMERS API'S======================================================

@app.route('/api/customers', methods=['POST'])
@staff_required
def api_get_customers():
    """Get all customers with their total sales and credit balance"""
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
@app.route('/api/customers', methods=['GET', 'POST'])
@staff_required
def api_customers():
    try:

        # ----------------------------------------------------
        # 1️⃣  HANDLE CUSTOMER CREATION (POST)
        # ----------------------------------------------------
        if request.method == 'POST':
            data = request.get_json()

            customer_name = data.get('customer_name')
            mobile = data.get('mobile')
            address = data.get('address', '')

            if not customer_name or not mobile:
                return jsonify({
                    "success": False,
                    "message": "Customer name and mobile are required"
                }), 400

            connection = get_db_connection()
            cursor = connection.cursor(dictionary=True)

            cursor.execute("""
                INSERT INTO customers (customer_name, mobile, address)
                VALUES (%s, %s, %s)
            """, (customer_name, mobile, address))

            connection.commit()
            customer_id = cursor.lastrowid
            cursor.close()
            connection.close()

            return jsonify({
                "success": True,
                "customer_id": customer_id,
                "message": "Customer created successfully"
            }), 200


        # ----------------------------------------------------
        # 2️⃣  HANDLE GET (RETURN CUSTOMERS LIST)
        # ----------------------------------------------------
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)

        cursor.execute("""
            SELECT 
                c.customer_id,
                c.customer_name,
                c.mobile,
                c.address,
                COALESCE(SUM(b.total_amount), 0) AS total_sales,
                COALESCE((
                    SELECT SUM(cn.remaining_balance)
                    FROM credit_notes cn
                    WHERE cn.customer_id = c.customer_id
                      AND cn.status = 'active'
                ), 0) AS credit_balance,
                COALESCE((
                    SELECT SUM(
                        COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.cash')) AS DECIMAL(10,2)), 0) +
                        COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.upi')) AS DECIMAL(10,2)), 0) +
                        COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.card')) AS DECIMAL(10,2)), 0)
                    )
                    FROM bills b2 
                    WHERE b2.customer_id = c.customer_id
                ), 0) AS total_paid,
                COALESCE(SUM(b.total_amount), 0) -
                COALESCE((
                    SELECT SUM(
                        COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.cash')) AS DECIMAL(10,2)), 0) +
                        COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.upi')) AS DECIMAL(10,2)), 0) +
                        COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.card')) AS DECIMAL(10,2)), 0)
                    )
                    FROM bills b2 
                    WHERE b2.customer_id = c.customer_id
                ), 0) AS amount_due
            FROM customers c
            LEFT JOIN bills b ON c.customer_id = b.customer_id
            GROUP BY c.customer_id, c.customer_name, c.mobile, c.address
            ORDER BY c.customer_name
        """)

        customers = cursor.fetchall()

        for c in customers:
            c['total_sales'] = float(c['total_sales'])
            c['credit_balance'] = float(c['credit_balance'])
            c['total_paid'] = float(c.get('total_paid', 0))
            c['amount_due'] = float(c.get('amount_due', 0))

        cursor.close()
        connection.close()

        return jsonify(customers), 200

    except Exception as e:
        print("Error in /api/customers:", str(e))
        return jsonify({"success": False, "message": str(e)}), 500

            
@app.route('/api/sales/<int:sale_id>')
@staff_required
def api_get_sale(sale_id):
    """Get details of a specific sale including items"""
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        # Get bill details with customer info
        cursor.execute("""
            SELECT b.bill_id, b.bill_number, b.total_amount, b.subtotal, 
                   b.discount_amount, b.customer_id, b.payment_split, b.created_at,
                   c.customer_name, c.mobile
            FROM bills b
            LEFT JOIN customers c ON b.customer_id = c.customer_id
            WHERE b.bill_id = %s
        """, (sale_id,))
        
        bill = cursor.fetchone()
        
        if not bill:
            cursor.close()
            connection.close()
            return jsonify({'error': 'Bill not found'}), 404
        
        # Get bill items with product details
        cursor.execute("""
            SELECT bi.product_id, bi.product_name, bi.quantity, 
                   bi.unit_price as price, bi.item_discount, bi.total,
                   p.brand, p.unit
            FROM bill_items bi
            JOIN products p ON bi.product_id = p.product_id
            WHERE bi.bill_id = %s
        """, (sale_id,))
        
        items = cursor.fetchall()
        
        # Convert Decimal to float for JSON serialization
        bill['total_amount'] = float(bill['total_amount'])
        bill['subtotal'] = float(bill['subtotal'])
        bill['discount_amount'] = float(bill['discount_amount'])
        
        # Parse payment_split JSON
        if bill.get('payment_split'):
            try:
                import json
                bill['payment_split'] = json.loads(bill['payment_split'])
            except:
                bill['payment_split'] = {}
        else:
            bill['payment_split'] = {}
        
        for item in items:
            item['quantity'] = float(item['quantity'])
            item['price'] = float(item['price'])
            item['item_discount'] = float(item['item_discount'])
            item['total'] = float(item['total'])
        
        bill['items'] = items
        
        cursor.close()
        connection.close()
        
        return jsonify(bill)
        
    except Exception as e:
        print(f"Error in api_get_sale: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
        
#________________REPORTS API'S__________________________________________

@app.route('/api/reports/sales', methods=['GET'])
@staff_required
def api_reports_sales():
    """
    Get sales report grouped by date
    Query params: start_date, end_date
    Returns: Daily sales summary with payment breakdowns
    """
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({'error': 'start_date and end_date are required'}), 400
        
        store_id = session.get('store_id')
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Query to get daily sales summary
            query = """
                SELECT 
                    DATE(b.created_at) as date,
                    COUNT(DISTINCT b.bill_id) as total_bills,
                    COALESCE(SUM(b.total_amount), 0) as total_sales,
                    COALESCE(SUM(
                        CASE 
                            WHEN JSON_EXTRACT(b.payment_split, '$.cash') IS NOT NULL 
                            THEN CAST(JSON_EXTRACT(b.payment_split, '$.cash') AS DECIMAL(10,2))
                            ELSE 0 
                        END
                    ), 0) as cash_sales,
                    COALESCE(SUM(
                        CASE 
                            WHEN JSON_EXTRACT(b.payment_split, '$.upi') IS NOT NULL 
                            THEN CAST(JSON_EXTRACT(b.payment_split, '$.upi') AS DECIMAL(10,2))
                            ELSE 0 
                        END
                    ), 0) as upi_sales,
                    COALESCE(SUM(
                        CASE 
                            WHEN JSON_EXTRACT(b.payment_split, '$.credit') IS NOT NULL 
                            THEN CAST(JSON_EXTRACT(b.payment_split, '$.credit') AS DECIMAL(10,2))
                            ELSE 0 
                        END
                    ), 0) as credit_sales
                FROM bills b
                WHERE b.store_id = %s 
                    AND DATE(b.created_at) BETWEEN %s AND %s
                GROUP BY DATE(b.created_at)
                ORDER BY date DESC
            """
            
            cursor.execute(query, (store_id, start_date, end_date))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            # Convert datetime objects to strings for JSON serialization
            for row in results:
                if 'date' in row and row['date']:
                    row['date'] = row['date'].isoformat()
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error in sales report: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_reports_sales: {str(e)}")
        return jsonify({'error': str(e)}), 500        
             
@app.route('/api/reports/products', methods=['GET'])
@staff_required
def api_reports_products():
    """
    Get product-wise sales report
    Query params: start_date, end_date
    Returns: Product sales summary with quantity and revenue
    """
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({'error': 'start_date and end_date are required'}), 400
        
        store_id = session.get('store_id')
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Query to get product-wise sales
            query = """
                SELECT 
                    p.product_id,
                    p.name as product_name,
                    p.brand,
                    COALESCE(SUM(bi.quantity), 0) as total_qty_sold,
                    COALESCE(SUM(bi.total), 0) as total_revenue
                FROM products p
                INNER JOIN bill_items bi ON p.product_id = bi.product_id
                INNER JOIN bills b ON bi.bill_id = b.bill_id
                WHERE b.store_id = %s 
                    AND DATE(b.created_at) BETWEEN %s AND %s
                GROUP BY p.product_id, p.name, p.brand
                ORDER BY total_revenue DESC
            """
            
            cursor.execute(query, (store_id, start_date, end_date))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error in product report: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_reports_products: {str(e)}")
        return jsonify({'error': str(e)}), 500
        
@app.route('/api/reports/stock', methods=['GET'])
@staff_required
def api_reports_stocks():
    """
    Get current stock report for all products
    Returns: Current stock levels with low stock alerts
    """
    try:
        store_id = session.get('store_id')
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Query to get current stock levels
            query = """
                SELECT 
                    p.product_id,
                    p.name as product_name,
                    p.brand,
                    p.unit,
                    COALESCE(i.quantity, 0) as current_stock,
                    COALESCE(i.min_stock_level, 0) as low_stock_threshold
                FROM products p
                LEFT JOIN inventory i ON p.product_id = i.product_id 
                    AND i.store_id = %s
                WHERE p.is_active = TRUE
                ORDER BY 
                    CASE 
                        WHEN COALESCE(i.quantity, 0) <= COALESCE(i.min_stock_level, 0) 
                        THEN 0 
                        ELSE 1 
                    END,
                    p.name
            """
            
            cursor.execute(query, (store_id,))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error in stock report: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_reports_stock: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/reports/sales/export', methods=['GET'])
@staff_required
def api_export_sales_report():
    """
    Export sales report as PDF for the selected date range
    Query params: start_date, end_date
    Returns: PDF file download
    """
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({'error': 'start_date and end_date are required'}), 400
        
        store_id = session.get('store_id')
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Get store details
            cursor.execute("""
                SELECT store_name, address, contact, email 
                FROM stores WHERE store_id = %s
            """, (store_id,))
            store = cursor.fetchone()
            
            # Query to get daily sales summary
            query = """
                SELECT 
                    DATE(b.created_at) as date,
                    COUNT(DISTINCT b.bill_id) as total_bills,
                    COALESCE(SUM(b.total_amount), 0) as total_sales,
                    COALESCE(SUM(
                        CASE 
                            WHEN JSON_EXTRACT(b.payment_split, '$.cash') IS NOT NULL 
                            THEN CAST(JSON_EXTRACT(b.payment_split, '$.cash') AS DECIMAL(10,2))
                            ELSE 0 
                        END
                    ), 0) as cash_sales,
                    COALESCE(SUM(
                        CASE 
                            WHEN JSON_EXTRACT(b.payment_split, '$.upi') IS NOT NULL 
                            THEN CAST(JSON_EXTRACT(b.payment_split, '$.upi') AS DECIMAL(10,2))
                            ELSE 0 
                        END
                    ), 0) as upi_sales,
                    COALESCE(SUM(
                        CASE 
                            WHEN JSON_EXTRACT(b.payment_split, '$.credit') IS NOT NULL 
                            THEN CAST(JSON_EXTRACT(b.payment_split, '$.credit') AS DECIMAL(10,2))
                            ELSE 0 
                        END
                    ), 0) as credit_sales
                FROM bills b
                WHERE b.store_id = %s 
                    AND DATE(b.created_at) BETWEEN %s AND %s
                GROUP BY DATE(b.created_at)
                ORDER BY date DESC
            """
            
            cursor.execute(query, (store_id, start_date, end_date))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            # Calculate totals
            total_bills = sum(int(row['total_bills']) for row in results)
            total_sales = sum(float(row['total_sales']) for row in results)
            total_cash = sum(float(row['cash_sales']) for row in results)
            total_upi = sum(float(row['upi_sales']) for row in results)
            total_credit = sum(float(row['credit_sales']) for row in results)
            
            # Generate PDF
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4,
                                  rightMargin=30, leftMargin=30,
                                  topMargin=30, bottomMargin=30)
            
            elements = []
            styles = getSampleStyleSheet()
            
            # Custom styles
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=24,
                textColor=colors.HexColor('#1e293b'),
                spaceAfter=12,
                alignment=TA_CENTER,
                fontName='Helvetica-Bold'
            )
            
            subheading_style = ParagraphStyle(
                'SubHeading',
                parent=styles['Heading1'],
                fontSize=18,
                textColor=colors.HexColor('#6366f1'),
                alignment=TA_CENTER,
                spaceAfter=12,
                fontName='Helvetica-Bold'
            )
            
            normal_style = ParagraphStyle(
                'CustomNormal',
                parent=styles['Normal'],
                fontSize=10,
                textColor=colors.HexColor('#374151'),
                alignment=TA_CENTER
            )
            
            # Store Header
            elements.append(Paragraph(store.get('store_name', 'Hardware Store') if store else 'Hardware Store', title_style))
            
            store_info = f"""
            <para alignment="center">
            {store.get('address', '') if store else ''}<br/>
            Phone: {store.get('contact', '') if store else ''} | Email: {store.get('email', '') if store else ''}<br/>
            </para>
            """
            elements.append(Paragraph(store_info, normal_style))
            elements.append(Spacer(1, 0.2*inch))
            
            # Report Title
            elements.append(Paragraph("SALES REPORT", subheading_style))
            
            # Date Range
            date_range_text = f"From: {start_date} To: {end_date}"
            if start_date == end_date:
                date_range_text = f"Date: {start_date}"
            elements.append(Paragraph(date_range_text, normal_style))
            elements.append(Spacer(1, 0.2*inch))
            
            # Summary Cards
            summary_data = [
                ['Total Bills', 'Total Sales', 'Cash Sales', 'UPI Sales', 'Credit Sales'],
                [str(total_bills), f'Rs {total_sales:,.2f}', f'Rs {total_cash:,.2f}', f'Rs {total_upi:,.2f}', f'Rs {total_credit:,.2f}']
            ]
            summary_table = Table(summary_data, colWidths=[1.4*inch, 1.4*inch, 1.4*inch, 1.4*inch, 1.4*inch])
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#6366f1')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 1), (-1, -1), 10),
                ('BACKGROUND', (0, 1), (-1, 1), colors.HexColor('#f0f9ff')),
                ('TEXTCOLOR', (0, 1), (-1, 1), colors.HexColor('#1e293b')),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
                ('TOPPADDING', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
            ]))
            elements.append(summary_table)
            elements.append(Spacer(1, 0.3*inch))
            
            # Daily Breakdown Table
            if results:
                table_data = [['Date', 'Bills', 'Total Sales', 'Cash', 'UPI', 'Credit']]
                
                for row in results:
                    date_str = row['date'].strftime('%d %b %Y') if hasattr(row['date'], 'strftime') else str(row['date'])
                    table_data.append([
                        date_str,
                        str(row['total_bills']),
                        f"Rs {float(row['total_sales']):,.2f}",
                        f"Rs {float(row['cash_sales']):,.2f}",
                        f"Rs {float(row['upi_sales']):,.2f}",
                        f"Rs {float(row['credit_sales']):,.2f}"
                    ])
                
                # Add total row
                table_data.append([
                    'TOTAL',
                    str(total_bills),
                    f"Rs {total_sales:,.2f}",
                    f"Rs {total_cash:,.2f}",
                    f"Rs {total_upi:,.2f}",
                    f"Rs {total_credit:,.2f}"
                ])
                
                items_table = Table(table_data, colWidths=[1.3*inch, 0.8*inch, 1.3*inch, 1.2*inch, 1.2*inch, 1.2*inch])
                items_table.setStyle(TableStyle([
                    # Header
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#6366f1')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                    
                    # Body
                    ('FONTNAME', (0, 1), (-1, -2), 'Helvetica'),
                    ('FONTSIZE', (0, 1), (-1, -2), 9),
                    ('ALIGN', (0, 1), (0, -1), 'LEFT'),
                    ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
                    
                    # Total row
                    ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#f0f9ff')),
                    ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, -1), (-1, -1), 10),
                    ('TEXTCOLOR', (0, -1), (-1, -1), colors.HexColor('#1e293b')),
                    
                    # Grid
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -2), [colors.white, colors.HexColor('#f9fafb')]),
                    
                    # Padding
                    ('TOPPADDING', (0, 0), (-1, -1), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                    ('LEFTPADDING', (0, 0), (-1, -1), 5),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 5),
                ]))
                elements.append(Paragraph("<b>Daily Breakdown</b>", ParagraphStyle('Heading', fontSize=12, spaceAfter=10, textColor=colors.HexColor('#1e293b'))))
                elements.append(items_table)
            else:
                elements.append(Paragraph("No sales data found for the selected date range.", normal_style))
            
            # Footer
            elements.append(Spacer(1, 0.3*inch))
            footer_style = ParagraphStyle(
                'Footer',
                parent=styles['Normal'],
                fontSize=8,
                textColor=colors.HexColor('#94a3b8'),
                alignment=TA_CENTER
            )
            generated_at = datetime.now().strftime('%d %b %Y, %I:%M %p')
            elements.append(Paragraph(f"Generated on: {generated_at}", footer_style))
            
            # Build PDF
            doc.build(elements)
            pdf_data = buffer.getvalue()
            buffer.close()
            
            # Generate filename
            if start_date == end_date:
                filename = f'Sales_Report_{start_date}.pdf'
            else:
                filename = f'Sales_Report_{start_date}_to_{end_date}.pdf'
            
            return send_file(
                io.BytesIO(pdf_data),
                mimetype='application/pdf',
                as_attachment=True,
                download_name=filename
            )
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error in export sales report: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_export_sales_report: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/reports/bills-by-date', methods=['GET'])
@staff_required
def api_reports_bills_by_date():
    """
    Get all bills for a specific date
    Query params: date (YYYY-MM-DD format)
    Returns: List of bills with basic information
    """
    try:
        date = request.args.get('date')
        
        if not date:
            return jsonify({'error': 'date parameter is required'}), 400
        
        store_id = session.get('store_id')
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Query to get all bills for the specified date
            query = """
                SELECT 
                    b.bill_id,
                    b.bill_number,
                    b.customer_name,
                    b.customer_contact,
                    b.total_amount,
                    b.payment_split,
                    b.created_at,
                    u.full_name as staff_name
                FROM bills b
                LEFT JOIN users u ON b.staff_id = u.user_id
                WHERE b.store_id = %s 
                    AND DATE(b.created_at) = %s
                ORDER BY b.created_at DESC
            """
            
            cursor.execute(query, (store_id, date))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            # Convert datetime objects to strings for JSON serialization
            for row in results:
                if 'created_at' in row and row['created_at']:
                    row['created_at'] = row['created_at'].isoformat()
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error in bills by date: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_reports_bills_by_date: {str(e)}")
        return jsonify({'error': str(e)}), 500
        
@app.route('/api/reports/bill-details/<int:bill_id>', methods=['GET'])
@staff_required
def api_reports_bill_details(bill_id):
    """
    Get detailed information for a specific bill including items
    Path param: bill_id
    Returns: Complete bill details with items
    """
    try:
        store_id = session.get('store_id')
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Get bill header information
            bill_query = """
                SELECT 
                    b.bill_id,
                    b.bill_number,
                    b.customer_name,
                    b.customer_contact,
                    b.subtotal,
                    b.discount_type,
                    b.discount_value,
                    b.discount_amount,
                    b.total_amount,
                    b.payment_split,
                    b.notes,
                    b.created_at,
                    u.full_name as staff_name
                FROM bills b
                LEFT JOIN users u ON b.staff_id = u.user_id
                WHERE b.bill_id = %s AND b.store_id = %s
            """
            
            cursor.execute(bill_query, (bill_id, store_id))
            bill = cursor.fetchone()
            
            if not bill:
                cursor.close()
                connection.close()
                return jsonify({'error': 'Bill not found'}), 404
            
            # Get bill items
            items_query = """
                SELECT 
                    bi.bill_item_id,
                    bi.product_name,
                    bi.quantity,
                    bi.unit_price,
                    bi.item_discount,
                    bi.total
                FROM bill_items bi
                WHERE bi.bill_id = %s
                ORDER BY bi.bill_item_id
            """
            
            cursor.execute(items_query, (bill_id,))
            items = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            # Convert datetime objects to strings
            if 'created_at' in bill and bill['created_at']:
                bill['created_at'] = bill['created_at'].isoformat()
            
            # Add items to bill object
            bill['items'] = items
            
            return jsonify(bill)
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error in bill details: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_reports_bill_details: {str(e)}")
        return jsonify({'error': str(e)}), 500
        
#________________________UNIVERSAL_________________________________
@app.route('/api/customers/search')
@staff_required
def api_search_customer():
    """Search customer by mobile number with total sales and credit notes"""
    try:
        mobile = request.args.get('mobile', '')
        
        if not mobile:
            return jsonify({'found': False, 'error': 'Mobile number required'}), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        # Get customer details
        cursor.execute("""
            SELECT customer_id, customer_name, mobile, address
            FROM customers
            WHERE mobile = %s
        """, (mobile,))
        
        customer = cursor.fetchone()
        
        if not customer:
            cursor.close()
            connection.close()
            return jsonify({'found': False})
        
        customer_id = customer['customer_id']
        
        # Calculate total sales for this customer
        cursor.execute("""
            SELECT COALESCE(SUM(total_amount), 0) as total_sales
            FROM bills
            WHERE customer_id = %s
        """, (customer_id,))
        sales_result = cursor.fetchone()
        total_sales = float(sales_result['total_sales']) if sales_result else 0.0
        
        # Calculate total available credit notes for this customer
        cursor.execute("""
            SELECT COALESCE(SUM(remaining_balance), 0) as total_credit_balance
            FROM credit_notes
            WHERE customer_id = %s AND status = 'active'
        """, (customer_id,))
        credit_result = cursor.fetchone()
        total_credit_balance = float(credit_result['total_credit_balance']) if credit_result else 0.0
        
        # Get all active credit notes for this customer
        cursor.execute("""
            SELECT credit_id, credit_number, total_amount, remaining_balance, created_at
            FROM credit_notes
            WHERE customer_id = %s AND status = 'active' AND remaining_balance > 0
            ORDER BY created_at ASC
        """, (customer_id,))
        credit_notes = cursor.fetchall()
        
        # Convert Decimal to float for JSON serialization
        for cn in credit_notes:
            cn['total_amount'] = float(cn['total_amount'])
            cn['remaining_balance'] = float(cn['remaining_balance'])
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'found': True,
            'customer': {
                'customer_id': customer['customer_id'],
                'customer_name': customer['customer_name'],
                'mobile': customer['mobile'],
                'address': customer.get('address', ''),
                'total_sales': round(total_sales, 2),
                'total_credit_balance': round(total_credit_balance, 2)
            },
            'credit_notes': credit_notes
        })
            
    except Exception as e:
        print(f"Error in api_search_customer: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

# ============================================================================
# UNIFIED PDF GENERATION FUNCTION
# ============================================================================
def generate_unified_pdf(data, pdf_type="INVOICE"):
    """
    Unified PDF format for Invoice, Credit Note, Quotation, and Outstanding Bills.
    
    Parameters:
    - data: Dictionary containing all necessary data
    - pdf_type: String - "INVOICE", "CREDIT NOTE", "QUOTATION", "OUTSTANDING BILL"
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, 
                          rightMargin=30, leftMargin=30,
                          topMargin=30, bottomMargin=30)
    
    elements = []
    styles = getSampleStyleSheet()
    
    # ===== CUSTOM STYLES =====
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1e293b'),
        spaceAfter=12,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )
    
    subheading_style = ParagraphStyle(
        'SubHeading',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=colors.HexColor('#6366f1'),
        alignment=TA_CENTER,
        spaceAfter=12,
        fontName='Helvetica-Bold'
    )
    
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=colors.HexColor('#1e293b'),
        spaceAfter=6,
        fontName='Helvetica-Bold'
    )
    
    normal_style = ParagraphStyle(
        'CustomNormal',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#374151')
    )
    
    # ===== STORE HEADER =====
    elements.append(Paragraph(data.get('store_name', 'Main Store'), title_style))
    
    store_info = f"""
    <para alignment="center">
    {data.get('store_address', '123 Hardware Street, City')}<br/>
    Phone: {data.get('store_contact', '1234567890')} | Email: {data.get('store_email', 'mainstore@hardwarestore.com')}<br/>
    </para>
    """
    elements.append(Paragraph(store_info, normal_style))
    elements.append(Spacer(1, 0.2*inch))
    
    # ===== DOCUMENT TYPE (SUBHEADING) =====
    elements.append(Paragraph(pdf_type, subheading_style))
    elements.append(Spacer(1, 0.15*inch))
    
    # ===== META INFORMATION TABLE =====
    if 'meta_info' in data:
        meta_data = data['meta_info']
        info_table = Table(meta_data, colWidths=[1.5*inch, 1.8*inch, 0.3*inch, 1.5*inch, 1.8*inch])
        info_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (3, 0), (3, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#374151')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        elements.append(info_table)
        elements.append(Spacer(1, 0.2*inch))
    
    # ===== ITEMS TABLE =====
    if 'items' in data and data['items']:
        items_table_data = data['items']
        
        # Determine column widths based on pdf_type
        if pdf_type == "OUTSTANDING BILL":
            col_widths = [0.5*inch, 1.5*inch, 1.2*inch, 1.2*inch, 1.2*inch, 1.4*inch]
        else:
            col_widths = [0.4*inch, 3*inch, 0.8*inch, 1.1*inch, 1*inch, 1.2*inch]
        
        items_table = Table(items_table_data, colWidths=col_widths)
        items_table.setStyle(TableStyle([
            # Header
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#6366f1')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
            
            # Body
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (0, 1), (0, -1), 'CENTER'),
            ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
            ('VALIGN', (0, 1), (-1, -1), 'MIDDLE'),
            
            # Grid
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9fafb')]),
            
            # Padding
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ]))
        elements.append(items_table)
        elements.append(Spacer(1, 0.15*inch))
    
    # ===== SUMMARY SECTION =====
    if 'summary' in data:
        summary_data = data['summary']
        summary_table = Table(summary_data, colWidths=[4.5*inch, 2.4*inch])
        summary_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -2), 'Helvetica'),
            ('FONTNAME', (0, -1), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -2), 10),
            ('FONTSIZE', (0, -1), (-1, -1), 14),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('TEXTCOLOR', (0, -1), (-1, -1), colors.HexColor('#6366f1')),
            ('LINEABOVE', (0, -1), (-1, -1), 2, colors.HexColor('#6366f1')),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        elements.append(summary_table)
        elements.append(Spacer(1, 0.2*inch))
    
    # ===== PAYMENT DETAILS (FOR INVOICES) =====
    if 'payment_details' in data:
        elements.append(Paragraph("Payment Details", heading_style))
        payment_table = Table(data['payment_details'], colWidths=[3*inch, 2*inch])
        payment_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f3f4f6')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        elements.append(payment_table)
        elements.append(Spacer(1, 0.15*inch))
    
    # ===== NOTES SECTION =====
    if 'notes' in data and data['notes']:
        elements.append(Paragraph("Notes", heading_style))
        elements.append(Paragraph(data['notes'], normal_style))
        elements.append(Spacer(1, 0.15*inch))
    
    # ===== ADDITIONAL SECTIONS (FOR CREDIT NOTES, ETC) =====
    if 'additional_info' in data:
        for info in data['additional_info']:
            elements.append(Paragraph(info, normal_style))
            elements.append(Spacer(1, 0.05*inch))
    
    # ===== FOOTER =====
    elements.append(Spacer(1, 0.3*inch))
    footer_style = ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#6b7280'),
        alignment=TA_CENTER
    )
    
    footer_text = data.get('footer_text', 'Thank you for your business!')
    elements.append(Paragraph(footer_text, footer_style))
    elements.append(Paragraph("This is a computer-generated document", footer_style))
    
    # Build PDF
    doc.build(elements)
    buffer.seek(0)
    return buffer

@app.route('/inventory')
@staff_required
def inventory():
    """Unified inventory management page for staff"""
    return render_template('tinventory.html')
    
@app.route('/staff_billing')
@staff_required
def staff_billing():
    """Unified inventory management page for staff"""
    return render_template('staff_billing.html')

@app.route('/customers')
@staff_required
def customers():
    """Customers page for staff"""
    return render_template('customers.html')
    
@app.route('/reports')
@staff_required
def reports():
    """Reports page for staff"""
    return render_template('reports.html')

# ============================================
# ADMIN DASHBOARD ROUTES
# Add these routes to your staff_app.py file before the "if __name__ == '__main__':" line
# ============================================

@app.route('/admin_dashboard')
@admin_required
def admin_dashboard():
    """Admin dashboard with overview of all stores"""
    return render_template('admin_dashboard.html')

@app.route('/api/admin/dashboard-stats')
@admin_required
def get_admin_dashboard_stats():
    """Get dashboard statistics for admin"""
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        stats = {}
        
        # 1. Total number of stores
        cursor.execute("SELECT COUNT(*) as total FROM stores WHERE is_active = 1")
        stats['total_stores'] = cursor.fetchone()['total']
        
        # 2. Total number of employees
        cursor.execute("SELECT COUNT(*) as total FROM users WHERE is_active = 1")
        stats['total_employees'] = cursor.fetchone()['total']
        
        # 3. Today's sales (all stores)
        cursor.execute("""
            SELECT COALESCE(SUM(total_amount), 0) as total 
            FROM bills 
            WHERE DATE(created_at) = CURDATE()
        """)
        stats['today_sales'] = float(cursor.fetchone()['total'])
        
        # 4. Total products
        cursor.execute("SELECT COUNT(*) as total FROM products WHERE is_active = 1")
        stats['total_products'] = cursor.fetchone()['total']
        
        # 5. Total sales (all time, all stores)
        cursor.execute("""
            SELECT COALESCE(SUM(total_amount), 0) as total 
            FROM bills
        """)
        stats['total_sales'] = float(cursor.fetchone()['total'])
        
        # Store-wise today's sales
        cursor.execute("""
            SELECT s.store_name, COALESCE(SUM(b.total_amount), 0) as sales
            FROM stores s
            LEFT JOIN bills b ON s.store_id = b.store_id 
                AND DATE(b.created_at) = CURDATE()
            WHERE s.is_active = 1
            GROUP BY s.store_id, s.store_name
            ORDER BY sales DESC
        """)
        stats['store_sales_today'] = cursor.fetchall()
        
        cursor.close()
        return jsonify(stats)
        
    except Error as e:
        print(f"Error fetching dashboard stats: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/recent-bills')
@admin_required
def get_admin_recent_bills():
    """Get recent bills from all stores"""
    connection = None
    cursor = None
    try:
        limit = request.args.get('limit', 10, type=int)
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT 
                b.bill_id,
                b.bill_number,
                b.total_amount,
                b.created_at,
                s.store_name,
                u.full_name as staff_name,
                b.customer_name,
                b.customer_contact
            FROM bills b
            INNER JOIN stores s ON b.store_id = s.store_id
            INNER JOIN users u ON b.staff_id = u.user_id
            ORDER BY b.created_at DESC
            LIMIT %s
        """, (limit,))
        
        bills = cursor.fetchall()
        
        # Convert datetime to string for JSON serialization
        for bill in bills:
            if bill['created_at']:
                bill['created_at'] = bill['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.close()
        return jsonify(bills)
        
    except Error as e:
        print(f"Error fetching recent bills: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/top-products')
@admin_required
def get_admin_top_products():
    """Get top selling products across all stores"""
    connection = None
    cursor = None
    try:
        limit = request.args.get('limit', 10, type=int)
        period = request.args.get('period', 'all')  # all, today, week, month
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Build date filter
        date_filter = ""
        if period == 'today':
            date_filter = "AND DATE(b.created_at) = CURDATE()"
        elif period == 'week':
            date_filter = "AND b.created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)"
        elif period == 'month':
            date_filter = "AND b.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)"
        
        cursor.execute(f"""
            SELECT 
                p.product_id,
                p.name as product_name,
                p.brand,
                p.category,
                SUM(bi.quantity) as total_quantity,
                SUM(bi.total) as total_sales,
                COUNT(DISTINCT b.bill_id) as order_count
            FROM bill_items bi
            INNER JOIN bills b ON bi.bill_id = b.bill_id
            INNER JOIN products p ON bi.product_id = p.product_id
            WHERE 1=1 {date_filter}
            GROUP BY p.product_id, p.name, p.brand, p.category
            ORDER BY total_sales DESC
            LIMIT %s
        """, (limit,))
        
        products = cursor.fetchall()
        
        cursor.close()
        return jsonify(products)
        
    except Error as e:
        print(f"Error fetching top products: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/sales-chart')
@admin_required
def get_admin_sales_chart():
    """Get sales data for charts"""
    connection = None
    cursor = None
    try:
        period = request.args.get('period', 'week')  # week, month, year
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        if period == 'week':
            cursor.execute("""
                SELECT 
                    DATE(created_at) as date,
                    COALESCE(SUM(total_amount), 0) as sales
                FROM bills
                WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                GROUP BY DATE(created_at)
                ORDER BY date
            """)
        elif period == 'month':
            cursor.execute("""
                SELECT 
                    DATE(created_at) as date,
                    COALESCE(SUM(total_amount), 0) as sales
                FROM bills
                WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                GROUP BY DATE(created_at)
                ORDER BY date
            """)
        else:  # year
            cursor.execute("""
                SELECT 
                    DATE_FORMAT(created_at, '%Y-%m') as date,
                    COALESCE(SUM(total_amount), 0) as sales
                FROM bills
                WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
                GROUP BY DATE_FORMAT(created_at, '%Y-%m')
                ORDER BY date
            """)
        
        chart_data = cursor.fetchall()
        
        # Convert date to string
        for item in chart_data:
            if isinstance(item['date'], datetime):
                item['date'] = item['date'].strftime('%Y-%m-%d')
        
        cursor.close()
        return jsonify(chart_data)
        
    except Error as e:
        print(f"Error fetching sales chart data: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            
# ============================================
# STORES & USERS MANAGEMENT ROUTES
# Add these routes to your staff_app.py file
# ============================================

@app.route('/admin')
@admin_required
def admin():
    """Stores and users management page"""
    return render_template('stores_users.html')

@app.route('/api/admin/stores')
@admin_required
def get_stores():
    """Get all stores"""
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT 
                store_id,
                store_name,
                address,
                contact,
                email,
                is_active,
                created_at,
                updated_at
            FROM stores
            ORDER BY store_name
        """)
        
        stores = cursor.fetchall()
        
        # Convert datetime to string
        for store in stores:
            if store['created_at']:
                store['created_at'] = store['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            if store['updated_at']:
                store['updated_at'] = store['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.close()
        return jsonify(stores)
        
    except Error as e:
        print(f"Error fetching stores: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/users')
@admin_required
def get_users():
    """Get all users with their store information"""
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT 
                u.user_id,
                u.username,
                u.full_name,
                u.email,
                u.contact,
                u.role,
                u.store_id,
                s.store_name,
                u.is_active,
                u.created_at,
                u.updated_at
            FROM users u
            LEFT JOIN stores s ON u.store_id = s.store_id
            ORDER BY u.role, u.full_name
        """)
        
        users = cursor.fetchall()
        
        # Convert datetime to string
        for user in users:
            if user['created_at']:
                user['created_at'] = user['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            if user['updated_at']:
                user['updated_at'] = user['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.close()
        return jsonify(users)
        
    except Error as e:
        print(f"Error fetching users: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/store/<int:store_id>')
@admin_required
def get_store(store_id):
    """Get single store details"""
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT 
                store_id,
                store_name,
                address,
                contact,
                email,
                is_active,
                created_at,
                updated_at
            FROM stores
            WHERE store_id = %s
        """, (store_id,))
        
        store = cursor.fetchone()
        
        if not store:
            return jsonify({'error': 'Store not found'}), 404
        
        # Convert datetime to string
        if store['created_at']:
            store['created_at'] = store['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        if store['updated_at']:
            store['updated_at'] = store['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.close()
        return jsonify(store)
        
    except Error as e:
        print(f"Error fetching store: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/user/<int:user_id>')
@admin_required
def get_user(user_id):
    """Get single user details"""
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT 
                u.user_id,
                u.username,
                u.full_name,
                u.email,
                u.contact,
                u.role,
                u.store_id,
                s.store_name,
                u.is_active,
                u.created_at,
                u.updated_at
            FROM users u
            LEFT JOIN stores s ON u.store_id = s.store_id
            WHERE u.user_id = %s
        """, (user_id,))
        
        user = cursor.fetchone()
        
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        # Convert datetime to string
        if user['created_at']:
            user['created_at'] = user['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        if user['updated_at']:
            user['updated_at'] = user['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.close()
        return jsonify(user)
        
    except Error as e:
        print(f"Error fetching user: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/store', methods=['POST'])
@admin_required
def create_store():
    """Create a new store"""
    connection = None
    cursor = None
    try:
        data = request.json
        
        # Validate required fields
        if not data.get('store_name'):
            return jsonify({'error': 'Store name is required'}), 400
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        cursor.execute("""
            INSERT INTO stores (store_name, address, contact, email, is_active)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            data.get('store_name'),
            data.get('address'),
            data.get('contact'),
            data.get('email'),
            data.get('is_active', 1)
        ))
        
        connection.commit()
        store_id = cursor.lastrowid
        
        cursor.close()
        return jsonify({
            'success': True,
            'store_id': store_id,
            'message': 'Store created successfully'
        }), 201
        
    except Error as e:
        if connection:
            connection.rollback()
        print(f"Error creating store: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/store/<int:store_id>', methods=['PUT'])
@admin_required
def update_store(store_id):
    """Update an existing store"""
    connection = None
    cursor = None
    try:
        data = request.json
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Build update query dynamically based on provided fields
        update_fields = []
        values = []
        
        if 'store_name' in data:
            update_fields.append('store_name = %s')
            values.append(data['store_name'])
        if 'address' in data:
            update_fields.append('address = %s')
            values.append(data['address'])
        if 'contact' in data:
            update_fields.append('contact = %s')
            values.append(data['contact'])
        if 'email' in data:
            update_fields.append('email = %s')
            values.append(data['email'])
        if 'is_active' in data:
            update_fields.append('is_active = %s')
            values.append(data['is_active'])
        
        if not update_fields:
            return jsonify({'error': 'No fields to update'}), 400
        
        values.append(store_id)
        
        cursor.execute(f"""
            UPDATE stores 
            SET {', '.join(update_fields)}
            WHERE store_id = %s
        """, values)
        
        connection.commit()
        
        cursor.close()
        return jsonify({
            'success': True,
            'message': 'Store updated successfully'
        })
        
    except Error as e:
        if connection:
            connection.rollback()
        print(f"Error updating store: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/store/<int:store_id>/toggle-status', methods=['PUT'])
@admin_required
def toggle_store_status(store_id):
    """Toggle store active/inactive status"""
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get current status
        cursor.execute("SELECT is_active FROM stores WHERE store_id = %s", (store_id,))
        store = cursor.fetchone()
        
        if not store:
            return jsonify({'error': 'Store not found'}), 404
        
        # Toggle status
        new_status = 0 if store['is_active'] else 1
        
        cursor.execute("""
            UPDATE stores 
            SET is_active = %s
            WHERE store_id = %s
        """, (new_status, store_id))
        
        connection.commit()
        
        cursor.close()
        return jsonify({
            'success': True,
            'is_active': new_status,
            'message': f'Store {"activated" if new_status else "deactivated"} successfully'
        })
        
    except Error as e:
        if connection:
            connection.rollback()
        print(f"Error toggling store status: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/user/<int:user_id>/toggle-status', methods=['PUT'])
@admin_required
def toggle_user_status(user_id):
    """Toggle user active/inactive status"""
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get current status
        cursor.execute("SELECT is_active FROM users WHERE user_id = %s", (user_id,))
        user = cursor.fetchone()
        
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        # Toggle status
        new_status = 0 if user['is_active'] else 1
        
        cursor.execute("""
            UPDATE users 
            SET is_active = %s
            WHERE user_id = %s
        """, (new_status, user_id))
        
        connection.commit()
        
        cursor.close()
        return jsonify({
            'success': True,
            'is_active': new_status,
            'message': f'User {"activated" if new_status else "deactivated"} successfully'
        })
        
    except Error as e:
        if connection:
            connection.rollback()
        print(f"Error toggling user status: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/user', methods=['POST'])
@admin_required
def create_user():
    """Create a new user"""
    connection = None
    cursor = None
    try:
        data = request.json
        
        # Validate required fields
        if not all(k in data for k in ['username', 'password', 'full_name', 'role']):
            return jsonify({'error': 'Missing required fields'}), 400
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Hash password (you should use proper password hashing like bcrypt)
        # For now, using simple hash - REPLACE WITH PROPER HASHING
        
        
        cursor.execute("""
            INSERT INTO users (username, password_hash, full_name, email, contact, role, store_id, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data['username'],
            data['password'],
            data['full_name'],
            data.get('email'),
            data.get('contact'),
            data['role'],
            data.get('store_id'),
            data.get('is_active', 1)
        ))
        
        connection.commit()
        user_id = cursor.lastrowid
        
        cursor.close()
        return jsonify({
            'success': True,
            'user_id': user_id,
            'message': 'User created successfully'
        }), 201
        
    except Error as e:
        if connection:
            connection.rollback()
        print(f"Error creating user: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/user/<int:user_id>', methods=['PUT'])
@admin_required
def update_user(user_id):
    """Update an existing user"""
    connection = None
    cursor = None
    try:
        data = request.json
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Build update query dynamically
        update_fields = []
        values = []
        
        if 'full_name' in data:
            update_fields.append('full_name = %s')
            values.append(data['full_name'])
        if 'email' in data:
            update_fields.append('email = %s')
            values.append(data['email'])
        if 'contact' in data:
            update_fields.append('contact = %s')
            values.append(data['contact'])
        if 'role' in data:
            update_fields.append('role = %s')
            values.append(data['role'])
        if 'store_id' in data:
            update_fields.append('store_id = %s')
            values.append(data['store_id'])
        if 'is_active' in data:
            update_fields.append('is_active = %s')
            values.append(data['is_active'])
        if 'password' in data and data['password']:
            # Hash new password
            import hashlib
            password_hash = hashlib.sha256(data['password'].encode()).hexdigest()
            update_fields.append('password_hash = %s')
            values.append(password_hash)
        
        if not update_fields:
            return jsonify({'error': 'No fields to update'}), 400
        
        values.append(user_id)
        
        cursor.execute(f"""
            UPDATE users 
            SET {', '.join(update_fields)}
            WHERE user_id = %s
        """, values)
        
        connection.commit()
        
        cursor.close()
        return jsonify({
            'success': True,
            'message': 'User updated successfully'
        })
        
    except Error as e:
        if connection:
            connection.rollback()
        print(f"Error updating user: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

# ============================================
# INVENTORY MANAGEMENT ROUTES
# Add these routes to your staff_app.py file
# ============================================

@app.route('/admin_inventory')
@admin_required
def admin_inventory():
    """Inventory management page"""
    return render_template('admin_inventory.html')

@app.route('/api/admin/inventory')
@admin_required
def get_inventory():
    """Get inventory with optional filters"""
    connection = None
    cursor = None
    try:
        store_id = request.args.get('store_id', type=int)
        search = request.args.get('search', '').strip()
        status = request.args.get('status', '').strip()
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Build query with filters
        query = """
            SELECT 
                i.inventory_id,
                i.store_id,
                i.product_id,
                i.quantity,
                i.min_stock_level,
                i.last_updated,
                p.name as product_name,
                p.brand,
                p.category,
                p.unit,
                s.store_name
            FROM inventory i
            INNER JOIN products p ON i.product_id = p.product_id
            INNER JOIN stores s ON i.store_id = s.store_id
            WHERE p.is_active = 1 AND s.is_active = 1
        """
        
        params = []
        
        if store_id:
            query += " AND i.store_id = %s"
            params.append(store_id)
        
        if search:
            query += " AND (p.name LIKE %s OR p.brand LIKE %s OR p.category LIKE %s)"
            search_param = f"%{search}%"
            params.extend([search_param, search_param, search_param])
        
        # Apply status filter after fetching
        query += " ORDER BY s.store_name, p.name"
        
        cursor.execute(query, params)
        inventory = cursor.fetchall()
        
        # Filter by status
        if status:
            filtered_inventory = []
            for item in inventory:
                qty = float(item['quantity'])
                min_level = float(item['min_stock_level'])
                
                if status == 'out-stock' and qty <= 0:
                    filtered_inventory.append(item)
                elif status == 'low-stock' and 0 < qty <= min_level:
                    filtered_inventory.append(item)
                elif status == 'in-stock' and qty > min_level:
                    filtered_inventory.append(item)
            
            inventory = filtered_inventory
        
        # Convert datetime to string
        for item in inventory:
            if item['last_updated']:
                item['last_updated'] = item['last_updated'].strftime('%Y-%m-%d %H:%M:%S')
            item['quantity'] = float(item['quantity'])
            item['min_stock_level'] = float(item['min_stock_level'])
        
        cursor.close()
        return jsonify(inventory)
        
    except Error as e:
        print(f"Error fetching inventory: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/inventory/store/<int:store_id>')
@admin_required
def get_inventory_by_store(store_id):
    """Get inventory for a specific store"""
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT 
                i.inventory_id,
                i.store_id,
                i.product_id,
                i.quantity,
                i.min_stock_level,
                p.name as product_name,
                p.brand,
                p.category,
                p.unit
            FROM inventory i
            INNER JOIN products p ON i.product_id = p.product_id
            WHERE i.store_id = %s AND p.is_active = 1
            ORDER BY p.name
        """, (store_id,))
        
        inventory = cursor.fetchall()
        
        for item in inventory:
            item['quantity'] = float(item['quantity'])
            item['min_stock_level'] = float(item['min_stock_level'])
        
        cursor.close()
        return jsonify(inventory)
        
    except Error as e:
        print(f"Error fetching store inventory: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/inventory/adjust', methods=['POST'])
@admin_required
def adjust_inventory():
    """Adjust inventory stock levels (Admin only)"""
    connection = None
    cursor = None
    try:
        data = request.json

        # ✅ Validate required fields
        required_fields = ['store_id', 'product_id', 'movement_type', 'quantity']
        if not all(k in data for k in required_fields):
            return jsonify({'error': 'Missing required fields'}), 400

        store_id = data['store_id']
        product_id = data['product_id']
        movement_type = data['movement_type']
        quantity = float(data['quantity'])

        # ✅ Notes: default only if admin didn't provide one
        notes = data.get('notes')
        if not notes or notes.strip() == "":
            notes = "Admin Adjusted"

        if quantity <= 0:
            return jsonify({'error': 'Quantity must be greater than 0'}), 400

        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = connection.cursor(dictionary=True)

        # 🔹 Fetch current stock
        cursor.execute("""
            SELECT quantity FROM inventory 
            WHERE store_id = %s AND product_id = %s
        """, (store_id, product_id))
        inventory = cursor.fetchone()

        if not inventory:
            # Create inventory record if missing
            cursor.execute("""
                INSERT INTO inventory (store_id, product_id, quantity, min_stock_level)
                VALUES (%s, %s, 0, 0)
            """, (store_id, product_id))
            current_qty = 0
        else:
            current_qty = float(inventory['quantity'])

        # 🔹 Determine new quantity
        if movement_type == 'in':
            new_qty = current_qty + quantity
        elif movement_type == 'out':
            new_qty = current_qty - quantity
            if new_qty < 0:
                return jsonify({'error': 'Insufficient stock for this operation'}), 400
        elif movement_type == 'adjustment':
            new_qty = quantity
        else:
            return jsonify({'error': 'Invalid movement type'}), 400

        # 🔹 Update inventory
        cursor.execute("""
            UPDATE inventory 
            SET quantity = %s,
                last_updated = NOW(),
                last_modified_by = %s
            WHERE store_id = %s AND product_id = %s
        """, (new_qty, session.get('user_id'), store_id, product_id))

        # 🔹 Record movement history
        cursor.execute("""
            INSERT INTO inventory_movements 
            (store_id, product_id, movement_type, quantity, previous_stock, new_stock, notes, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            store_id, product_id, movement_type, quantity,
            current_qty, new_qty, notes, session.get('user_id')
        ))

        connection.commit()

        return jsonify({
            'success': True,
            'message': 'Inventory adjusted successfully',
            'previous_stock': current_qty,
            'new_stock': new_qty,
            'notes': notes
        })

    except Error as e:
        if connection:
            connection.rollback()
        print(f"Error adjusting inventory: {e}")
        return jsonify({'error': str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/inventory/<int:inventory_id>', methods=['PUT'])
@admin_required
def update_inventory(inventory_id):
    """Update inventory min stock level"""
    connection = None
    cursor = None
    try:
        data = request.json
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        update_fields = []
        values = []
        
        if 'min_stock_level' in data:
            update_fields.append('min_stock_level = %s')
            values.append(data['min_stock_level'])
        
        if 'notes' in data:
            update_fields.append('notes = %s')
            values.append(data['notes'])
        
        if not update_fields:
            return jsonify({'error': 'No fields to update'}), 400
        
        update_fields.append('last_modified_by = %s')
        values.append(session.get('user_id'))
        
        values.append(inventory_id)
        
        cursor.execute(f"""
            UPDATE inventory 
            SET {', '.join(update_fields)}
            WHERE inventory_id = %s
        """, values)
        
        connection.commit()
        
        cursor.close()
        return jsonify({
            'success': True,
            'message': 'Inventory updated successfully'
        })
        
    except Error as e:
        if connection:
            connection.rollback()
        print(f"Error updating inventory: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/inventory/movements')
@admin_required
def get_inventory_movements():
    """Get inventory movement history"""
    connection = None
    cursor = None
    try:
        store_id = request.args.get('store_id', type=int)
        product_id = request.args.get('product_id', type=int)
        limit = request.args.get('limit', 50, type=int)
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        query = """
            SELECT 
                im.movement_id,
                im.store_id,
                im.product_id,
                im.movement_type,
                im.quantity,
                im.previous_stock,
                im.new_stock,
                im.notes,
                im.created_at,
                p.name as product_name,
                s.store_name,
                u.full_name as created_by_name
            FROM inventory_movements im
            INNER JOIN products p ON im.product_id = p.product_id
            INNER JOIN stores s ON im.store_id = s.store_id
            LEFT JOIN users u ON im.created_by = u.user_id
            WHERE 1=1
        """
        
        params = []
        
        if store_id:
            query += " AND im.store_id = %s"
            params.append(store_id)
        
        if product_id:
            query += " AND im.product_id = %s"
            params.append(product_id)
        
        query += " ORDER BY im.created_at DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        movements = cursor.fetchall()
        
        # Convert datetime to string
        for movement in movements:
            if movement['created_at']:
                movement['created_at'] = movement['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            movement['quantity'] = float(movement['quantity'])
            movement['previous_stock'] = float(movement['previous_stock'])
            movement['new_stock'] = float(movement['new_stock'])
        
        cursor.close()
        return jsonify(movements)
        
    except Error as e:
        print(f"Error fetching inventory movements: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@app.route('/api/admin/inventory/low-stock')
@admin_required
def get_low_stock_items():
    """Get items with low stock"""
    connection = None
    cursor = None
    try:
        store_id = request.args.get('store_id', type=int)
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        query = """
            SELECT 
                i.inventory_id,
                i.store_id,
                i.product_id,
                i.quantity,
                i.min_stock_level,
                p.name as product_name,
                p.brand,
                p.category,
                s.store_name
            FROM inventory i
            INNER JOIN products p ON i.product_id = p.product_id
            INNER JOIN stores s ON i.store_id = s.store_id
            WHERE i.quantity <= i.min_stock_level 
            AND p.is_active = 1 
            AND s.is_active = 1
        """
        
        params = []
        if store_id:
            query += " AND i.store_id = %s"
            params.append(store_id)
        
        query += " ORDER BY i.quantity ASC"
        
        cursor.execute(query, params)
        items = cursor.fetchall()
        
        for item in items:
            item['quantity'] = float(item['quantity'])
            item['min_stock_level'] = float(item['min_stock_level'])
        
        cursor.close()
        return jsonify(items)
        
    except Error as e:
        print(f"Error fetching low stock items: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            
            
"""
============================================
ADMIN PRODUCTS MANAGEMENT - BACKEND CODE
============================================

INSTRUCTIONS:
1. Copy ALL the code below (starting from the first @app.route)
2. Open your staff_app.py file
3. Find line ~3881 (after your existing admin routes)
4. Paste this entire code block there
5. Save the file
6. Restart Flask

This provides:
- Add products
- Edit products
- Activate/Deactivate products
- Filtering and search
============================================
"""

# ============================================
# ROUTE 1: GET ALL PRODUCTS (with filters)
# ============================================
@app.route('/api/admin/products')
@admin_required
def get_admin_products():
    """Get all products for admin with filtering options"""
    try:
        category = request.args.get('category', '')
        brand = request.args.get('brand', '')
        search = request.args.get('search', '')
        status = request.args.get('status', '')
        
        query = """
            SELECT 
                p.product_id,
                p.name as product_name,
                p.brand,
                p.category,
                p.unit,
                p.description,
                p.is_active,
                p.created_at,
                p.updated_at,
                GROUP_CONCAT(DISTINCT s.store_name ORDER BY s.store_name SEPARATOR ', ') as stores,
                GROUP_CONCAT(DISTINCT s.store_id ORDER BY s.store_id) as store_ids,
                SUM(COALESCE(i.quantity, 0)) as total_stock
            FROM products p
            LEFT JOIN inventory i ON p.product_id = i.product_id
            LEFT JOIN stores s ON i.store_id = s.store_id
            WHERE 1=1
        """
        
        params = []
        
        if category:
            query += " AND p.category = %s"
            params.append(category)
        
        if brand:
            query += " AND p.brand = %s"
            params.append(brand)
        
        if search:
            query += " AND (p.name LIKE %s OR p.brand LIKE %s OR p.category LIKE %s)"
            search_param = f'%{search}%'
            params.extend([search_param, search_param, search_param])
        
        if status == 'active':
            query += " AND p.is_active = 1"
        elif status == 'inactive':
            query += " AND p.is_active = 0"
        
        query += """
            GROUP BY p.product_id, p.name, p.brand, p.category, p.unit, 
                     p.description, p.is_active, p.created_at, p.updated_at
            ORDER BY p.brand, p.name
        """
        
        products = execute_query(query, tuple(params), fetch_all=True) or []
        
        # Process the results
        for product in products:
            product['total_stock'] = float(product.get('total_stock', 0) or 0)
            product['is_active'] = bool(product.get('is_active', 1))
            
            # Convert store_ids string to list
            if product.get('store_ids'):
                product['store_ids'] = [int(sid) for sid in product['store_ids'].split(',')]
            else:
                product['store_ids'] = []
            
            # Convert stores to list
            if product.get('stores'):
                product['stores'] = product['stores'].split(', ')
            else:
                product['stores'] = []
        
        return jsonify(products)
        
    except Exception as e:
        print(f"Error in get_admin_products: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# ============================================
# ROUTE 2: GET SINGLE PRODUCT DETAILS
# ============================================
@app.route('/api/admin/products/<int:product_id>')
@admin_required
def get_admin_product_details(product_id):
    """Get detailed information about a specific product"""
    try:
        query = """
            SELECT 
                p.product_id,
                p.name as product_name,
                p.brand,
                p.category,
                p.unit,
                p.description,
                p.is_active,
                p.created_at,
                p.updated_at,
                GROUP_CONCAT(DISTINCT s.store_name ORDER BY s.store_name SEPARATOR ', ') as stores,
                GROUP_CONCAT(DISTINCT s.store_id ORDER BY s.store_id) as store_ids
            FROM products p
            LEFT JOIN inventory i ON p.product_id = i.product_id
            LEFT JOIN stores s ON i.store_id = s.store_id
            WHERE p.product_id = %s
            GROUP BY p.product_id
        """
        
        product = execute_query(query, (product_id,), fetch_one=True)
        
        if not product:
            return jsonify({'error': 'Product not found'}), 404
        
        # Process the result
        product['is_active'] = bool(product.get('is_active', 1))
        
        # Convert store_ids string to list
        if product.get('store_ids'):
            product['store_ids'] = [int(sid) for sid in product['store_ids'].split(',')]
        else:
            product['store_ids'] = []
        
        # Convert stores to list
        if product.get('stores'):
            product['stores'] = product['stores'].split(', ')
        else:
            product['stores'] = []
        
        return jsonify(product)
        
    except Exception as e:
        print(f"Error in get_admin_product_details: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# ============================================
# ROUTE 3: CREATE NEW PRODUCT
# ============================================
@app.route('/api/admin/products', methods=['POST'])
@admin_required
def create_admin_product():
    """Create a new product"""
    try:
        data = request.get_json()
        
        product_name = data.get('product_name', '').strip()
        brand = data.get('brand', '').strip()
        category = data.get('category', '').strip()
        unit = data.get('unit', 'pcs')
        description = data.get('description', '').strip()
        store_ids = data.get('store_ids', [])
        
        # Validate required fields
        if not product_name or not brand or not category:
            return jsonify({
                'success': False,
                'error': 'Product name, brand, and category are required'
            }), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Insert product - always active when created
            insert_query = """
                INSERT INTO products (name, brand, category, unit, description, is_active)
                VALUES (%s, %s, %s, %s, %s, 1)
            """
            cursor.execute(insert_query, (product_name, brand, category, unit, description))
            product_id = cursor.lastrowid
            
            # If store_ids are provided, create inventory entries
            if store_ids:
                for store_id in store_ids:
                    inventory_query = """
                        INSERT INTO inventory (store_id, product_id, quantity, min_stock_level)
                        VALUES (%s, %s, 0, 10)
                        ON DUPLICATE KEY UPDATE product_id = product_id
                    """
                    cursor.execute(inventory_query, (store_id, product_id))
            
            connection.commit()
            
            return jsonify({
                'success': True,
                'message': f'Product "{product_name}" created successfully',
                'product_id': product_id
            })
            
        except Exception as e:
            connection.rollback()
            raise e
        finally:
            cursor.close()
            connection.close()
            
    except Exception as e:
        print(f"Error in create_admin_product: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================
# ROUTE 4: UPDATE EXISTING PRODUCT
# ============================================
@app.route('/api/admin/products/<int:product_id>', methods=['PUT'])
@admin_required
def update_admin_product(product_id):
    """Update an existing product"""
    try:
        data = request.get_json()
        
        product_name = data.get('product_name', '').strip()
        brand = data.get('brand', '').strip()
        category = data.get('category', '').strip()
        unit = data.get('unit', 'pcs')
        description = data.get('description', '').strip()
        store_ids = data.get('store_ids', [])
        
        # Validate required fields
        if not product_name or not brand or not category:
            return jsonify({
                'success': False,
                'error': 'Product name, brand, and category are required'
            }), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Update product (don't change is_active here)
            update_query = """
                UPDATE products
                SET name = %s, brand = %s, category = %s, unit = %s, 
                    description = %s, updated_at = CURRENT_TIMESTAMP
                WHERE product_id = %s
            """
            cursor.execute(update_query, (product_name, brand, category, unit, description, product_id))
            
            # Get current store associations
            cursor.execute("SELECT store_id FROM inventory WHERE product_id = %s", (product_id,))
            current_stores = {row['store_id'] for row in cursor.fetchall()}
            
            new_stores = set(store_ids)
            
            # Add new stores
            stores_to_add = new_stores - current_stores
            for store_id in stores_to_add:
                inventory_query = """
                    INSERT INTO inventory (store_id, product_id, quantity, min_stock_level)
                    VALUES (%s, %s, 0, 10)
                """
                cursor.execute(inventory_query, (store_id, product_id))
            
            connection.commit()
            
            return jsonify({
                'success': True,
                'message': f'Product "{product_name}" updated successfully'
            })
            
        except Exception as e:
            connection.rollback()
            raise e
        finally:
            cursor.close()
            connection.close()
            
    except Exception as e:
        print(f"Error in update_admin_product: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================
# ROUTE 5: ACTIVATE/DEACTIVATE PRODUCT
# ============================================
@app.route('/api/admin/products/<int:product_id>/toggle-status', methods=['PUT'])
@admin_required
def toggle_product_status(product_id):
    """Activate or deactivate a product (instead of deleting)"""
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Get current product status and name
            cursor.execute(
                "SELECT name, is_active FROM products WHERE product_id = %s", 
                (product_id,)
            )
            product = cursor.fetchone()
            
            if not product:
                return jsonify({'success': False, 'error': 'Product not found'}), 404
            
            product_name = product['name']
            current_status = product['is_active']
            
            # Toggle status
            new_status = 0 if current_status else 1
            
            cursor.execute(
                "UPDATE products SET is_active = %s, updated_at = CURRENT_TIMESTAMP WHERE product_id = %s",
                (new_status, product_id)
            )
            connection.commit()
            
            status_text = 'activated' if new_status else 'deactivated'
            
            return jsonify({
                'success': True,
                'message': f'Product "{product_name}" has been {status_text}',
                'is_active': bool(new_status)
            })
            
        except Exception as e:
            connection.rollback()
            raise e
        finally:
            cursor.close()
            connection.close()
            
    except Exception as e:
        print(f"Error in toggle_product_status: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================
# ROUTE 6: GET FILTER OPTIONS
# ============================================
@app.route('/api/admin/products/filters')
@admin_required
def get_product_filters():
    """Get unique categories and brands for filtering"""
    try:
        # Get unique categories
        categories_query = """
            SELECT DISTINCT category 
            FROM products 
            WHERE category IS NOT NULL AND category != ''
            ORDER BY category
        """
        categories = execute_query(categories_query, fetch_all=True) or []
        
        # Get unique brands
        brands_query = """
            SELECT DISTINCT brand 
            FROM products 
            WHERE brand IS NOT NULL AND brand != ''
            ORDER BY brand
        """
        brands = execute_query(brands_query, fetch_all=True) or []
        
        # Get active stores
        stores_query = """
            SELECT store_id, store_name 
            FROM stores 
            WHERE is_active = 1
            ORDER BY store_name
        """
        stores = execute_query(stores_query, fetch_all=True) or []
        
        return jsonify({
            'categories': [c['category'] for c in categories],
            'brands': [b['brand'] for b in brands],
            'stores': stores
        })
        
    except Exception as e:
        print(f"Error in get_product_filters: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# ============================================
# ROUTE 7: SERVE ADMIN PRODUCTS PAGE
# ============================================
@app.route('/admin/products')
@admin_required
def admin_products_page():
    """Render the admin products management page"""
    return render_template('admin_products.html')


# ============================================
# END OF ADMIN PRODUCTS ROUTES
# ============================================

# ============================================
# ADMIN REPORTS ROUTES
# Add these routes to your staff_app.py file
# ============================================

# Route: GET /admin/reports - Serve admin reports page
@app.route('/admin/reports')
@admin_required
def admin_reports_page():
    """Render the admin reports page"""
    return render_template('admin_reports.html')


# Route: GET /api/admin/reports/sales - Sales report (all stores)
@app.route('/api/admin/reports/sales', methods=['GET'])
@admin_required
def api_admin_reports_sales():
    """
    Get sales report for admin across all stores or specific store
    Query params: start_date, end_date, store_id (optional)
    """
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        store_id = request.args.get('store_id')
        
        if not start_date or not end_date:
            return jsonify({'error': 'start_date and end_date are required'}), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Base query
            query = """
                SELECT 
                    DATE(b.created_at) as date,
                    COUNT(DISTINCT b.bill_id) as total_bills,
                    COALESCE(SUM(b.total_amount), 0) as total_sales,
                    COALESCE(SUM(
                        CASE 
                            WHEN JSON_EXTRACT(b.payment_split, '$.cash') IS NOT NULL 
                            THEN CAST(JSON_EXTRACT(b.payment_split, '$.cash') AS DECIMAL(10,2))
                            ELSE 0 
                        END
                    ), 0) as cash_sales,
                    COALESCE(SUM(
                        CASE 
                            WHEN JSON_EXTRACT(b.payment_split, '$.upi') IS NOT NULL 
                            THEN CAST(JSON_EXTRACT(b.payment_split, '$.upi') AS DECIMAL(10,2))
                            ELSE 0 
                        END
                    ), 0) as upi_sales,
                    COALESCE(SUM(
                        CASE 
                            WHEN JSON_EXTRACT(b.payment_split, '$.credit') IS NOT NULL 
                            THEN CAST(JSON_EXTRACT(b.payment_split, '$.credit') AS DECIMAL(10,2))
                            ELSE 0 
                        END
                    ), 0) as credit_sales
                FROM bills b
                WHERE DATE(b.created_at) BETWEEN %s AND %s
            """
            
            params = [start_date, end_date]
            
            # Add store filter if provided
            if store_id:
                query += " AND b.store_id = %s"
                params.append(store_id)
            
            query += """
                GROUP BY DATE(b.created_at)
                ORDER BY date DESC
            """
            
            cursor.execute(query, tuple(params))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            # Convert datetime objects to strings
            for row in results:
                if 'date' in row and row['date']:
                    row['date'] = row['date'].isoformat()
                # Convert Decimal to float
                for key in row:
                    if isinstance(row[key], Decimal):
                        row[key] = float(row[key])
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            raise e
            
    except Exception as e:
        print(f"Error in api_admin_reports_sales: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# Route: GET /api/admin/reports/products - Product sales report
@app.route('/api/admin/reports/products', methods=['GET'])
@admin_required
def api_admin_reports_products():
    """
    Get product-wise sales report for admin
    Query params: start_date, end_date, store_id (optional)
    """
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        store_id = request.args.get('store_id')
        
        if not start_date or not end_date:
            return jsonify({'error': 'start_date and end_date are required'}), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    p.name as product_name,
                    p.brand,
                    p.category,
                    SUM(bi.quantity) as total_quantity,
                    SUM(bi.total) as total_revenue
                FROM bill_items bi
                JOIN products p ON bi.product_id = p.product_id
                JOIN bills b ON bi.bill_id = b.bill_id
                WHERE DATE(b.created_at) BETWEEN %s AND %s
            """
            
            params = [start_date, end_date]
            
            if store_id:
                query += " AND b.store_id = %s"
                params.append(store_id)
            
            query += """
                GROUP BY p.product_id, p.name, p.brand, p.category
                ORDER BY total_revenue DESC
                LIMIT 50
            """
            
            cursor.execute(query, tuple(params))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            # Convert Decimal to float
            for row in results:
                for key in row:
                    if isinstance(row[key], Decimal):
                        row[key] = float(row[key])
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            raise e
            
    except Exception as e:
        print(f"Error in api_admin_reports_products: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# Route: GET /api/admin/reports/stock - Current stock levels
@app.route('/api/admin/reports/stock', methods=['GET'])
@admin_required
def api_admin_reports_stock():
    """
    Get current stock levels across all stores or specific store
    Query params: store_id (optional)
    """
    try:
        store_id = request.args.get('store_id')
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    p.name as product_name,
                    p.brand,
                    p.category,
                    s.store_name,
                    i.quantity,
                    i.min_stock_level
                FROM inventory i
                JOIN products p ON i.product_id = p.product_id
                JOIN stores s ON i.store_id = s.store_id
                WHERE p.is_active = 1
            """
            
            params = []
            
            if store_id:
                query += " AND i.store_id = %s"
                params.append(store_id)
            
            query += " ORDER BY i.quantity ASC"
            
            if params:
                cursor.execute(query, tuple(params))
            else:
                cursor.execute(query)
                
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            # Convert Decimal to float
            for row in results:
                for key in row:
                    if isinstance(row[key], Decimal):
                        row[key] = float(row[key])
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            raise e
            
    except Exception as e:
        print(f"Error in api_admin_reports_stock: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# Route: GET /api/admin/reports/summary - Overall summary statistics
@app.route('/api/admin/reports/summary', methods=['GET'])
@admin_required
def api_admin_reports_summary():
    """
    Get overall summary statistics
    Query params: start_date, end_date, store_id (optional)
    """
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        store_id = request.args.get('store_id')
        
        if not start_date or not end_date:
            return jsonify({'error': 'start_date and end_date are required'}), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Get sales summary
            sales_query = """
                SELECT 
                    COUNT(DISTINCT bill_id) as total_bills,
                    COALESCE(SUM(total_amount), 0) as total_sales,
                    COALESCE(AVG(total_amount), 0) as avg_bill_value
                FROM bills
                WHERE DATE(created_at) BETWEEN %s AND %s
            """
            
            params = [start_date, end_date]
            
            if store_id:
                sales_query += " AND store_id = %s"
                params.append(store_id)
            
            cursor.execute(sales_query, tuple(params))
            sales_data = cursor.fetchone()
            
            # Get product count
            product_query = """
                SELECT COUNT(DISTINCT p.product_id) as unique_products_sold
                FROM bill_items bi
                JOIN bills b ON bi.bill_id = b.bill_id
                JOIN products p ON bi.product_id = p.product_id
                WHERE DATE(b.created_at) BETWEEN %s AND %s
            """
            
            params = [start_date, end_date]
            
            if store_id:
                product_query += " AND b.store_id = %s"
                params.append(store_id)
            
            cursor.execute(product_query, tuple(params))
            product_data = cursor.fetchone()
            
            cursor.close()
            connection.close()
            
            # Combine results
            summary = {
                'total_bills': sales_data['total_bills'],
                'total_sales': float(sales_data['total_sales']),
                'avg_bill_value': float(sales_data['avg_bill_value']),
                'unique_products_sold': product_data['unique_products_sold']
            }
            
            return jsonify(summary)
            
        except Exception as e:
            if connection:
                connection.close()
            raise e
            
    except Exception as e:
        print(f"Error in api_admin_reports_summary: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# Route: GET /api/admin/reports/low-stock - Low stock alert
@app.route('/api/admin/reports/low-stock', methods=['GET'])
@admin_required
def api_admin_reports_low_stock():
    """Get products with low stock levels"""
    try:
        store_id = request.args.get('store_id')
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    p.name as product_name,
                    p.brand,
                    p.category,
                    s.store_name,
                    i.quantity,
                    i.min_stock_level,
                    (i.min_stock_level - i.quantity) as shortage
                FROM inventory i
                JOIN products p ON i.product_id = p.product_id
                JOIN stores s ON i.store_id = s.store_id
                WHERE p.is_active = 1 
                    AND i.quantity <= i.min_stock_level
            """
            
            params = []
            
            if store_id:
                query += " AND i.store_id = %s"
                params.append(store_id)
            
            query += " ORDER BY shortage DESC"
            
            if params:
                cursor.execute(query, tuple(params))
            else:
                cursor.execute(query)
                
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            # Convert Decimal to float
            for row in results:
                for key in row:
                    if isinstance(row[key], Decimal):
                        row[key] = float(row[key])
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            raise e
            
    except Exception as e:
        print(f"Error in api_admin_reports_low_stock: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

# ============================================
# ADMIN REPORTS - DRILL-DOWN ROUTES
# Add these routes to your staff_app.py file (in addition to the previous admin reports routes)
# ============================================

# Route: GET /api/admin/reports/bills-by-date - Get bills for a specific date (admin version)
@app.route('/api/admin/reports/bills-by-date', methods=['GET'])
@admin_required
def api_admin_reports_bills_by_date():
    """
    Get all bills for a specific date (admin can see all stores)
    Query params: date (YYYY-MM-DD format), store_id (optional)
    """
    try:
        date = request.args.get('date')
        store_id = request.args.get('store_id')
        
        if not date:
            return jsonify({'error': 'date parameter is required'}), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Query to get all bills for the specified date
            query = """
                SELECT 
                    b.bill_id,
                    b.bill_number,
                    b.customer_name,
                    b.customer_contact,
                    b.total_amount,
                    b.payment_split,
                    b.created_at,
                    u.full_name as staff_name,
                    s.store_name
                FROM bills b
                LEFT JOIN users u ON b.staff_id = u.user_id
                LEFT JOIN stores s ON b.store_id = s.store_id
                WHERE DATE(b.created_at) = %s
            """
            
            params = [date]
            
            # Add store filter if provided
            if store_id:
                query += " AND b.store_id = %s"
                params.append(store_id)
            
            query += " ORDER BY b.created_at DESC"
            
            cursor.execute(query, tuple(params))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            # Convert datetime and Decimal objects for JSON serialization
            for row in results:
                if 'created_at' in row and row['created_at']:
                    row['created_at'] = row['created_at'].isoformat()
                for key in row:
                    if isinstance(row[key], Decimal):
                        row[key] = float(row[key])
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            raise e
            
    except Exception as e:
        print(f"Error in api_admin_reports_bills_by_date: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# Route: GET /api/admin/reports/bill-details/<bill_id> - Get bill details (admin version)
@app.route('/api/admin/reports/bill-details/<int:bill_id>', methods=['GET'])
@admin_required
def api_admin_reports_bill_details(bill_id):
    """
    Get detailed information for a specific bill (admin can see all stores)
    Path param: bill_id
    """
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Get bill header information
            bill_query = """
                SELECT 
                    b.bill_id,
                    b.bill_number,
                    b.customer_name,
                    b.customer_contact,
                    b.subtotal,
                    b.discount_type,
                    b.discount_value,
                    b.discount_amount,
                    b.total_amount,
                    b.payment_split,
                    b.notes,
                    b.created_at,
                    u.full_name as staff_name,
                    s.store_name
                FROM bills b
                LEFT JOIN users u ON b.staff_id = u.user_id
                LEFT JOIN stores s ON b.store_id = s.store_id
                WHERE b.bill_id = %s
            """
            
            cursor.execute(bill_query, (bill_id,))
            bill = cursor.fetchone()
            
            if not bill:
                cursor.close()
                connection.close()
                return jsonify({'error': 'Bill not found'}), 404
            
            # Get bill items
            items_query = """
                SELECT 
                    bi.bill_item_id,
                    bi.product_name,
                    bi.quantity,
                    bi.unit_price,
                    bi.item_discount,
                    bi.total
                FROM bill_items bi
                WHERE bi.bill_id = %s
                ORDER BY bi.bill_item_id
            """
            
            cursor.execute(items_query, (bill_id,))
            items = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            # Convert datetime and Decimal objects
            if 'created_at' in bill and bill['created_at']:
                bill['created_at'] = bill['created_at'].isoformat()
            
            for key in bill:
                if isinstance(bill[key], Decimal):
                    bill[key] = float(bill[key])
            
            for item in items:
                for key in item:
                    if isinstance(item[key], Decimal):
                        item[key] = float(item[key])
            
            # Add items to bill object
            bill['items'] = items
            
            return jsonify(bill)
            
        except Exception as e:
            if connection:
                connection.close()
            raise e
            
    except Exception as e:
        print(f"Error in api_admin_reports_bill_details: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# ============================================
# END OF ADMIN DRILL-DOWN ROUTES
# ============================================
# ============================================
# END OF ADMIN REPORTS ROUTES
# ============================================
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

