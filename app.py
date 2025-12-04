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


# ============================================
# AUTHENTICATION ROUTES
# ============================================
"""
@app.route('/')
def index():
    Root route - redirect based on authentication
    if is_logged_in():
        if is_admin():
            return redirect(url_for('admin_dashboard'))
        else:
            return redirect(url_for('staff_dashboard'))
    return redirect(url_for('login'))
    """
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

# ============================================
# ADMIN DASHBOARD
# ============================================

@app.route('/admin_dashboard')
@admin_required
def admin_dashboard():
    """Admin dashboard with statistics"""
    # Get statistics
    total_stores_query = "SELECT COUNT(*) as count FROM stores WHERE is_active = TRUE"
    total_stores = execute_query(total_stores_query, fetch_one=True)
    total_stores = total_stores['count'] if total_stores else 0
    
    total_staff_query = "SELECT COUNT(*) as count FROM users WHERE role = 'staff' AND is_active = TRUE"
    total_staff = execute_query(total_staff_query, fetch_one=True)
    total_staff = total_staff['count'] if total_staff else 0
    
    total_sales_query = "SELECT COALESCE(SUM(total_amount), 0) as total FROM bills"
    total_sales = execute_query(total_sales_query, fetch_one=True)
    total_sales = total_sales['total'] if total_sales else 0
    
    today_sales_query = """
        SELECT COALESCE(SUM(total_amount), 0) as total 
        FROM bills 
        WHERE DATE(created_at) = CURDATE()
    """
    today_sales = execute_query(today_sales_query, fetch_one=True)
    today_sales = today_sales['total'] if today_sales else 0
    
    # Get top products
    top_products_query = """
        SELECT p.name, SUM(bi.quantity) as total_qty, SUM(bi.total) as total_sales
        FROM bill_items bi
        JOIN products p ON bi.product_id = p.product_id
        GROUP BY p.product_id
        ORDER BY total_sales DESC
        LIMIT 5
    """
    top_products = execute_query(top_products_query, fetch_all=True) or []
    
    # Get recent bills
    recent_bills_query = """
        SELECT b.*, s.store_name, u.full_name as staff_name
        FROM bills b
        JOIN stores s ON b.store_id = s.store_id
        JOIN users u ON b.staff_id = u.user_id
        ORDER BY b.created_at DESC
        LIMIT 10
    """
    recent_bills = execute_query(recent_bills_query, fetch_all=True) or []
    
    return render_template('admin_dashboard.html',
                         total_stores=total_stores,
                         total_staff=total_staff,
                         total_sales=total_sales,
                         today_sales=today_sales,
                         top_products=top_products,
                         recent_bills=recent_bills)

# ============================================
# STORE MANAGEMENT (ADMIN)
# ============================================

@app.route('/admin_stores')
@admin_required
def admin_stores():
    """List all stores"""
    query = "SELECT * FROM stores ORDER BY store_name"
    stores = execute_query(query, fetch_all=True) or []
    return render_template('admin_stores.html', stores=stores)

@app.route('/admin/stores/add', methods=['GET', 'POST'])
@admin_required
def admin_add_store():
    """Add new store"""
    if request.method == 'POST':
        store_name = request.form.get('store_name')
        address = request.form.get('address')
        contact = request.form.get('contact')
        email = request.form.get('email')
        
        if not store_name:
            flash('Store name is required.', 'danger')
            return redirect(url_for('admin_add_store'))
        
        query = """
            INSERT INTO stores (store_name, address, contact, email)
            VALUES (%s, %s, %s, %s)
        """
        result = execute_query(query, (store_name, address, contact, email), commit=True)
        
        if result:
            flash('Store added successfully!', 'success')
            return redirect(url_for('admin_stores'))
        else:
            flash('Error adding store.', 'danger')
    
    return render_template('add_store.html')

@app.route('/admin/stores/edit/<int:store_id>', methods=['GET', 'POST'])
@admin_required
def admin_edit_store(store_id):
    """Edit existing store"""
    if request.method == 'POST':
        store_name = request.form.get('store_name')
        address = request.form.get('address')
        contact = request.form.get('contact')
        email = request.form.get('email')
        
        query = """
            UPDATE stores 
            SET store_name = %s, address = %s, contact = %s, email = %s
            WHERE store_id = %s
        """
        result = execute_query(query, (store_name, address, contact, email, store_id), commit=True)
        
        if result is not None:
            flash('Store updated successfully!', 'success')
            return redirect(url_for('admin_stores'))
        else:
            flash('Error updating store.', 'danger')
    
    query = "SELECT * FROM stores WHERE store_id = %s"
    store = execute_query(query, (store_id,), fetch_one=True)
    
    if not store:
        flash('Store not found.', 'danger')
        return redirect(url_for('admin_stores'))
    
    return render_template('edit_store.html', store=store)

@app.route('/admin/stores/delete/<int:store_id>')
@admin_required
def admin_delete_store(store_id):
    """Deactivate store (soft delete)"""
    query = "UPDATE stores SET is_active = FALSE WHERE store_id = %s"
    result = execute_query(query, (store_id,), commit=True)
    
    if result is not None:
        flash('Store deactivated successfully!', 'success')
    else:
        flash('Error deactivating store.', 'danger')
    
    return redirect(url_for('admin_stores'))

@app.route('/admin/stores/activate/<int:store_id>')
@admin_required
def admin_activate_store(store_id):
    """Activate store"""
    query = "UPDATE stores SET is_active = TRUE WHERE store_id = %s"
    result = execute_query(query, (store_id,), commit=True)
    
    if result is not None:
        flash('Store activated successfully!', 'success')
    else:
        flash('Error activating store.', 'danger')
    
    return redirect(url_for('admin_stores'))

# ============================================
# STAFF MANAGEMENT (ADMIN)
# ============================================

@app.route('/admin_staff')
@admin_required
def admin_staff():
    """List all staff members"""
    query = """
        SELECT u.*, s.store_name
        FROM users u
        LEFT JOIN stores s ON u.store_id = s.store_id
        WHERE u.role = 'staff'
        ORDER BY u.full_name
    """
    staff = execute_query(query, fetch_all=True) or []
    return render_template('admin_staff.html', staff=staff)

@app.route('/admin/staff/add', methods=['GET', 'POST'])
@admin_required
def admin_add_staff():
    """Add new staff member"""
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        full_name = request.form.get('full_name')
        email = request.form.get('email')
        contact = request.form.get('contact')
        store_id = request.form.get('store_id')
        
        if not all([username, password, full_name, store_id]):
            flash('Username, password, full name, and store are required.', 'danger')
            return redirect(url_for('admin_add_staff'))
        
        # Check if username exists
        check_query = "SELECT user_id FROM users WHERE username = %s"
        existing = execute_query(check_query, (username,), fetch_one=True)
        
        if existing:
            flash('Username already exists.', 'danger')
            return redirect(url_for('admin_add_staff'))
        
        # Store plain text password
        query = """
            INSERT INTO users (username, password_hash, full_name, email, contact, role, store_id)
            VALUES (%s, %s, %s, %s, %s, 'staff', %s)
        """
        result = execute_query(query, (username, password, full_name, email, contact, store_id), commit=True)
        
        if result:
            flash('Staff member added successfully!', 'success')
            return redirect(url_for('admin_staff'))
        else:
            flash('Error adding staff member.', 'danger')
    
    stores_query = "SELECT * FROM stores WHERE is_active = TRUE ORDER BY store_name"
    stores = execute_query(stores_query, fetch_all=True) or []
    return render_template('add_staff.html', stores=stores)

@app.route('/admin/staff/edit/<int:user_id>', methods=['GET', 'POST'])
@admin_required
def admin_edit_staff(user_id):
    """Edit staff member"""
    if request.method == 'POST':
        full_name = request.form.get('full_name')
        email = request.form.get('email')
        contact = request.form.get('contact')
        store_id = request.form.get('store_id')
        new_password = request.form.get('new_password')
        
        # Update basic info
        query = """
            UPDATE users 
            SET full_name = %s, email = %s, contact = %s, store_id = %s
            WHERE user_id = %s
        """
        result = execute_query(query, (full_name, email, contact, store_id, user_id), commit=True)
        
        # Update password if provided (plain text)
        if new_password:
            pass_query = "UPDATE users SET password_hash = %s WHERE user_id = %s"
            execute_query(pass_query, (new_password, user_id), commit=True)
        
        if result is not None:
            flash('Staff member updated successfully!', 'success')
            return redirect(url_for('admin_staff'))
        else:
            flash('Error updating staff member.', 'danger')
    
    user_query = "SELECT * FROM users WHERE user_id = %s"
    user = execute_query(user_query, (user_id,), fetch_one=True)
    
    stores_query = "SELECT * FROM stores WHERE is_active = TRUE ORDER BY store_name"
    stores = execute_query(stores_query, fetch_all=True) or []
    
    if not user:
        flash('Staff member not found.', 'danger')
        return redirect(url_for('admin_staff'))
    
    return render_template('edit_staff.html', user=user, stores=stores)

@app.route('/admin/staff/delete/<int:user_id>')
@admin_required
def admin_delete_staff(user_id):
    """Toggle staff member active status"""
    # First check current status
    check_query = "SELECT is_active FROM users WHERE user_id = %s"
    user = execute_query(check_query, (user_id,), fetch_one=True)
    
    if not user:
        flash('Staff member not found.', 'danger')
        return redirect(url_for('admin_staff'))
    
    # Toggle the status
    new_status = not user['is_active']
    query = "UPDATE users SET is_active = %s WHERE user_id = %s"
    result = execute_query(query, (new_status, user_id), commit=True)
    
    if result is not None:
        if new_status:
            flash('Staff member activated successfully!', 'success')
        else:
            flash('Staff member deactivated successfully!', 'success')
    else:
        flash('Error updating staff member status.', 'danger')
    
    return redirect(url_for('admin_staff'))
# ============================================
# PRODUCT MANAGEMENT (ADMIN)
# ============================================

@app.route('/admin/products')
@admin_required
def admin_products():
    """List all products"""
    query = "SELECT * FROM products ORDER BY brand, category, name"
    products = execute_query(query, fetch_all=True) or []
    return render_template('products.html', products=products)

@app.route('/admin/products/add', methods=['GET', 'POST'])
@admin_required
def admin_add_product():
    """Add new product"""
    if request.method == 'POST':
        brand = request.form.get('brand')
        name = request.form.get('name')
        category = request.form.get('category')
        unit = request.form.get('unit', 'pcs')
        description = request.form.get('description')
        
        if not all([brand, name, category]):
            flash('Brand, Product name, and Category are required.', 'danger')
            return redirect(url_for('admin_add_product'))
        
        query = """
            INSERT INTO products (brand, name, category, unit, description)
            VALUES (%s, %s, %s, %s, %s)
        """
        result = execute_query(query, (brand, name, category, unit, description), commit=True)
        
        if result:
            flash('Product added successfully!', 'success')
            return redirect(url_for('admin_products'))
        else:
            flash('Error adding product.', 'danger')
    
    return render_template('add_product.html')

@app.route('/admin/products/edit/<int:product_id>', methods=['GET', 'POST'])
@admin_required
def admin_edit_product(product_id):
    """Edit product"""
    if request.method == 'POST':
        brand = request.form.get('brand')
        name = request.form.get('name')
        category = request.form.get('category')
        unit = request.form.get('unit')
        description = request.form.get('description')
        
        if not all([brand, name, category]):
            flash('Brand, Product name, and Category are required.', 'danger')
            return redirect(url_for('admin_edit_product', product_id=product_id))
        
        query = """
            UPDATE products 
            SET brand = %s, name = %s, category = %s, unit = %s, description = %s
            WHERE product_id = %s
        """
        result = execute_query(query, (brand, name, category, unit, description, product_id), commit=True)
        
        if result is not None:
            flash('Product updated successfully!', 'success')
            return redirect(url_for('admin_products'))
        else:
            flash('Error updating product.', 'danger')
    
    query = "SELECT * FROM products WHERE product_id = %s"
    product = execute_query(query, (product_id,), fetch_one=True)
    
    if not product:
        flash('Product not found.', 'danger')
        return redirect(url_for('admin_products'))
    
    return render_template('edit_product.html', product=product)

@app.route('/admin/products/delete/<int:product_id>')
@admin_required
def admin_delete_product(product_id):
    """Deactivate product"""
    query = "UPDATE products SET is_active = FALSE WHERE product_id = %s"
    result = execute_query(query, (product_id,), commit=True)
    
    if result is not None:
        flash('Product deactivated successfully!', 'success')
    else:
        flash('Error deactivating product.', 'danger')
    
    return redirect(url_for('admin_products'))

# ============================================
# INVENTORY MANAGEMENT (ADMIN)
# ============================================

@app.route('/admin/inventory')
@admin_required
def admin_inventory():
    """View inventory across all stores with filters"""
    store_id = request.args.get('store_id')
    category = request.args.get('category')
    brand = request.args.get('brand')
    search = request.args.get('search')
    stock_status = request.args.get('stock_status')
    
    # Get all stores for filter dropdown
    stores_query = "SELECT * FROM stores WHERE is_active = TRUE ORDER BY store_name"
    stores = execute_query(stores_query, fetch_all=True) or []
    
    # Get all categories for filter dropdown
    categories_query = "SELECT DISTINCT category FROM products WHERE category IS NOT NULL AND is_active = TRUE ORDER BY category"
    categories_result = execute_query(categories_query, fetch_all=True) or []
    categories = [cat['category'] for cat in categories_result]
    
    # Get all brands for filter dropdown
    brands_query = "SELECT DISTINCT brand FROM products WHERE brand IS NOT NULL AND is_active = TRUE ORDER BY brand"
    brands_result = execute_query(brands_query, fetch_all=True) or []
    brands = [b['brand'] for b in brands_result]
    
    # Base query - show all inventory or filtered by store
    query = """
        SELECT i.*, p.name as product_name, p.brand, p.category, p.unit, 
               s.store_name, u.full_name as last_modified_by_name
        FROM inventory i
        JOIN products p ON i.product_id = p.product_id
        JOIN stores s ON i.store_id = s.store_id
        LEFT JOIN users u ON i.last_modified_by = u.user_id
        WHERE p.is_active = TRUE
    """
    
    params = []
    
    # Apply store filter if selected
    if store_id:
        query += " AND i.store_id = %s"
        params.append(store_id)
    
    # Apply category filter
    if category:
        query += " AND p.category = %s"
        params.append(category)
    
    # Apply brand filter
    if brand:
        query += " AND p.brand = %s"
        params.append(brand)
    
    # Apply search filter
    if search:
        query += " AND p.name LIKE %s"
        params.append(f"%{search}%")
    
    query += " ORDER BY s.store_name, p.name"
    
    inventory = execute_query(query, tuple(params) if params else None, fetch_all=True) or []
    
    # Apply stock status filter (post-query filtering)
    if stock_status:
        if stock_status == 'out_of_stock':
            inventory = [item for item in inventory if item['quantity'] == 0]
        elif stock_status == 'low_stock':
            inventory = [item for item in inventory if 0 < item['quantity'] < item['min_stock_level']]
        elif stock_status == 'in_stock':
            inventory = [item for item in inventory if item['quantity'] >= item['min_stock_level']]
    
    return render_template('admin_inventory.html',
                         inventory=inventory, 
                         stores=stores, 
                         categories=categories,
                         brands=brands,
                         selected_store=store_id,
                         selected_category=category,
                         selected_brand=brand,
                         selected_stock_status=stock_status,
                         search_query=search)

@app.route('/admin/inventory/update/<int:inventory_id>', methods=['POST'])
@admin_required
def admin_update_inventory(inventory_id):
    """Update inventory quantity"""
    quantity = request.form.get('quantity')
    min_stock_level = request.form.get('min_stock_level')
    notes = request.form.get('notes')
    
    # Get current inventory details
    current_query = "SELECT * FROM inventory WHERE inventory_id = %s"
    current_inv = execute_query(current_query, (inventory_id,), fetch_one=True)
    
    if not current_inv:
        flash('Inventory item not found.', 'danger')
        return redirect(url_for('admin_inventory'))
    
    old_quantity = current_inv['quantity']
    new_quantity = float(quantity)
    
    # Update inventory
    query = """
        UPDATE inventory 
        SET quantity = %s, min_stock_level = %s, notes = %s, last_modified_by = %s
        WHERE inventory_id = %s
    """
    result = execute_query(query, (quantity, min_stock_level, notes, session['user_id'], inventory_id), commit=True)
    
    if result is not None:
        # Log the movement
        quantity_change = new_quantity - old_quantity
        movement_type = 'in' if quantity_change > 0 else 'out' if quantity_change < 0 else 'adjustment'
        
        log_query = """
            INSERT INTO inventory_movements 
            (store_id, product_id, movement_type, quantity, previous_stock, new_stock, notes, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_query(log_query, (
            current_inv['store_id'], 
            current_inv['product_id'], 
            movement_type, 
            abs(quantity_change),
            old_quantity,
            new_quantity,
            f"Manual adjustment: {notes}" if notes else "Manual adjustment",
            session['user_id']
        ), commit=True)
        
        flash('Inventory updated successfully!', 'success')
    else:
        flash('Error updating inventory.', 'danger')
    
    return redirect(url_for('admin_inventory'))

@app.route('/admin/inventory/history/<int:inventory_id>')
@admin_required
def admin_inventory_history(inventory_id):
    """Get inventory movement history for a specific inventory item"""
    try:
        # Get inventory details
        inv_query = """
            SELECT i.*, p.name as product_name, p.unit, s.store_name
            FROM inventory i
            JOIN products p ON i.product_id = p.product_id
            JOIN stores s ON i.store_id = s.store_id
            WHERE i.inventory_id = %s
        """
        inventory = execute_query(inv_query, (inventory_id,), fetch_one=True)
        
        if not inventory:
            return jsonify({'error': 'Inventory not found'}), 404
        
        # Get movement history with flexible column selection
        history_query = """
            SELECT im.movement_id, im.movement_type, im.quantity, 
                   im.reference_type, im.reference_id, im.notes, im.created_at,
                   u.full_name as user_name, 
                   b.bill_number
            FROM inventory_movements im
            LEFT JOIN users u ON im.created_by = u.user_id
            LEFT JOIN bills b ON im.reference_type = 'bill' AND im.reference_id = b.bill_id
            WHERE im.store_id = %s AND im.product_id = %s
            ORDER BY im.created_at DESC
            LIMIT 50
        """
        
        # Try to get with new columns first
        try:
            history_query_with_stock = """
                SELECT im.movement_id, im.movement_type, im.quantity, 
                       im.previous_stock, im.new_stock,
                       im.reference_type, im.reference_id, im.notes, im.created_at,
                       u.full_name as user_name, 
                       b.bill_number
                FROM inventory_movements im
                LEFT JOIN users u ON im.created_by = u.user_id
                LEFT JOIN bills b ON im.reference_type = 'bill' AND im.reference_id = b.bill_id
                WHERE im.store_id = %s AND im.product_id = %s
                ORDER BY im.created_at DESC
                LIMIT 50
            """
            history = execute_query(history_query_with_stock, (inventory['store_id'], inventory['product_id']), fetch_all=True) or []
        except:
            # Fallback to query without stock columns if they don't exist
            history = execute_query(history_query, (inventory['store_id'], inventory['product_id']), fetch_all=True) or []
        
        # Format history for JSON response
        formatted_history = []
        for record in history:
            formatted_history.append({
                'movement_id': record['movement_id'],
                'movement_type': record['movement_type'],
                'quantity': float(record['quantity']),
                'previous_stock': float(record.get('previous_stock', 0)) if record.get('previous_stock') is not None else None,
                'new_stock': float(record.get('new_stock', 0)) if record.get('new_stock') is not None else None,
                'reference_type': record.get('reference_type'),
                'reference_id': record.get('reference_id'),
                'bill_number': record.get('bill_number'),
                'notes': record.get('notes'),
                'user_name': record.get('user_name'),
                'created_at': record['created_at'].strftime('%d %b %Y %I:%M %p') if record['created_at'] else None
            })
        
        return jsonify({
            'inventory': {
                'product_name': inventory['product_name'],
                'store_name': inventory['store_name'],
                'current_stock': float(inventory['quantity']),
                'unit': inventory['unit']
            },
            'history': formatted_history
        })
        
    except Exception as e:
        print(f"Error in admin_inventory_history: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Error loading history: {str(e)}'}), 500

@app.route('/admin/inventory/add', methods=['GET', 'POST'])
@admin_required
def admin_add_inventory():
    """Add new inventory entry"""
    if request.method == 'POST':
        store_id = request.form.get('store_id')
        product_id = request.form.get('product_id')
        quantity = request.form.get('quantity', 0)
        min_stock_level = request.form.get('min_stock_level', 0)
        notes = request.form.get('notes')
        
        # Check if inventory already exists
        check_query = """
            SELECT inventory_id FROM inventory 
            WHERE store_id = %s AND product_id = %s
        """
        existing = execute_query(check_query, (store_id, product_id), fetch_one=True)
        
        if existing:
            flash('Inventory for this product already exists in this store.', 'warning')
            return redirect(url_for('admin_inventory'))
        
        query = """
            INSERT INTO inventory (store_id, product_id, quantity, min_stock_level, notes, last_modified_by)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        result = execute_query(query, (store_id, product_id, quantity, min_stock_level, notes, session['user_id']), commit=True)
        
        if result:
            # Log movement
            log_query = """
                INSERT INTO inventory_movements (store_id, product_id, movement_type, quantity, reference_type, created_by)
                VALUES (%s, %s, 'in', %s, 'initial', %s)
            """
            execute_query(log_query, (store_id, product_id, quantity, session['user_id']), commit=True)
            
            flash('Inventory added successfully!', 'success')
            return redirect(url_for('admin_inventory'))
        else:
            flash('Error adding inventory.', 'danger')
    
    stores_query = "SELECT * FROM stores WHERE is_active = TRUE ORDER BY store_name"
    stores = execute_query(stores_query, fetch_all=True) or []
    
    products_query = "SELECT * FROM products WHERE is_active = TRUE ORDER BY name"
    products = execute_query(products_query, fetch_all=True) or []
    
    return render_template('add_inventory.html', stores=stores, products=products)
    """Admin reports dashboard"""
    # Sales by store
    store_sales_query = """
        SELECT s.store_name, 
               COUNT(b.bill_id) as total_bills,
               COALESCE(SUM(b.total_amount), 0) as total_sales
        FROM stores s
        LEFT JOIN bills b ON s.store_id = b.store_id
        WHERE s.is_active = TRUE
        GROUP BY s.store_id
        ORDER BY total_sales DESC
    """
    store_sales = execute_query(store_sales_query, fetch_all=True) or []
    
    # Sales by staff
    staff_sales_query = """
        SELECT u.full_name, s.store_name,
               COUNT(b.bill_id) as total_bills,
               COALESCE(SUM(b.total_amount), 0) as total_sales
        FROM users u
        LEFT JOIN bills b ON u.user_id = b.staff_id
        LEFT JOIN stores s ON u.store_id = s.store_id
        WHERE u.role = 'staff' AND u.is_active = TRUE
        GROUP BY u.user_id
        ORDER BY total_sales DESC
    """
    staff_sales = execute_query(staff_sales_query, fetch_all=True) or []
    
    # Top products
    top_products_query = """
        SELECT p.name, p.category,
               SUM(bi.quantity) as total_quantity,
               COALESCE(SUM(bi.total), 0) as total_sales
        FROM products p
        JOIN bill_items bi ON p.product_id = bi.product_id
        GROUP BY p.product_id
        ORDER BY total_sales DESC
        LIMIT 10
    """
    top_products = execute_query(top_products_query, fetch_all=True) or []
    
    # Low stock items
    low_stock_query = """
        SELECT p.name, s.store_name, i.quantity, i.min_stock_level
        FROM inventory i
        JOIN products p ON i.product_id = p.product_id
        JOIN stores s ON i.store_id = s.store_id
        WHERE i.quantity < i.min_stock_level AND p.is_active = TRUE
        ORDER BY (i.min_stock_level - i.quantity) DESC
        LIMIT 20
    """
    low_stock = execute_query(low_stock_query, fetch_all=True) or []
    
    return render_template('admin_reports.html',
                         store_sales=store_sales,
                         staff_sales=staff_sales,
                         top_products=top_products,
                         low_stock=low_stock)
'''@app.route('/admin/reports')
def admin_reportss():
    # Render your reports page
    return render_template('admin_reports.html')
'''

@app.route('/api/store-bills', methods=['GET'])
@admin_required
def api_store_bills():
    """
    Get all bills for a specific store
    Query params: store_id (required), date_from, date_to
    Returns: List of bills with details and summary
    """
    try:
        store_id = request.args.get('store_id')
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        
        if not store_id:
            return jsonify({'error': 'store_id is required'}), 400
        
        # Set default dates if not provided (last 30 days)
        if not date_to:
            date_to = datetime.now().strftime('%Y-%m-%d')
        if not date_from:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Query to get all bills for the store
            bills_query = """
                SELECT 
                    b.bill_id,
                    b.created_at,
                    b.customer_name,
                    b.customer_contact as customer_phone,
                    b.total_amount,
                    b.discount_amount,
                    b.payment_split,
                    u.full_name as staff_name,
                    COUNT(bi.bill_item_id) as total_items,
                    COALESCE(
                        CASE 
                            WHEN JSON_EXTRACT(b.payment_split, '$.cash') IS NOT NULL 
                            THEN CAST(JSON_EXTRACT(b.payment_split, '$.cash') AS DECIMAL(10,2))
                            ELSE 0 
                        END, 0
                    ) as cash_amount,
                    COALESCE(
                        CASE 
                            WHEN JSON_EXTRACT(b.payment_split, '$.upi') IS NOT NULL 
                            THEN CAST(JSON_EXTRACT(b.payment_split, '$.upi') AS DECIMAL(10,2))
                            ELSE 0 
                        END, 0
                    ) as upi_amount,
                    COALESCE(
                        CASE 
                            WHEN JSON_EXTRACT(b.payment_split, '$.credit') IS NOT NULL 
                            THEN CAST(JSON_EXTRACT(b.payment_split, '$.credit') AS DECIMAL(10,2))
                            ELSE 0 
                        END, 0
                    ) as credit_amount
                FROM bills b
                LEFT JOIN users u ON b.staff_id = u.user_id
                LEFT JOIN bill_items bi ON b.bill_id = bi.bill_id
                WHERE b.store_id = %s 
                    AND DATE(b.created_at) BETWEEN %s AND %s
                GROUP BY b.bill_id, b.created_at, b.customer_name, b.customer_contact, 
                         b.total_amount, b.discount_amount, b.payment_split, u.full_name
                ORDER BY b.created_at DESC
            """
            
            cursor.execute(bills_query, (store_id, date_from, date_to))
            bills = cursor.fetchall()
            
            # Calculate summary
            summary_query = """
                SELECT 
                    COUNT(DISTINCT b.bill_id) as total_bills,
                    COALESCE(SUM(b.total_amount), 0) as total_amount,
                    COALESCE(AVG(b.total_amount), 0) as avg_amount,
                    COALESCE(SUM(bi.quantity), 0) as total_items
                FROM bills b
                LEFT JOIN bill_items bi ON b.bill_id = bi.bill_id
                WHERE b.store_id = %s 
                    AND DATE(b.created_at) BETWEEN %s AND %s
            """
            
            cursor.execute(summary_query, (store_id, date_from, date_to))
            summary = cursor.fetchone()
            
            cursor.close()
            connection.close()
            
            # Convert datetime and Decimal objects for JSON serialization
            from decimal import Decimal
            for bill in bills:
                if 'created_at' in bill and bill['created_at']:
                    bill['created_at'] = bill['created_at'].isoformat()
                # Convert Decimal to float for JSON
                for key in ['total_amount', 'discount_amount', 'cash_amount', 'upi_amount', 'credit_amount']:
                    if key in bill and bill[key] is not None:
                        bill[key] = float(bill[key]) if isinstance(bill[key], Decimal) else bill[key]
            
            # Convert summary Decimals to float
            if summary:
                for key in ['total_amount', 'avg_amount', 'total_items']:
                    if key in summary and summary[key] is not None:
                        summary[key] = float(summary[key]) if isinstance(summary[key], Decimal) else summary[key]
            
            return jsonify({
                'bills': bills,
                'summary': summary
            })
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error fetching store bills: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_store_bills: {str(e)}")
        return jsonify({'error': str(e)}), 500
# ============================================
# STAFF DASHBOARD
# ============================================


@app.route('/api/admin/bill-details/<int:bill_id>', methods=['GET'])
@admin_required
def api_admin_bill_details(bill_id):
    """
    Get detailed information for a specific bill including items (Admin view)
    Path param: bill_id
    Returns: Complete bill details with items
    """
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            from decimal import Decimal
            cursor = connection.cursor(dictionary=True)
            
            # Get bill header information with store details
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
                    s.store_name,
                    s.address as store_address,
                    s.contact as store_contact,
                    u.full_name as staff_name
                FROM bills b
                LEFT JOIN stores s ON b.store_id = s.store_id
                LEFT JOIN users u ON b.staff_id = u.user_id
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
                    bi.total,
                    p.brand,
                    p.unit
                FROM bill_items bi
                LEFT JOIN products p ON bi.product_id = p.product_id
                WHERE bi.bill_id = %s
                ORDER BY bi.bill_item_id
            """
            
            cursor.execute(items_query, (bill_id,))
            items = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            # Convert datetime and Decimal objects to JSON-serializable types
            if 'created_at' in bill and bill['created_at']:
                bill['created_at'] = bill['created_at'].isoformat()
            
            # Convert bill Decimals to float
            for key in ['subtotal', 'discount_value', 'discount_amount', 'total_amount']:
                if key in bill and bill[key] is not None:
                    bill[key] = float(bill[key]) if isinstance(bill[key], Decimal) else bill[key]
            
            # Convert items Decimals to float
            for item in items:
                for key in ['quantity', 'unit_price', 'item_discount', 'total']:
                    if key in item and item[key] is not None:
                        item[key] = float(item[key]) if isinstance(item[key], Decimal) else item[key]
            
            # Parse payment_split JSON
            if bill.get('payment_split'):
                try:
                    bill['payment_split'] = json.loads(bill['payment_split']) if isinstance(bill['payment_split'], str) else bill['payment_split']
                except:
                    bill['payment_split'] = {}
            
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
        print(f"Error in api_admin_bill_details: {str(e)}")
        return jsonify({'error': str(e)}), 500

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

# ============================================
# ERROR HANDLERS
# ============================================

@app.errorhandler(404)
def not_found_error(error):
    """404 error handler"""
    flash('Page not found.', 'warning')
    return redirect(url_for('index'))

@app.errorhandler(500)
def internal_error(error):
    """500 error handler"""
    flash('An internal error occurred.', 'danger')
    return redirect(url_for('index'))

@app.errorhandler(403)
def forbidden_error(error):
    """403 error handler"""
    flash('Access forbidden.', 'danger')
    return redirect(url_for('index'))

# ============================================
# API ENDPOINTS FOR DASHBOARD
# ============================================

@app.route('/api/dashboard/stats')
@staff_required
def api_dashboard_stats():
    """API endpoint to get dashboard statistics for staff"""
    try:
        store_id = session.get('store_id')
        
        if not store_id:
            return jsonify({'error': 'Store not assigned to user'}), 400
        
        # Today's bills count
        today_bills_query = """
            SELECT COUNT(*) as count 
            FROM bills 
            WHERE store_id = %s AND DATE(created_at) = CURDATE()
        """
        today_bills_result = execute_query(today_bills_query, (store_id,), fetch_one=True)
        today_bills = today_bills_result['count'] if today_bills_result else 0
        
        # Today's sales total
        today_sales_query = """
            SELECT COALESCE(SUM(total_amount), 0) as total 
            FROM bills 
            WHERE store_id = %s AND DATE(created_at) = CURDATE()
        """
        today_sales_result = execute_query(today_sales_query, (store_id,), fetch_one=True)
        today_sales = float(today_sales_result['total']) if today_sales_result else 0.0
        
        # Low stock items count
        low_stock_query = """
            SELECT COUNT(*) as count
            FROM inventory i
            JOIN products p ON i.product_id = p.product_id
            WHERE i.store_id = %s 
            AND i.quantity < i.min_stock_level 
            AND p.is_active = TRUE
        """
        low_stock_result = execute_query(low_stock_query, (store_id,), fetch_one=True)
        low_stock_count = low_stock_result['count'] if low_stock_result else 0
        
        # Total customer dues (active credit notes)
        total_dues_query = """
            SELECT COALESCE(SUM(remaining_balance), 0) as total
            FROM credit_notes
            WHERE store_id = %s AND status = 'active'
        """
        total_dues_result = execute_query(total_dues_query, (store_id,), fetch_one=True)
        total_dues = float(total_dues_result['total']) if total_dues_result else 0.0
        
        return jsonify({
            'success': True,
            'today_bills': today_bills,
            'today_sales': today_sales,
            'low_stock_count': low_stock_count,
            'total_dues': total_dues
        })
        
    except Exception as e:
        print(f"Error in api_dashboard_stats: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Failed to load statistics: {str(e)}'}), 500
# ============================================
# API ENDPOINTS FOR BILLING
# ============================================
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

@app.route('/api/products', methods=['POST'])
@staff_required
def api_create_product():
    """Create a new product"""
    try:
        data = request.get_json()
        store_id = session.get('store_id')
        user_id = session.get('user_id')
        
        product_name = data.get('product_name')
        brand = data.get('brand', '')
        unit = data.get('unit', 'Pcs')
        opening_stock = float(data.get('opening_stock', 0))
        low_stock_threshold = float(data.get('low_stock_threshold', 10))
        
        if not product_name or not unit:
            return jsonify({'success': False, 'message': 'Product name and unit are required'}), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'success': False, 'message': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Insert product
            cursor.execute("""
                INSERT INTO products (name, brand, unit, is_active)
                VALUES (%s, %s, %s, TRUE)
            """, (product_name, brand, unit))
            
            product_id = cursor.lastrowid
            
            # Insert inventory record
            cursor.execute("""
                INSERT INTO inventory (store_id, product_id, quantity, min_stock_level, last_modified_by)
                VALUES (%s, %s, %s, %s, %s)
            """, (store_id, product_id, opening_stock, low_stock_threshold, user_id))
            
            # Log initial stock if opening_stock > 0
            if opening_stock > 0:
                cursor.execute("""
                    INSERT INTO inventory_movements 
                    (store_id, product_id, movement_type, quantity, previous_stock, new_stock,
                     reference_type, notes, created_by, created_at)
                    VALUES (%s, %s, 'in', %s, 0, %s, 'opening', 'Opening stock', %s, NOW())
                """, (store_id, product_id, opening_stock, opening_stock, user_id))
            
            connection.commit()
            cursor.close()
            connection.close()
            
            return jsonify({
                'success': True,
                'message': 'Product added successfully',
                'product_id': product_id
            })
            
        except Exception as e:
            connection.rollback()
            cursor.close()
            connection.close()
            return jsonify({'success': False, 'message': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_create_product: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500

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

@app.route('/api/customers', methods=['GET'])
@staff_required
def api_get_customers():
    """Get all customers with their total sales and credit balance"""
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = connection.cursor(dictionary=True)
        
        # Get all customers with their totals and credit due
        cursor.execute("""
            SELECT 
                c.customer_id,
                c.customer_name,
                c.mobile,
                c.address,
                COALESCE(SUM(b.total_amount), 0) as total_sales,
                COALESCE(
                    (SELECT SUM(cn.remaining_balance) 
                     FROM credit_notes cn 
                     WHERE cn.customer_id = c.customer_id 
                     AND cn.status = 'active'), 0
                ) as credit_balance,
                COALESCE(
                    (SELECT SUM(
                        CASE 
                            WHEN JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.cash')) IS NOT NULL 
                            THEN CAST(JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.cash')) AS DECIMAL(10,2))
                            ELSE 0 
                        END +
                        CASE 
                            WHEN JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.upi')) IS NOT NULL 
                            THEN CAST(JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.upi')) AS DECIMAL(10,2))
                            ELSE 0 
                        END +
                        CASE 
                            WHEN JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.card')) IS NOT NULL 
                            THEN CAST(JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.card')) AS DECIMAL(10,2))
                            ELSE 0 
                        END
                    )
                    FROM bills b2 
                    WHERE b2.customer_id = c.customer_id), 0
                ) as total_paid,
                COALESCE(SUM(b.total_amount), 0) - 
                COALESCE(
                    (SELECT SUM(
                        CASE 
                            WHEN JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.cash')) IS NOT NULL 
                            THEN CAST(JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.cash')) AS DECIMAL(10,2))
                            ELSE 0 
                        END +
                        CASE 
                            WHEN JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.upi')) IS NOT NULL 
                            THEN CAST(JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.upi')) AS DECIMAL(10,2))
                            ELSE 0 
                        END +
                        CASE 
                            WHEN JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.card')) IS NOT NULL 
                            THEN CAST(JSON_UNQUOTE(JSON_EXTRACT(b2.payment_split, '$.card')) AS DECIMAL(10,2))
                            ELSE 0 
                        END
                    )
                    FROM bills b2 
                    WHERE b2.customer_id = c.customer_id), 0
                ) as amount_due
            FROM customers c
            LEFT JOIN bills b ON c.customer_id = b.customer_id
            GROUP BY c.customer_id, c.customer_name, c.mobile, c.address
            ORDER BY c.customer_name
        """)
        
        customers = cursor.fetchall()
        
        # Convert Decimal to float for JSON serialization
        for customer in customers:
            customer['total_sales'] = float(customer['total_sales'])
            customer['credit_balance'] = float(customer['credit_balance'])
            customer['total_paid'] = float(customer.get('total_paid', 0))
            customer['amount_due'] = float(customer.get('amount_due', 0))
        
        cursor.close()
        connection.close()
        
        return jsonify(customers)
        
    except Exception as e:
        print(f"Error in api_get_customers: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

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

@app.route('/api/customers', methods=['POST'])
@staff_required
def api_create_customer():
    """Create new customer"""
    try:
        data = request.get_json()
        mobile = data.get('mobile')
        customer_name = data.get('customer_name')
        address = data.get('address', '')
        
        if not mobile or not customer_name:
            return jsonify({'success': False, 'message': 'Mobile and name required'}), 400
        
        # Check if customer already exists
        check_query = "SELECT customer_id FROM customers WHERE mobile = %s"
        existing = execute_query(check_query, (mobile,), fetch_one=True)
        if existing:
            return jsonify({'success': False, 'message': 'Customer with this mobile number already exists'}), 400
        
        query = """
            INSERT INTO customers (customer_name, mobile, address, created_at)
            VALUES (%s, %s, %s, NOW())
        """
        customer_id = execute_query(query, (customer_name, mobile, address), commit=True)
        
        if customer_id:
            # Fetch the created customer
            customer_query = "SELECT * FROM customers WHERE customer_id = %s"
            customer = execute_query(customer_query, (customer_id,), fetch_one=True)
            
            return jsonify({
                'success': True, 
                'customer_id': customer_id,
                'customer': customer
            })
        else:
            return jsonify({'success': False, 'message': 'Failed to create customer'}), 500
            
    except Exception as e:
        print(f"Error in api_create_customer: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

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
    """Get recent credit notes (active with balance first, then recent fully used)"""
    try:
        store_id = session.get('store_id')
        
        query = """
            SELECT cn.credit_id, cn.credit_number, cn.customer_id, cn.total_amount as amount,
                   cn.remaining_balance, cn.status, cn.created_at,
                   c.customer_name, c.mobile
            FROM credit_notes cn
            JOIN customers c ON cn.customer_id = c.customer_id
            WHERE cn.store_id = %s
            ORDER BY 
                CASE 
                    WHEN cn.status = 'active' AND cn.remaining_balance > 0 THEN 0
                    ELSE 1
                END,
                cn.created_at DESC
            LIMIT 50
        """
        credit_notes = execute_query(query, (store_id,), fetch_all=True) or []
        
        # Convert Decimal to float for JSON serialization
        for cn in credit_notes:
            cn['amount'] = float(cn['amount'])
            cn['remaining_balance'] = float(cn['remaining_balance'])
        
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
                return_qty = item['return_qty']
                original_rate = item['original_rate']
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

def create_customers_table():
    """Create customers table if it doesn't exist"""
    try:
        connection = get_db_connection()
        if not connection:
            return False
        
        cursor = connection.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id INT PRIMARY KEY AUTO_INCREMENT,
                customer_name VARCHAR(100) NOT NULL,
                mobile VARCHAR(15) NOT NULL UNIQUE,
                address TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_mobile (mobile)
            )
        """)
        
        connection.commit()
        cursor.close()
        connection.close()
        print("✓ Customers table created/verified successfully!")
        return True
    except Error as e:
        print(f"✗ Error creating customers table: {e}")
        return False
        
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

@app.route('/billing')
@staff_required
def billing():
    """Billing page for staff"""
    store_id = session.get('store_id')
    
    # Get store information
    store_query = "SELECT * FROM stores WHERE store_id = %s"
    store = execute_query(store_query, (store_id,), fetch_one=True)
    
    return render_template('billing.html', store=store)

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

@app.route('/stock_update')
@staff_required
def stock_update():
    """Legacy stock update page (redirects to unified page)"""
    return redirect(url_for('tinventory'))

@app.route('/dashboard')
@login_required
def dashboard():
    """Dashboard redirect based on role"""
    if is_admin():
        return redirect(url_for('admin_dashboard'))
    else:
        return redirect(url_for('staff_dashboard'))
        
@app.route('/credit_notes')
@staff_required
def credit_notes():
    """Credit notes page for staff"""
    return render_template('credit_notes.html')

@app.route('/credit_management')
@staff_required
def credit_management():
    """Credit management page - view and clear credits for staff's store"""
    return render_template('credit_management.html')

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
def api_reports_productss():
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

@app.route('/api/reports/sales', methods=['GET'])
@staff_required
def api_reports_saless():
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
def api_reports_stock():
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

@app.route('/quotations')
@admin_or_staff_required
def quotations():
    """Render the quotations page"""
    return render_template('quotations.html')

# ==========================================
# API: Get All Products for Quotations
# ==========================================
@app.route('/api/quotations/products', methods=['GET'])
@admin_or_staff_required
def get_quotation_products():
    """Get all products (including out of stock) for quotation"""
    try:
        # Get store_id from session
        store_id = session.get('store_id')
        
        if not store_id:
            print("ERROR: No store_id in session")
            return jsonify({'error': 'Store not found in session. Please login again.'}), 400
        
        print(f"Fetching products for store_id: {store_id}")
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Get products with inventory data
        # Your schema: products table has columns: product_id, brand, name, category, unit, is_active
        # Inventory table has: inventory_id, store_id, product_id, quantity
        query = """
            SELECT 
                p.product_id,
                p.name as product_name,
                p.category as category_name,
                p.brand,
                p.unit,
                COALESCE(i.quantity, 0) as stock_quantity
            FROM products p
            LEFT JOIN inventory i ON p.product_id = i.product_id AND i.store_id = %s
            WHERE p.is_active = 1
            ORDER BY p.name
        """
        
        print(f"Executing query with store_id: {store_id}")
        cursor.execute(query, (store_id,))
        
        products = cursor.fetchall()
        print(f"Found {len(products)} products")
        
        # Format products for frontend
        for product in products:
            # Convert Decimal to float for stock quantity
            product['stock_quantity'] = float(product['stock_quantity']) if product.get('stock_quantity') else 0.0
            
            # No default price - user must enter it manually
            product['selling_price'] = 0.0
            
            # Ensure category_name is set
            if not product.get('category_name'):
                product['category_name'] = 'Uncategorized'
        
        cursor.close()
        conn.close()
        
        print(f"Returning {len(products)} products with prices")
        return jsonify(products), 200
        
    except Exception as e:
        print(f"Error fetching products: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Failed to fetch products: {str(e)}'}), 500

# ==========================================
# API: Generate Quotation PDF
# ==========================================
@app.route('/api/quotations/generate', methods=['POST'])
@admin_or_staff_required
def generate_quotation_pdf():
    """Generate a quotation PDF"""
    try:
        data = request.json
        
        # Validate data
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
        
        # Get store details - your schema: stores table has store_name, address, contact
        store_id = session.get('store_id')
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT store_name, address, contact as contact_number
            FROM stores
            WHERE store_id = %s
        """, (store_id,))
        
        store = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not store:
            return jsonify({'error': 'Store not found'}), 400
        
        # Generate PDF
        pdf_buffer = io.BytesIO()
        doc = SimpleDocTemplate(pdf_buffer, pagesize=A4, topMargin=0.5*inch, bottomMargin=0.5*inch)
        
        # Container for PDF elements
        elements = []
        
        # Styles
        styles = getSampleStyleSheet()
        
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#667eea'),
            spaceAfter=12,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=14,
            textColor=colors.HexColor('#2c3e50'),
            spaceAfter=10,
            fontName='Helvetica-Bold'
        )
        
        normal_style = ParagraphStyle(
            'CustomNormal',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.HexColor('#2c3e50')
        )
        
        small_style = ParagraphStyle(
            'CustomSmall',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#666666')
        )
        
        # Title
        elements.append(Paragraph("QUOTATION", title_style))
        elements.append(Spacer(1, 0.2*inch))
        
        # Store and Customer Details in two columns
        details_data = [
            [
                Paragraph(f"<b>From:</b><br/>{store['store_name']}<br/>{store['address'] or 'N/A'}<br/>Phone: {store['contact_number'] or 'N/A'}", normal_style),
                Paragraph(f"<b>To:</b><br/>{customer_name}<br/>Mobile: {customer_mobile}<br/>{customer_address if customer_address else 'N/A'}", normal_style)
            ]
        ]
        
        details_table = Table(details_data, colWidths=[3*inch, 3*inch])
        details_table.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ]))
        elements.append(details_table)
        elements.append(Spacer(1, 0.1*inch))
        
        # Quotation Info
        quotation_number = f"QT-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        quotation_date = datetime.now().strftime('%d %B, %Y')
        
        info_data = [
            ["Quotation No:", quotation_number, "Date:", quotation_date]
        ]
        
        info_table = Table(info_data, colWidths=[1.2*inch, 2*inch, 0.8*inch, 2*inch])
        info_table.setStyle(TableStyle([
            ('FONT', (0, 0), (-1, -1), 'Helvetica', 9),
            ('FONT', (0, 0), (0, -1), 'Helvetica-Bold', 9),
            ('FONT', (2, 0), (2, -1), 'Helvetica-Bold', 9),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#2c3e50')),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ]))
        elements.append(info_table)
        elements.append(Spacer(1, 0.3*inch))
        
        # Items Table
        table_data = [['#', 'Product Name', 'Qty', 'Unit Price', 'Disc.%', 'Disc.', 'Total']]
        
        subtotal = 0
        for idx, item in enumerate(items, 1):
            quantity = float(item.get('quantity', 0))
            unit_price = float(item.get('unit_price', 0))
            discount = float(item.get('discount', 0))
            discount_percentage = float(item.get('discount_percentage', 0))
            item_total = (quantity * unit_price) - discount
            subtotal += item_total
            
            # Get brand and product name
            brand = item.get('brand', '').strip()
            product_name = item.get('product_name', '')
            
            # Combine brand + product name
            if brand:
                full_product_name = f"{brand} - {product_name}"
            else:
                full_product_name = product_name
            
            table_data.append([
                str(idx),
                full_product_name,
                f"{quantity:.2f}",
                f"Rs .{unit_price:.2f}",
                f"{discount_percentage:.1f}%" if discount_percentage > 0 else '-',  # Show percentage
                f"Rs .{discount:.2f}" if discount > 0 else '-',  # Show amount
                f"Rs .{item_total:.2f}"
            ])
        
        # Calculate totals
        discount_amount = (subtotal * discount_percentage) / 100
        grand_total = subtotal - discount_amount
        
        # Add summary rows (with extra columns for alignment)
        table_data.append(['', '', '', '', '', 'Subtotal:', f"Rs .{subtotal:.2f}"])
        
        if discount_percentage > 0:
            table_data.append(['', '', '', '', '', f'Discount ({discount_percentage}%):', f"Rs .{discount_amount:.2f}"])
        
        table_data.append(['', '', '', '', '', 'Grand Total:', f"Rs .{grand_total:.2f}"])
        
        # Create table with 7 columns now: #, Product Name, Qty, Unit Price, Disc.%, Disc., Total
        items_table = Table(table_data, colWidths=[0.4*inch, 2.2*inch, 0.7*inch, 1.0*inch, 0.6*inch, 0.9*inch, 1.0*inch])
        
        items_table.setStyle(TableStyle([
            # Header
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONT', (0, 0), (-1, 0), 'Helvetica-Bold', 10),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            
            # Body
            ('FONT', (0, 1), (-1, -4), 'Helvetica', 9),
            ('ALIGN', (0, 1), (0, -1), 'CENTER'),  # # column
            ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),  # Numeric columns
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            
            # Grid
            ('GRID', (0, 0), (-1, -4), 0.5, colors.grey),
            ('LINEBELOW', (0, 0), (-1, 0), 2, colors.HexColor('#667eea')),
            
            # Alternating row colors
            ('ROWBACKGROUNDS', (0, 1), (-1, -4), [colors.white, colors.HexColor('#f8f9fa')]),
            
            # Summary section (now columns 5 and 6 instead of 4 and 5)
            ('FONT', (5, -3), (-1, -1), 'Helvetica-Bold', 10),
            ('LINEABOVE', (5, -3), (-1, -3), 1, colors.grey),
            ('BACKGROUND', (5, -1), (-1, -1), colors.HexColor('#d4edda')),
            ('TEXTCOLOR', (5, -1), (-1, -1), colors.HexColor('#155724')),
            ('FONT', (5, -1), (-1, -1), 'Helvetica-Bold', 12),
            
            # Padding
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ]))
        
        elements.append(items_table)
        elements.append(Spacer(1, 0.4*inch))
        
        # Terms & Conditions
        elements.append(Paragraph("<b>Terms & Conditions:</b>", heading_style))
        terms = [
            "1. This quotation is valid for 30 days from the date of issue.",
            "2. Prices are subject to change without prior notice.",
            "3. Goods once sold will not be taken back or exchanged.",
            "4. Payment terms: As per agreed terms.",
            "5. All disputes are subject to local jurisdiction only."
        ]
        for term in terms:
            elements.append(Paragraph(term, small_style))
            elements.append(Spacer(1, 0.05*inch))
        
        elements.append(Spacer(1, 0.3*inch))
        
        # Footer
        elements.append(Paragraph("<i>Thank you for your business!</i>", 
                                 ParagraphStyle('Footer', parent=normal_style, alignment=TA_CENTER, textColor=colors.grey)))
        
        # Build PDF
        doc.build(elements)
        
        # Return PDF
        pdf_buffer.seek(0)
        
        return send_file(
            pdf_buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'Quotation_{customer_name.replace(" ", "_")}_{datetime.now().strftime("%Y%m%d%H%M%S")}.pdf'
        )
        
    except Exception as e:
        print(f"Error generating quotation: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': 'Failed to generate quotation', 'details': str(e)}), 500

@app.route('/api/admin/reports/sales-summary', methods=['GET'])
@admin_required
def api_admin_sales_summary():
    """
    Get overall sales summary across all stores or for a specific store
    Query params: date_from, date_to, store_id (optional)
    Returns: Total sales, bills, cash, UPI, credit breakdown
    """
    try:
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        store_id = request.args.get('store_id')  # Optional filter
        
        if not date_from or not date_to:
            return jsonify({'error': 'date_from and date_to are required'}), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Base query with optional store filtering
            query = """
                SELECT 
                    COUNT(DISTINCT b.bill_id) as total_bills,
                    COALESCE(SUM(b.total_amount), 0) as total_sales,
                    COALESCE(SUM(b.discount_amount), 0) as total_discount,
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
                    ), 0) as credit_sales,
                    COALESCE(SUM(
                        CASE 
                            WHEN JSON_EXTRACT(b.payment_split, '$.card') IS NOT NULL 
                            THEN CAST(JSON_EXTRACT(b.payment_split, '$.card') AS DECIMAL(10,2))
                            ELSE 0 
                        END
                    ), 0) as card_sales
                FROM bills b
                WHERE DATE(b.created_at) BETWEEN %s AND %s
            """
            
            params = [date_from, date_to]
            
            # Add store filter if provided
            if store_id:
                query += " AND b.store_id = %s"
                params.append(store_id)
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            cursor.close()
            connection.close()
            
            return jsonify(result if result else {})
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error in sales summary: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_admin_sales_summary: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 2. STORE-WISE SALES PERFORMANCE
@app.route('/api/admin/reports/store-sales', methods=['GET'])
@admin_required
def api_admin_store_sales():
    """
    Get sales performance by store
    Query params: date_from, date_to, store_id (optional)
    Returns: Sales breakdown by each store with bills count and revenue
    """
    try:
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        store_id = request.args.get('store_id')
        
        if not date_from or not date_to:
            return jsonify({'error': 'date_from and date_to are required'}), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    s.store_id,
                    s.store_name,
                    COUNT(DISTINCT b.bill_id) as total_bills,
                    COALESCE(SUM(b.total_amount), 0) as total_sales,
                    COALESCE(AVG(b.total_amount), 0) as avg_bill_value,
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
                FROM stores s
                LEFT JOIN bills b ON s.store_id = b.store_id 
                    AND DATE(b.created_at) BETWEEN %s AND %s
                WHERE s.is_active = TRUE
            """
            
            params = [date_from, date_to]
            
            if store_id:
                query += " AND s.store_id = %s"
                params.append(store_id)
            
            query += """
                GROUP BY s.store_id, s.store_name
                ORDER BY total_sales DESC
            """
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error in store sales: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_admin_store_sales: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 3. STAFF PERFORMANCE REPORT (across all stores or specific store)
@app.route('/api/admin/reports/staff-sales', methods=['GET'])
@admin_required
def api_admin_staff_sales():
    """
    Get staff sales performance
    Query params: date_from, date_to, store_id (optional)
    Returns: Sales by each staff member with bill count
    """
    try:
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        store_id = request.args.get('store_id')
        
        if not date_from or not date_to:
            return jsonify({'error': 'date_from and date_to are required'}), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    u.user_id,
                    u.full_name as staff_name,
                    s.store_name,
                    COUNT(DISTINCT b.bill_id) as total_bills,
                    COALESCE(SUM(b.total_amount), 0) as total_sales,
                    COALESCE(AVG(b.total_amount), 0) as avg_bill_value
                FROM users u
                INNER JOIN bills b ON u.user_id = b.staff_id
                LEFT JOIN stores s ON u.store_id = s.store_id
                WHERE u.role = 'staff' 
                    AND DATE(b.created_at) BETWEEN %s AND %s
            """
            
            params = [date_from, date_to]
            
            if store_id:
                query += " AND u.store_id = %s"
                params.append(store_id)
            
            query += """
                GROUP BY u.user_id, u.full_name, s.store_name
                ORDER BY total_sales DESC
                LIMIT 20
            """
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error in staff sales: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_admin_staff_sales: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 4. TOP PRODUCTS REPORT (across all stores or specific store)
@app.route('/api/admin/reports/top-products', methods=['GET'])
@admin_required
def api_admin_top_products():
    """
    Get top selling products
    Query params: date_from, date_to, store_id (optional), limit (default 10)
    Returns: Top products by quantity sold and revenue
    """
    try:
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        store_id = request.args.get('store_id')
        limit = request.args.get('limit', 10)
        
        if not date_from or not date_to:
            return jsonify({'error': 'date_from and date_to are required'}), 400
        
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    p.product_id,
                    p.name,
                    p.brand,
                    p.category,
                    COALESCE(SUM(bi.quantity), 0) as total_quantity,
                    COALESCE(SUM(bi.total), 0) as total_sales,
                    COUNT(DISTINCT b.bill_id) as times_sold
                FROM products p
                INNER JOIN bill_items bi ON p.product_id = bi.product_id
                INNER JOIN bills b ON bi.bill_id = b.bill_id
                WHERE DATE(b.created_at) BETWEEN %s AND %s
            """
            
            params = [date_from, date_to]
            
            if store_id:
                query += " AND b.store_id = %s"
                params.append(store_id)
            
            query += """
                GROUP BY p.product_id, p.name, p.brand, p.category
                ORDER BY total_sales DESC
                LIMIT %s
            """
            
            params.append(int(limit))
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error in top products: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_admin_top_products: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 5. LOW STOCK ALERT (across all stores or specific store)
@app.route('/api/admin/reports/low-stock', methods=['GET'])
@admin_required
def api_admin_low_stock():
    """
    Get products with low stock levels
    Query params: store_id (optional)
    Returns: Products where current stock <= minimum stock level
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
                    p.product_id,
                    p.name,
                    p.brand,
                    p.category,
                    s.store_id,
                    s.store_name,
                    i.quantity,
                    i.min_stock_level,
                    (i.min_stock_level - i.quantity) as deficit
                FROM inventory i
                INNER JOIN products p ON i.product_id = p.product_id
                INNER JOIN stores s ON i.store_id = s.store_id
                WHERE i.quantity <= i.min_stock_level 
                    AND p.is_active = TRUE
                    AND s.is_active = TRUE
            """
            
            params = []
            
            if store_id:
                query += " AND i.store_id = %s"
                params.append(store_id)
            
            query += """
                ORDER BY deficit DESC, s.store_name, p.name
            """
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error in low stock: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_admin_low_stock: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 6. STORE LIST FOR FILTER DROPDOWN
@app.route('/api/admin/stores/active', methods=['GET'])
@admin_required
def api_admin_active_stores():
    """
    Get list of all active stores for filter dropdown
    Returns: List of active stores with id and name
    """
    try:
        connection = get_db_connection()
        if not connection:
            return jsonify({'error': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    store_id,
                    store_name,
                    address,
                    contact
                FROM stores
                WHERE is_active = TRUE
                ORDER BY store_name
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return jsonify(results)
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error getting active stores: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500
            
    except Exception as e:
        print(f"Error in api_admin_active_stores: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 7. UPDATE MAIN ADMIN REPORTS ROUTE TO PASS DATA
@app.route('/admin/reports')
@admin_required
def admin_reports():
    """
    Admin Reports Dashboard
    Query params: date_from, date_to, store_id (optional)
    """
    try:
        # Get filter parameters from URL
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        store_id = request.args.get('store_id')
        
        # Set default dates if not provided (last 30 days)
        if not date_to:
            date_to = datetime.now().strftime('%Y-%m-%d')
        if not date_from:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        connection = get_db_connection()
        if not connection:
            flash('Database connection failed', 'danger')
            return render_template('admin_reports.html', 
                                 stores=[], 
                                 sales_overview={},
                                 store_sales=[],
                                 staff_sales=[],
                                 top_products=[],
                                 low_stock=[],
                                 date_from=date_from,
                                 date_to=date_to,
                                 selected_store_id=store_id)
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            # 1. GET ALL ACTIVE STORES FOR FILTER
            cursor.execute("""
                SELECT store_id, store_name 
                FROM stores 
                WHERE is_active = TRUE 
                ORDER BY store_name
            """)
            stores = cursor.fetchall()
            
            # 2. SALES OVERVIEW SUMMARY
            sales_query = """
                SELECT 
                    COUNT(DISTINCT b.bill_id) as total_bills,
                    COALESCE(SUM(b.total_amount), 0) as total_sales,
                    COALESCE(SUM(b.discount_amount), 0) as total_discount,
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
            
            sales_params = [date_from, date_to]
            if store_id:
                sales_query += " AND b.store_id = %s"
                sales_params.append(store_id)
            
            cursor.execute(sales_query, sales_params)
            sales_overview = cursor.fetchone()
            if not sales_overview:
                sales_overview = {
                    'total_bills': 0,
                    'total_sales': 0,
                    'total_discount': 0,
                    'cash_sales': 0,
                    'upi_sales': 0,
                    'credit_sales': 0
                }
            
            # 3. STORE-WISE SALES
            store_sales_query = """
                SELECT 
                    s.store_id,
                    s.store_name,
                    COUNT(DISTINCT b.bill_id) as total_bills,
                    COALESCE(SUM(b.total_amount), 0) as total_sales,
                    COALESCE(AVG(b.total_amount), 0) as avg_bill_value
                FROM stores s
                LEFT JOIN bills b ON s.store_id = b.store_id 
                    AND DATE(b.created_at) BETWEEN %s AND %s
                WHERE s.is_active = TRUE
            """
            
            store_params = [date_from, date_to]
            if store_id:
                store_sales_query += " AND s.store_id = %s"
                store_params.append(store_id)
            
            store_sales_query += """
                GROUP BY s.store_id, s.store_name
                ORDER BY total_sales DESC
            """
            
            cursor.execute(store_sales_query, store_params)
            store_sales = cursor.fetchall()
            
            # 4. STAFF PERFORMANCE
            staff_query = """
                SELECT 
                    u.user_id,
                    u.full_name as staff_name,
                    s.store_name,
                    COUNT(DISTINCT b.bill_id) as total_bills,
                    COALESCE(SUM(b.total_amount), 0) as total_sales,
                    COALESCE(AVG(b.total_amount), 0) as avg_bill_value
                FROM users u
                INNER JOIN bills b ON u.user_id = b.staff_id
                LEFT JOIN stores s ON u.store_id = s.store_id
                WHERE u.role = 'staff' 
                    AND DATE(b.created_at) BETWEEN %s AND %s
            """
            
            staff_params = [date_from, date_to]
            if store_id:
                staff_query += " AND u.store_id = %s"
                staff_params.append(store_id)
            
            staff_query += """
                GROUP BY u.user_id, u.full_name, s.store_name
                ORDER BY total_sales DESC
                LIMIT 20
            """
            
            cursor.execute(staff_query, staff_params)
            staff_sales = cursor.fetchall()
            
            # 5. TOP PRODUCTS
            products_query = """
                SELECT 
                    p.product_id,
                    p.name,
                    p.brand,
                    p.category,
                    COALESCE(SUM(bi.quantity), 0) as total_quantity,
                    COALESCE(SUM(bi.total), 0) as total_sales,
                    COUNT(DISTINCT b.bill_id) as times_sold
                FROM products p
                INNER JOIN bill_items bi ON p.product_id = bi.product_id
                INNER JOIN bills b ON bi.bill_id = b.bill_id
                WHERE DATE(b.created_at) BETWEEN %s AND %s
            """
            
            products_params = [date_from, date_to]
            if store_id:
                products_query += " AND b.store_id = %s"
                products_params.append(store_id)
            
            products_query += """
                GROUP BY p.product_id, p.name, p.brand, p.category
                ORDER BY total_sales DESC
                LIMIT 10
            """
            
            cursor.execute(products_query, products_params)
            top_products = cursor.fetchall()
            
            # 6. LOW STOCK ITEMS
            low_stock_query = """
                SELECT 
                    p.product_id,
                    p.name,
                    p.brand,
                    p.category,
                    s.store_id,
                    s.store_name,
                    i.quantity,
                    i.min_stock_level,
                    (i.min_stock_level - i.quantity) as deficit
                FROM inventory i
                INNER JOIN products p ON i.product_id = p.product_id
                INNER JOIN stores s ON i.store_id = s.store_id
                WHERE i.quantity <= i.min_stock_level 
                    AND p.is_active = TRUE
                    AND s.is_active = TRUE
            """
            
            low_stock_params = []
            if store_id:
                low_stock_query += " AND i.store_id = %s"
                low_stock_params.append(store_id)
            
            low_stock_query += """
                ORDER BY deficit DESC, s.store_name, p.name
                LIMIT 50
            """
            
            cursor.execute(low_stock_query, low_stock_params)
            low_stock = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            # RENDER TEMPLATE WITH ALL DATA
            return render_template('admin_reports.html',
                                 stores=stores,
                                 sales_overview=sales_overview,
                                 store_sales=store_sales,
                                 staff_sales=staff_sales,
                                 top_products=top_products,
                                 low_stock=low_stock,
                                 date_from=date_from,
                                 date_to=date_to,
                                 selected_store_id=store_id)
            
        except Exception as e:
            if connection:
                connection.close()
            print(f"Error in admin reports: {str(e)}")
            import traceback
            traceback.print_exc()
            flash(f'Error loading reports: {str(e)}', 'danger')
            return render_template('admin_reports.html',
                                 stores=[],
                                 sales_overview={'total_bills': 0, 'total_sales': 0, 'cash_sales': 0, 'upi_sales': 0, 'credit_sales': 0},
                                 store_sales=[],
                                 staff_sales=[],
                                 top_products=[],
                                 low_stock=[],
                                 date_from=date_from,
                                 date_to=date_to,
                                 selected_store_id=store_id)
            
    except Exception as e:
        print(f"Error in admin_reports: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error: {str(e)}', 'danger')
        return render_template('admin_reports.html',
                             stores=[],
                             sales_overview={'total_bills': 0, 'total_sales': 0, 'cash_sales': 0, 'upi_sales': 0, 'credit_sales': 0},
                             store_sales=[],
                             staff_sales=[],
                             top_products=[],
                             low_stock=[],
                             date_from=date_from if 'date_from' in locals() else None,
                             date_to=date_to if 'date_to' in locals() else None,
                             selected_store_id=store_id if 'store_id' in locals() else None)
#++++++++++++++++++++++++++
# ============================================
# ADMIN CREDIT & CREDIT NOTE MANAGEMENT APIs (CORRECTED)
# ============================================
# Add these routes to your app.py file

# ---------------------------------------
# API 1: Get All Credit Notes with Filters
# ---------------------------------------
@app.route('/api/admin/credit-notes', methods=['GET'])
@admin_required
def api_get_all_credit_notes():
    """Get all credit notes with filters"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get filter parameters
        store_id = request.args.get('store_id', type=int)
        customer_search = request.args.get('customer_search', '').strip()
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        status = request.args.get('status')  # 'active', 'fully_used', 'expired'
        min_amount = request.args.get('min_amount', type=float)
        max_amount = request.args.get('max_amount', type=float)
        
        # Base query
        query = """
            SELECT 
                cn.credit_id,
                cn.credit_number,
                cn.bill_id,
                b.bill_number,
                cn.customer_id,
                c.customer_name,
                c.mobile as customer_contact,
                cn.store_id,
                s.store_name,
                cn.total_amount,
                cn.remaining_balance,
                (cn.total_amount - cn.remaining_balance) as used_amount,
                cn.status,
                cn.notes,
                cn.created_at,
                cn.staff_id,
                u.full_name as staff_name
            FROM credit_notes cn
            INNER JOIN bills b ON cn.bill_id = b.bill_id
            LEFT JOIN customers c ON cn.customer_id = c.customer_id
            INNER JOIN stores s ON cn.store_id = s.store_id
            LEFT JOIN users u ON cn.staff_id = u.user_id
        """
        
        conditions = []
        params = []
        
        # Apply filters
        if store_id:
            conditions.append("cn.store_id = %s")
            params.append(store_id)
        
        if customer_search:
            conditions.append("(c.customer_name LIKE %s OR c.mobile LIKE %s OR cn.credit_number LIKE %s)")
            search_param = f"%{customer_search}%"
            params.extend([search_param, search_param, search_param])
        
        if date_from:
            conditions.append("DATE(cn.created_at) >= %s")
            params.append(date_from)
        
        if date_to:
            conditions.append("DATE(cn.created_at) <= %s")
            params.append(date_to)
        
        if status:
            conditions.append("cn.status = %s")
            params.append(status)
        
        if min_amount is not None:
            conditions.append("cn.total_amount >= %s")
            params.append(min_amount)
        
        if max_amount is not None:
            conditions.append("cn.total_amount <= %s")
            params.append(max_amount)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY cn.created_at DESC"
        
        cursor.execute(query, params)
        credit_notes = cursor.fetchall()
        
        # Get summary statistics
        summary_query = """
            SELECT 
                COUNT(cn.credit_id) as total_notes,
                COALESCE(SUM(cn.total_amount), 0) as total_amount,
                COALESCE(SUM(cn.total_amount - cn.remaining_balance), 0) as total_used,
                COALESCE(SUM(cn.remaining_balance), 0) as total_remaining,
                COUNT(CASE WHEN cn.status = 'active' THEN 1 END) as active_count,
                COUNT(CASE WHEN cn.status = 'fully_used' THEN 1 END) as fully_used_count,
                COUNT(CASE WHEN cn.status = 'expired' THEN 1 END) as expired_count
            FROM credit_notes cn
        """
        
        summary_conditions = []
        summary_params = []
        
        if store_id:
            summary_conditions.append("cn.store_id = %s")
            summary_params.append(store_id)
        
        if summary_conditions:
            summary_query += " WHERE " + " AND ".join(summary_conditions)
        
        cursor.execute(summary_query, summary_params)
        summary = cursor.fetchone()
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'credit_notes': credit_notes,
            'summary': summary
        })
        
    except Exception as e:
        print(f"Error fetching credit notes: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


# ---------------------------------------
# API 2: Get Credit Note Details
# ---------------------------------------
@app.route('/api/admin/credit-note/<int:credit_id>', methods=['GET'])
@admin_required
def api_get_credit_note_details(credit_id):
    """Get detailed information for a specific credit note"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get credit note details
        cursor.execute("""
            SELECT 
                cn.credit_id,
                cn.credit_number,
                cn.bill_id,
                b.bill_number,
                cn.customer_id,
                c.customer_name,
                c.mobile as customer_contact,
                c.address as customer_address,
                cn.store_id,
                s.store_name,
                cn.total_amount,
                cn.remaining_balance,
                (cn.total_amount - cn.remaining_balance) as used_amount,
                cn.status,
                cn.notes,
                cn.staff_id,
                u.full_name as staff_name,
                cn.created_at
            FROM credit_notes cn
            INNER JOIN bills b ON cn.bill_id = b.bill_id
            LEFT JOIN customers c ON cn.customer_id = c.customer_id
            INNER JOIN stores s ON cn.store_id = s.store_id
            LEFT JOIN users u ON cn.staff_id = u.user_id
            WHERE cn.credit_id = %s
        """, (credit_id,))
        
        credit_note = cursor.fetchone()
        
        if not credit_note:
            return jsonify({
                'success': False,
                'message': 'Credit note not found'
            }), 404
        
        # Get usage history
        cursor.execute("""
            SELECT 
                cnu.usage_id,
                cnu.bill_id,
                b.bill_number,
                cnu.amount_used,
                cnu.used_at
            FROM credit_note_usage cnu
            LEFT JOIN bills b ON cnu.bill_id = b.bill_id
            WHERE cnu.credit_id = %s
            ORDER BY cnu.used_at DESC
        """, (credit_id,))
        
        usage_history = cursor.fetchall()
        
        # Get return items (if any)
        cursor.execute("""
            SELECT 
                ri.return_id,
                ri.product_name,
                ri.quantity,
                ri.unit_price,
                ri.refund_amount,
                ri.date
            FROM return_items ri
            WHERE ri.credit_id = %s
            ORDER BY ri.date DESC
        """, (credit_id,))
        
        return_items = cursor.fetchall()
        
        # Get original bill items
        cursor.execute("""
            SELECT 
                bi.product_name,
                bi.quantity,
                bi.unit_price,
                bi.total
            FROM bill_items bi
            WHERE bi.bill_id = %s
        """, (credit_note['bill_id'],))
        
        original_items = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'credit_note': credit_note,
            'usage_history': usage_history,
            'return_items': return_items,
            'original_items': original_items
        })
        
    except Exception as e:
        print(f"Error fetching credit note details: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


# ---------------------------------------
# API 3: Get Customer Credit Notes
# ---------------------------------------
@app.route('/api/admin/customer-credit-notes/<int:customer_id>', methods=['GET'])
@admin_required
def api_get_customer_credit_notes(customer_id):
    """Get all credit notes for a specific customer"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get customer info
        cursor.execute("""
            SELECT 
                customer_id,
                customer_name,
                mobile,
                address,
                created_at
            FROM customers
            WHERE customer_id = %s
        """, (customer_id,))
        
        customer = cursor.fetchone()
        
        if not customer:
            return jsonify({
                'success': False,
                'message': 'Customer not found'
            }), 404
        
        # Get all credit notes
        cursor.execute("""
            SELECT 
                cn.credit_id,
                cn.credit_number,
                cn.bill_id,
                b.bill_number,
                s.store_name,
                cn.total_amount,
                cn.remaining_balance,
                (cn.total_amount - cn.remaining_balance) as used_amount,
                cn.status,
                cn.created_at,
                u.full_name as staff_name
            FROM credit_notes cn
            INNER JOIN bills b ON cn.bill_id = b.bill_id
            INNER JOIN stores s ON cn.store_id = s.store_id
            LEFT JOIN users u ON cn.staff_id = u.user_id
            WHERE cn.customer_id = %s
            ORDER BY cn.created_at DESC
        """, (customer_id,))
        
        credit_notes = cursor.fetchall()
        
        # Get usage summary
        cursor.execute("""
            SELECT 
                COUNT(cn.credit_id) as total_notes,
                COALESCE(SUM(cn.total_amount), 0) as total_amount,
                COALESCE(SUM(cn.remaining_balance), 0) as total_remaining,
                COALESCE(SUM(cn.total_amount - cn.remaining_balance), 0) as total_used
            FROM credit_notes cn
            WHERE cn.customer_id = %s
        """, (customer_id,))
        
        summary = cursor.fetchone()
        
        # Get all bills for this customer
        cursor.execute("""
            SELECT 
                b.bill_id,
                b.bill_number,
                b.total_amount,
                b.created_at,
                s.store_name,
                u.full_name as staff_name
            FROM bills b
            INNER JOIN stores s ON b.store_id = s.store_id
            LEFT JOIN users u ON b.staff_id = u.user_id
            WHERE b.customer_id = %s
            ORDER BY b.created_at DESC
            LIMIT 50
        """, (customer_id,))
        
        bills = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'customer': customer,
            'credit_notes': credit_notes,
            'summary': summary,
            'bills': bills
        })
        
    except Exception as e:
        print(f"Error fetching customer credit notes: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


# ---------------------------------------
# API 4: Get All Customers with Credit Notes
# ---------------------------------------
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


@app.route('/api/admin/customers-with-credits-old', methods=['GET'])
@admin_required
def api_get_customers_with_credits_old():
    """Get all customers who have credit notes (OLD)"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get filter parameters
        store_id = request.args.get('store_id', type=int)
        customer_search = request.args.get('customer_search', '').strip()
        
        # Base query
        query = """
            SELECT 
                c.customer_id,
                c.customer_name,
                c.mobile,
                c.address,
                COUNT(DISTINCT cn.credit_id) as total_credit_notes,
                COALESCE(SUM(cn.total_amount), 0) as total_credit_amount,
                COALESCE(SUM(cn.remaining_balance), 0) as total_remaining,
                COALESCE(SUM(cn.total_amount - cn.remaining_balance), 0) as total_used,
                COUNT(CASE WHEN cn.status = 'active' THEN 1 END) as active_notes
            FROM customers c
            INNER JOIN credit_notes cn ON c.customer_id = cn.customer_id
        """
        
        conditions = []
        params = []
        
        # Apply filters
        if store_id:
            conditions.append("cn.store_id = %s")
            params.append(store_id)
        
        if customer_search:
            conditions.append("(c.customer_name LIKE %s OR c.mobile LIKE %s)")
            search_param = f"%{customer_search}%"
            params.extend([search_param, search_param])
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += """
            GROUP BY c.customer_id, c.customer_name, c.mobile, c.address
            ORDER BY total_remaining DESC, c.customer_name
        """
        
        cursor.execute(query, params)
        customers = cursor.fetchall()
        
        # Get summary
        summary_query = """
            SELECT 
                COUNT(DISTINCT c.customer_id) as total_customers,
                COUNT(DISTINCT cn.credit_id) as total_notes,
                COALESCE(SUM(cn.total_amount), 0) as total_amount,
                COALESCE(SUM(cn.remaining_balance), 0) as total_remaining
            FROM customers c
            INNER JOIN credit_notes cn ON c.customer_id = cn.customer_id
        """
        
        summary_conditions = []
        summary_params = []
        
        if store_id:
            summary_conditions.append("cn.store_id = %s")
            summary_params.append(store_id)
        
        if summary_conditions:
            summary_query += " WHERE " + " AND ".join(summary_conditions)
        
        cursor.execute(summary_query, summary_params)
        summary = cursor.fetchone()
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'customers': customers,
            'summary': summary
        })
        
    except Exception as e:
        print(f"Error fetching customers with credits: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


# ---------------------------------------
# API 5: Export Credit Notes to CSV
# ---------------------------------------
@app.route('/api/admin/credit-notes/export', methods=['GET'])
@admin_required
def api_export_credit_notes():
    """Export credit notes data to CSV"""
    try:
        import csv
        from io import StringIO
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        store_id = request.args.get('store_id', type=int)
        
        query = """
            SELECT 
                cn.credit_number,
                b.bill_number,
                c.customer_name,
                c.mobile as customer_contact,
                s.store_name,
                cn.total_amount,
                (cn.total_amount - cn.remaining_balance) as used_amount,
                cn.remaining_balance,
                cn.status,
                cn.notes,
                u.full_name as staff_name,
                cn.created_at
            FROM credit_notes cn
            INNER JOIN bills b ON cn.bill_id = b.bill_id
            LEFT JOIN customers c ON cn.customer_id = c.customer_id
            INNER JOIN stores s ON cn.store_id = s.store_id
            LEFT JOIN users u ON cn.staff_id = u.user_id
        """
        
        params = []
        if store_id:
            query += " WHERE cn.store_id = %s"
            params.append(store_id)
        
        query += " ORDER BY cn.created_at DESC"
        
        cursor.execute(query, params)
        credit_notes = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        # Create CSV
        si = StringIO()
        writer = csv.writer(si)
        
        # Write header
        writer.writerow([
            'Credit Note #', 'Bill #', 'Customer', 'Contact', 'Store',
            'Total Amount', 'Used', 'Remaining', 'Status', 'Notes',
            'Staff', 'Created At'
        ])
        
        # Write data
        for note in credit_notes:
            writer.writerow([
                note['credit_number'],
                note['bill_number'],
                note['customer_name'],
                note['customer_contact'],
                note['store_name'],
                note['total_amount'],
                note['used_amount'],
                note['remaining_balance'],
                note['status'],
                note['notes'],
                note['staff_name'],
                note['created_at'].strftime('%Y-%m-%d %H:%M:%S') if note['created_at'] else ''
            ])
        
        output = si.getvalue()
        si.close()
        
        from flask import Response
        return Response(
            output,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=credit_notes_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
        )
        
    except Exception as e:
        print(f"Error exporting credit notes: {str(e)}")
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


# ---------------------------------------
# API 6: Export Customers with Credits to CSV
# ---------------------------------------
@app.route('/api/admin/customers-credits/export', methods=['GET'])
@admin_required
def api_export_customers_credits():
    """Export customers with credits data to CSV"""
    try:
        import csv
        from io import StringIO
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        store_id = request.args.get('store_id', type=int)
        
        query = """
            SELECT 
                c.customer_name,
                c.mobile,
                c.address,
                COUNT(DISTINCT cn.credit_id) as total_credit_notes,
                COALESCE(SUM(cn.total_amount), 0) as total_credit_amount,
                COALESCE(SUM(cn.remaining_balance), 0) as total_remaining,
                COALESCE(SUM(cn.total_amount - cn.remaining_balance), 0) as total_used,
                COUNT(CASE WHEN cn.status = 'active' THEN 1 END) as active_notes,
                c.created_at
            FROM customers c
            INNER JOIN credit_notes cn ON c.customer_id = cn.customer_id
        """
        
        params = []
        if store_id:
            query += " WHERE cn.store_id = %s"
            params.append(store_id)
        
        query += """
            GROUP BY c.customer_id, c.customer_name, c.mobile, c.address, c.created_at
            ORDER BY total_remaining DESC, c.customer_name
        """
        
        cursor.execute(query, params)
        customers = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        # Create CSV
        si = StringIO()
        writer = csv.writer(si)
        
        # Write header
        writer.writerow([
            'Customer Name', 'Mobile', 'Address', 'Total Notes',
            'Total Amount', 'Used', 'Remaining', 'Active Notes', 'Customer Since'
        ])
        
        # Write data
        for customer in customers:
            writer.writerow([
                customer['customer_name'],
                customer['mobile'],
                customer['address'],
                customer['total_credit_notes'],
                customer['total_credit_amount'],
                customer['total_used'],
                customer['total_remaining'],
                customer['active_notes'],
                customer['created_at'].strftime('%Y-%m-%d') if customer['created_at'] else ''
            ])
        
        output = si.getvalue()
        si.close()
        
        from flask import Response
        return Response(
            output,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=customers_credits_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
        )
        
    except Exception as e:
        print(f"Error exporting customers with credits: {str(e)}")
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


# ---------------------------------------
# PAGE ROUTE: Admin Credit Management
# ---------------------------------------
@app.route('/admin/credit-management')
@admin_required
def admin_credit_management():
    """Admin page for credit note management"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get all stores for filter
        cursor.execute("""
            SELECT store_id, store_name 
            FROM stores 
            WHERE is_active = TRUE 
            ORDER BY store_name
        """)
        stores = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return render_template('admin_credit_management.html', stores=stores)
        
    except Exception as e:
        print(f"Error loading credit management page: {str(e)}")
        flash('Error loading page', 'danger')
        return redirect(url_for('admin_dashboard'))
        
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
@app.route('/api/admin/customers-with-credit-balance', methods=['GET'])
@admin_required
def api_get_customers_with_credit_balance():
    """Get all customers with outstanding credit balance"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get filter parameters
        store_id = request.args.get('store_id', type=int)
        customer_search = request.args.get('customer_search', '').strip()
        min_balance = request.args.get('min_balance', type=float)
        
        # Query to calculate credit balance per customer
        query = """
            SELECT 
                c.customer_id,
                c.customer_name,
                c.mobile,
                c.address,
                COUNT(DISTINCT b.bill_id) as total_bills,
                COALESCE(SUM(b.total_amount), 0) as total_purchases,
                COALESCE(SUM(
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.cash') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.cash') AS DECIMAL(10,2))
                        ELSE 0 
                    END +
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.upi') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.upi') AS DECIMAL(10,2))
                        ELSE 0 
                    END +
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.card') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.card') AS DECIMAL(10,2))
                        ELSE 0 
                    END
                ), 0) as total_paid,
                COALESCE(SUM(
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.credit') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.credit') AS DECIMAL(10,2))
                        ELSE 0 
                    END
                ), 0) as amount_due_credit,
                MAX(b.created_at) as last_purchase_date
            FROM customers c
            INNER JOIN bills b ON c.customer_id = b.customer_id
        """
        
        conditions = []
        params = []
        
        # Apply filters
        if store_id:
            conditions.append("b.store_id = %s")
            params.append(store_id)
        
        if customer_search:
            conditions.append("(c.customer_name LIKE %s OR c.mobile LIKE %s)")
            search_param = f"%{customer_search}%"
            params.extend([search_param, search_param])
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += """
            GROUP BY c.customer_id, c.customer_name, c.mobile, c.address
            HAVING amount_due_credit > 0
        """
        
        # Apply having condition for min balance
        if min_balance is not None:
            query += " AND amount_due_credit >= %s"
            params.append(min_balance)
        
        query += " ORDER BY amount_due_credit DESC, c.customer_name"
        
        cursor.execute(query, params)
        customers = cursor.fetchall()
        
        # Get summary statistics
        summary_query = """
            SELECT 
                COUNT(DISTINCT c.customer_id) as total_customers,
                COALESCE(SUM(b.total_amount), 0) as total_purchases,
                COALESCE(SUM(
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.cash') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.cash') AS DECIMAL(10,2))
                        ELSE 0 
                    END +
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.upi') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.upi') AS DECIMAL(10,2))
                        ELSE 0 
                    END +
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.card') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.card') AS DECIMAL(10,2))
                        ELSE 0 
                    END
                ), 0) as total_paid,
                COALESCE(SUM(
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.credit') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.credit') AS DECIMAL(10,2))
                        ELSE 0 
                    END
                ), 0) as total_credit_due
            FROM customers c
            INNER JOIN bills b ON c.customer_id = b.customer_id
        """
        
        summary_conditions = []
        summary_params = []
        
        if store_id:
            summary_conditions.append("b.store_id = %s")
            summary_params.append(store_id)
        
        if summary_conditions:
            summary_query += " WHERE " + " AND ".join(summary_conditions)
        
        cursor.execute(summary_query, summary_params)
        summary = cursor.fetchone()
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'customers': customers,
            'summary': summary
        })
        
    except Exception as e:
        print(f"Error fetching customers with credit balance: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


# ---------------------------------------
# API 2: Get Customer Credit Balance Details
# ---------------------------------------
@app.route('/api/admin/customer-credit-balance/<int:customer_id>', methods=['GET'])
@admin_required
def api_get_customer_credit_balance_details(customer_id):
    """Get detailed credit balance information for a customer"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get customer info
        cursor.execute("""
            SELECT 
                customer_id,
                customer_name,
                mobile,
                address,
                created_at
            FROM customers
            WHERE customer_id = %s
        """, (customer_id,))
        
        customer = cursor.fetchone()
        
        if not customer:
            return jsonify({
                'success': False,
                'message': 'Customer not found'
            }), 404
        
        # Get credit summary
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT b.bill_id) as total_bills,
                COALESCE(SUM(b.total_amount), 0) as total_purchases,
                COALESCE(SUM(
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.cash') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.cash') AS DECIMAL(10,2))
                        ELSE 0 
                    END +
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.upi') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.upi') AS DECIMAL(10,2))
                        ELSE 0 
                    END +
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.card') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.card') AS DECIMAL(10,2))
                        ELSE 0 
                    END
                ), 0) as total_paid,
                COALESCE(SUM(
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.credit') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.credit') AS DECIMAL(10,2))
                        ELSE 0 
                    END
                ), 0) as amount_due_credit
            FROM bills b
            WHERE b.customer_id = %s
        """, (customer_id,))
        
        summary = cursor.fetchone()
        
        # Get all credit bills (bills with credit payment)
        cursor.execute("""
            SELECT 
                b.bill_id,
                b.bill_number,
                b.total_amount,
                b.payment_split,
                CASE 
                    WHEN JSON_EXTRACT(b.payment_split, '$.cash') IS NOT NULL 
                    THEN CAST(JSON_EXTRACT(b.payment_split, '$.cash') AS DECIMAL(10,2))
                    ELSE 0 
                END as cash_paid,
                CASE 
                    WHEN JSON_EXTRACT(b.payment_split, '$.upi') IS NOT NULL 
                    THEN CAST(JSON_EXTRACT(b.payment_split, '$.upi') AS DECIMAL(10,2))
                    ELSE 0 
                END as upi_paid,
                CASE 
                    WHEN JSON_EXTRACT(b.payment_split, '$.card') IS NOT NULL 
                    THEN CAST(JSON_EXTRACT(b.payment_split, '$.card') AS DECIMAL(10,2))
                    ELSE 0 
                END as card_paid,
                CASE 
                    WHEN JSON_EXTRACT(b.payment_split, '$.credit') IS NOT NULL 
                    THEN CAST(JSON_EXTRACT(b.payment_split, '$.credit') AS DECIMAL(10,2))
                    ELSE 0 
                END as credit_amount,
                b.created_at,
                s.store_name,
                u.full_name as staff_name
            FROM bills b
            INNER JOIN stores s ON b.store_id = s.store_id
            LEFT JOIN users u ON b.staff_id = u.user_id
            WHERE b.customer_id = %s
                AND JSON_EXTRACT(b.payment_split, '$.credit') IS NOT NULL
                AND CAST(JSON_EXTRACT(b.payment_split, '$.credit') AS DECIMAL(10,2)) > 0
            ORDER BY b.created_at DESC
        """, (customer_id,))
        
        credit_bills = cursor.fetchall()
        
        # Get credit notes balance
        cursor.execute("""
            SELECT 
                COUNT(cn.credit_id) as total_credit_notes,
                COALESCE(SUM(cn.remaining_balance), 0) as credit_notes_balance
            FROM credit_notes cn
            WHERE cn.customer_id = %s
                AND cn.status = 'active'
        """, (customer_id,))
        
        credit_notes_info = cursor.fetchone()
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'customer': customer,
            'summary': summary,
            'credit_bills': credit_bills,
            'credit_notes_info': credit_notes_info
        })
        
    except Exception as e:
        print(f"Error fetching customer credit balance details: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


# ---------------------------------------
# API 3: Export Customers Credit Balance to CSV
# ---------------------------------------
@app.route('/api/admin/customers-credit-balance/export', methods=['GET'])
@admin_required
def api_export_customers_credit_balance():
    """Export customers with credit balance to CSV"""
    try:
        import csv
        from io import StringIO
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        store_id = request.args.get('store_id', type=int)
        
        query = """
            SELECT 
                c.customer_name,
                c.mobile,
                c.address,
                COUNT(DISTINCT b.bill_id) as total_bills,
                COALESCE(SUM(b.total_amount), 0) as total_purchases,
                COALESCE(SUM(
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.cash') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.cash') AS DECIMAL(10,2))
                        ELSE 0 
                    END +
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.upi') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.upi') AS DECIMAL(10,2))
                        ELSE 0 
                    END +
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.card') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.card') AS DECIMAL(10,2))
                        ELSE 0 
                    END
                ), 0) as total_paid,
                COALESCE(SUM(
                    CASE 
                        WHEN JSON_EXTRACT(b.payment_split, '$.credit') IS NOT NULL 
                        THEN CAST(JSON_EXTRACT(b.payment_split, '$.credit') AS DECIMAL(10,2))
                        ELSE 0 
                    END
                ), 0) as amount_due_credit,
                MAX(b.created_at) as last_purchase_date
            FROM customers c
            INNER JOIN bills b ON c.customer_id = b.customer_id
        """
        
        params = []
        if store_id:
            query += " WHERE b.store_id = %s"
            params.append(store_id)
        
        query += """
            GROUP BY c.customer_id, c.customer_name, c.mobile, c.address
            HAVING amount_due_credit > 0
            ORDER BY amount_due_credit DESC
        """
        
        cursor.execute(query, params)
        customers = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        # Create CSV
        si = StringIO()
        writer = csv.writer(si)
        
        # Write header
        writer.writerow([
            'Customer Name', 'Mobile', 'Address', 'Total Bills',
            'Total Purchases', 'Total Paid', 'Amount Due (Credit)', 'Last Purchase'
        ])
        
        # Write data
        for customer in customers:
            writer.writerow([
                customer['customer_name'],
                customer['mobile'],
                customer['address'] or '',
                customer['total_bills'],
                customer['total_purchases'],
                customer['total_paid'],
                customer['amount_due_credit'],
                customer['last_purchase_date'].strftime('%Y-%m-%d') if customer['last_purchase_date'] else ''
            ])
        
        output = si.getvalue()
        si.close()
        
        from flask import Response
        return Response(
            output,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=customer_credit_balance_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
        )
        
    except Exception as e:
        print(f"Error exporting customer credit balance: {str(e)}")
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


# ---------------------------------------
# PAGE ROUTE: Admin Credit Balance Tracking
# ---------------------------------------
@app.route('/admin/credit-balance-tracking')
@admin_required
def admin_credit_balance_tracking():
    """Admin page for tracking customer credit balances"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get all stores for filter
        cursor.execute("""
            SELECT store_id, store_name 
            FROM stores 
            WHERE is_active = TRUE 
            ORDER BY store_name
        """)
        stores = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return render_template('admin_credit_balance_tracking.html', stores=stores)
        
    except Exception as e:
        print(f"Error loading credit balance tracking page: {str(e)}")
        flash('Error loading page', 'danger')
        return redirect(url_for('admin_dashboard'))



# ============================================
# CREDIT SETTLEMENT APIs
# ============================================

# ---------------------------------------
# API 1: Get Outstanding Credit for Customer
# ---------------------------------------
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


# ---------------------------------------
# API 2: Record Credit Payment (Settle Credit)
# ---------------------------------------
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


# ---------------------------------------
# API 3: Get Credit Payment History
# ---------------------------------------
@app.route('/api/credit-settlement/payment-history/<int:customer_id>', methods=['GET'])
@login_required
def api_get_credit_payment_history(customer_id):
    """Get payment history for a customer's credit settlements"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get user's store_id
        user_store_id = session.get('store_id')
        
        # Get customer details
        cursor.execute("""
            SELECT customer_id, customer_name, mobile
            FROM customers
            WHERE customer_id = %s
        """, (customer_id,))
        customer = cursor.fetchone()
        
        if not customer:
            return jsonify({
                'success': False,
                'message': 'Customer not found'
            }), 404
        
        # Build query
        query = """
            SELECT 
                cp.payment_id,
                cp.payment_number,
                cp.bill_id,
                b.bill_number,
                cp.store_id,
                s.store_name,
                cp.original_credit_amount,
                cp.payment_amount,
                cp.remaining_credit,
                cp.payment_method,
                cp.payment_reference,
                cp.notes,
                cp.created_at,
                u.full_name as recorded_by_name
            FROM credit_payments cp
            INNER JOIN bills b ON cp.bill_id = b.bill_id
            INNER JOIN stores s ON cp.store_id = s.store_id
            LEFT JOIN users u ON cp.recorded_by = u.user_id
            WHERE cp.customer_id = %s
        """
        
        params = [customer_id]
        
        # If staff user, filter by their store
        if user_store_id:
            query += " AND cp.store_id = %s"
            params.append(user_store_id)
        
        query += " ORDER BY cp.created_at DESC"
        
        cursor.execute(query, params)
        payment_history = cursor.fetchall()
        
        # Calculate summary
        total_payments = sum(payment['payment_amount'] for payment in payment_history)
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'customer': customer,
            'payment_history': payment_history,
            'total_payments': float(total_payments),
            'total_transactions': len(payment_history)
        })
        
    except Exception as e:
        print(f"Error fetching payment history: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


# ---------------------------------------
# API 4: Get Credit Payment Details by Payment ID
# ---------------------------------------
@app.route('/api/credit-settlement/payment/<int:payment_id>', methods=['GET'])
@login_required
def api_get_credit_payment_details(payment_id):
    """Get detailed information about a specific credit payment"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get user's store_id
        user_store_id = session.get('store_id')
        
        # Build query
        query = """
            SELECT 
                cp.payment_id,
                cp.payment_number,
                cp.customer_id,
                c.customer_name,
                c.mobile,
                cp.bill_id,
                b.bill_number,
                b.total_amount as bill_total,
                cp.store_id,
                s.store_name,
                cp.original_credit_amount,
                cp.payment_amount,
                cp.remaining_credit,
                cp.payment_method,
                cp.payment_reference,
                cp.notes,
                cp.created_at,
                u.full_name as recorded_by_name
            FROM credit_payments cp
            INNER JOIN customers c ON cp.customer_id = c.customer_id
            INNER JOIN bills b ON cp.bill_id = b.bill_id
            INNER JOIN stores s ON cp.store_id = s.store_id
            LEFT JOIN users u ON cp.recorded_by = u.user_id
            WHERE cp.payment_id = %s
        """
        
        params = [payment_id]
        
        # If staff user, verify access to this store
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
                'message': 'Payment not found or you do not have access'
            }), 404
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'payment': payment
        })
        
    except Exception as e:
        print(f"Error fetching payment details: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


# ---------------------------------------
# API 5: Get All Credit Settlements Report (Admin)
# ---------------------------------------
@app.route('/api/admin/credit-settlements/report', methods=['GET'])
@admin_required
def api_admin_credit_settlements_report():
    """Get comprehensive report of all credit settlements"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get filters from query params
        store_id = request.args.get('store_id', type=int)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        payment_method = request.args.get('payment_method')
        
        # Build query
        query = """
            SELECT 
                cp.payment_id,
                cp.payment_number,
                cp.customer_id,
                c.customer_name,
                c.mobile,
                cp.bill_id,
                b.bill_number,
                cp.store_id,
                s.store_name,
                cp.payment_amount,
                cp.remaining_credit,
                cp.payment_method,
                cp.payment_reference,
                cp.created_at,
                u.full_name as recorded_by_name
            FROM credit_payments cp
            INNER JOIN customers c ON cp.customer_id = c.customer_id
            INNER JOIN bills b ON cp.bill_id = b.bill_id
            INNER JOIN stores s ON cp.store_id = s.store_id
            LEFT JOIN users u ON cp.recorded_by = u.user_id
            WHERE 1=1
        """
        
        params = []
        
        if store_id:
            query += " AND cp.store_id = %s"
            params.append(store_id)
        
        if start_date:
            query += " AND DATE(cp.created_at) >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND DATE(cp.created_at) <= %s"
            params.append(end_date)
        
        if payment_method:
            query += " AND cp.payment_method = %s"
            params.append(payment_method)
        
        query += " ORDER BY cp.created_at DESC"
        
        cursor.execute(query, params)
        settlements = cursor.fetchall()
        
        # Calculate summary statistics
        total_settlements = len(settlements)
        total_amount_collected = sum(s['payment_amount'] for s in settlements)
        
        # Group by payment method
        method_summary = {}
        for settlement in settlements:
            method = settlement['payment_method']
            if method not in method_summary:
                method_summary[method] = {'count': 0, 'amount': 0}
            method_summary[method]['count'] += 1
            method_summary[method]['amount'] += settlement['payment_amount']
        
        # Group by store
        store_summary = {}
        for settlement in settlements:
            store_name = settlement['store_name']
            if store_name not in store_summary:
                store_summary[store_name] = {'count': 0, 'amount': 0}
            store_summary[store_name]['count'] += 1
            store_summary[store_name]['amount'] += settlement['payment_amount']
        
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'settlements': settlements,
            'summary': {
                'total_settlements': total_settlements,
                'total_amount_collected': float(total_amount_collected),
                'method_breakdown': method_summary,
                'store_breakdown': store_summary
            }
        })
        
    except Exception as e:
        print(f"Error fetching credit settlements report: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500
#+++++++++++++++
@app.route('/api/credit-notes/advance-payment', methods=['POST'])
@staff_required
def api_create_advance_payment_credit_note():
    """Create a credit note for advance payment (works with existing schema)"""
    try:
        data = request.get_json()
        
        # Validate required fields
        customer_id = data.get('customer_id')
        amount = data.get('amount')
        payment_method = data.get('payment_method')
        payment_reference = data.get('payment_reference')
        notes = data.get('notes')
        
        if not customer_id or not amount:
            return jsonify({
                'success': False,
                'message': 'Customer ID and amount are required'
            }), 400
        
        if float(amount) <= 0:
            return jsonify({
                'success': False,
                'message': 'Amount must be greater than 0'
            }), 400
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get staff's store_id
        store_id = session.get('store_id')
        staff_id = session.get('user_id')
        
        if not store_id or not staff_id:
            cursor.close()
            connection.close()
            return jsonify({
                'success': False,
                'message': 'Store or staff not assigned'
            }), 400
        
        # Verify customer exists and get details
        cursor.execute("""
            SELECT customer_id, customer_name, mobile 
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
        
        # Generate credit note number
        cursor.execute("""
            SELECT credit_number FROM credit_notes 
            WHERE store_id = %s 
            ORDER BY credit_id DESC LIMIT 1
        """, (store_id,))
        
        last_cn = cursor.fetchone()
        
        if last_cn and last_cn['credit_number']:
            # Extract number from format like CN-001 or CN-ADV-001
            try:
                parts = last_cn['credit_number'].split('-')
                last_num = int(parts[-1])
                new_num = last_num + 1
            except:
                new_num = 1
        else:
            new_num = 1
        
        credit_number = f"CN-ADV-{new_num:05d}"
        
        # Create a special "advance payment" bill entry
        # This is needed because bill_id is NOT NULL in credit_notes table
        bill_number = f"ADV-{new_num:05d}"
        
        # Create payment_split JSON for advance payment
        payment_split = {
            'cash': float(amount) if payment_method == 'cash' else 0,
            'upi': float(amount) if payment_method == 'upi' else 0,
            'bank_transfer': float(amount) if payment_method == 'bank_transfer' else 0,
            'card': float(amount) if payment_method == 'card' else 0,
            'cheque': float(amount) if payment_method == 'cheque' else 0,
            'credit': 0
        }
        
        # Insert dummy bill for advance payment
        insert_bill_query = """
            INSERT INTO bills (
                bill_number, 
                store_id, 
                staff_id, 
                customer_id,
                customer_name,
                customer_contact,
                subtotal,
                discount_amount,
                total_amount,
                payment_split,
                notes,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s, %s, %s, NOW())
        """
        
        bill_notes = f"ADVANCE PAYMENT - {payment_method.upper()}"
        if payment_reference:
            bill_notes += f" | Ref: {payment_reference}"
        
        cursor.execute(insert_bill_query, (
            bill_number,
            store_id,
            staff_id,
            customer_id,
            customer['customer_name'],
            customer['mobile'],
            amount,
            amount,
            json.dumps(payment_split),
            bill_notes
        ))
        
        bill_id = cursor.lastrowid
        
        # Create the credit note
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
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'active', %s, NOW())
        """
        
        cn_notes = f"Advance Payment - {payment_method}"
        if payment_reference:
            cn_notes += f" | Ref: {payment_reference}"
        if notes:
            cn_notes += f" | {notes}"
        
        cursor.execute(insert_cn_query, (
            credit_number,
            bill_id,
            store_id,
            staff_id,
            customer_id,
            amount,
            amount,  # remaining_balance starts as full amount
            cn_notes
        ))
        
        credit_id = cursor.lastrowid
        
        connection.commit()
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'credit_id': credit_id,
            'credit_number': credit_number,
            'message': 'Advance payment recorded successfully'
        })
        
    except Exception as e:
        print(f"Error creating advance payment credit note: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/credit-notes/advance-payment-v2', methods=['POST'])
@staff_required
def api_create_advance_payment_credit_note_v2():
    """Create a credit note for advance payment (improved version with schema changes)"""
    try:
        data = request.get_json()
        
        customer_id = data.get('customer_id')
        amount = data.get('amount')
        payment_method = data.get('payment_method')
        payment_reference = data.get('payment_reference')
        notes = data.get('notes')
        
        if not customer_id or not amount:
            return jsonify({
                'success': False,
                'message': 'Customer ID and amount are required'
            }), 400
        
        if float(amount) <= 0:
            return jsonify({
                'success': False,
                'message': 'Amount must be greater than 0'
            }), 400
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        store_id = session.get('store_id')
        staff_id = session.get('user_id')
        
        if not store_id or not staff_id:
            cursor.close()
            connection.close()
            return jsonify({
                'success': False,
                'message': 'Store or staff not assigned'
            }), 400
        
        # Verify customer exists
        cursor.execute("SELECT customer_id FROM customers WHERE customer_id = %s", (customer_id,))
        customer = cursor.fetchone()
        
        if not customer:
            cursor.close()
            connection.close()
            return jsonify({
                'success': False,
                'message': 'Customer not found'
            }), 404
        
        # Generate credit note number
        cursor.execute("""
            SELECT credit_number FROM credit_notes 
            WHERE store_id = %s 
            ORDER BY credit_id DESC LIMIT 1
        """, (store_id,))
        
        last_cn = cursor.fetchone()
        
        if last_cn and last_cn['credit_number']:
            try:
                last_num = int(last_cn['credit_number'].split('-')[-1])
                new_num = last_num + 1
            except:
                new_num = 1
        else:
            new_num = 1
        
        credit_number = f"CN-ADV-{new_num:05d}"
        
        # Build notes
        cn_notes = f"Advance Payment - {payment_method}"
        if payment_reference:
            cn_notes += f" | Ref: {payment_reference}"
        if notes:
            cn_notes += f" | {notes}"
        
        # Insert credit note (with bill_id as NULL for advance payments)
        insert_cn_query = """
            INSERT INTO credit_notes (
                credit_number,
                credit_type,
                bill_id,
                store_id,
                staff_id,
                customer_id,
                total_amount,
                remaining_balance,
                status,
                notes,
                created_at
            ) VALUES (%s, 'advance', NULL, %s, %s, %s, %s, %s, 'active', %s, NOW())
        """
        
        cursor.execute(insert_cn_query, (
            credit_number,
            store_id,
            staff_id,
            customer_id,
            amount,
            amount,
            cn_notes
        ))
        
        credit_id = cursor.lastrowid
        
        connection.commit()
        cursor.close()
        connection.close()
        
        return jsonify({
            'success': True,
            'credit_id': credit_id,
            'credit_number': credit_number,
            'message': 'Advance payment recorded successfully'
        })
        
    except Exception as e:
        print(f"Error creating advance payment credit note: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500
 #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
# ============================================================================
# BILL AND CREDIT NOTE PRINTING ENDPOINTS - FIXED FOR YOUR SCHEMA
# ============================================================================
# Add these endpoints to your app.py file

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
import io
from datetime import datetime


@app.route('/api/bills/<int:bill_id>/print', methods=['GET'])
@staff_required
def api_print_bill(bill_id):
    """Generate printable PDF for a bill with all details"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get bill details with store and customer info
        cursor.execute("""
            SELECT 
                b.bill_id,
                b.bill_number,
                b.subtotal,
                b.discount_type,
                b.discount_value,
                b.discount_amount,
                b.total_amount,
                b.payment_split,
                b.notes,
                b.created_at,
                b.customer_name,
                b.customer_contact,
                s.store_name,
                s.address as store_address,
                s.contact as store_contact,
                s.email as store_email,
                u.full_name as staff_name,
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
        
        # Get bill items - using actual column names from your schema
        cursor.execute("""
            SELECT 
                product_name,
                quantity,
                unit_price,
                item_discount,
                total as line_total
            FROM bill_items
            WHERE bill_id = %s
            ORDER BY bill_item_id
        """, (bill_id,))
        
        items = cursor.fetchall()
        
        # Check if credit note was issued for this bill
        cursor.execute("""
            SELECT credit_number
            FROM credit_notes
            WHERE bill_id = %s
            LIMIT 1
        """, (bill_id,))
        
        credit_note = cursor.fetchone()
        
        cursor.close()
        connection.close()
        
        # Generate PDF
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, 
                              rightMargin=30, leftMargin=30,
                              topMargin=30, bottomMargin=30)
        
        # Container for the PDF elements
        elements = []
        
        # Styles
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#1e293b'),
            spaceAfter=12,
            alignment=TA_CENTER,
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
        
        # Store Header
        elements.append(Paragraph(bill['store_name'], title_style))
        
        store_info = f"""
        <para alignment="center">
        {bill['store_address'] or ''}<br/>
        Phone: {bill['store_contact'] or ''} | Email: {bill['store_email'] or ''}<br/>
        </para>
        """
        elements.append(Paragraph(store_info, normal_style))
        elements.append(Spacer(1, 0.2*inch))
        
        # Invoice Title
        invoice_title = ParagraphStyle(
            'InvoiceTitle',
            parent=styles['Heading1'],
            fontSize=18,
            textColor=colors.HexColor('#6366f1'),
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        elements.append(Paragraph("INVOICE", invoice_title))
        elements.append(Spacer(1, 0.15*inch))
        
        # Bill Info and Customer Info side by side
        bill_info_data = [
            ['Bill Number:', bill['bill_number']],
            ['Date:', bill['created_at'].strftime('%d-%b-%Y %I:%M %p')],
            ['Staff:', bill['staff_name']],
        ]
        
        customer_info_data = [
            ['Customer:', bill['customer_name'] or 'Walk-in Customer'],
            ['Mobile:', bill['customer_contact'] or 'N/A'],
            ['Address:', bill['customer_address'] or 'N/A'],
        ]
        
        # Create two-column layout for bill and customer info
        info_table_data = []
        for i in range(max(len(bill_info_data), len(customer_info_data))):
            row = []
            if i < len(bill_info_data):
                row.extend(bill_info_data[i])
            else:
                row.extend(['', ''])
            
            row.append('')  # Spacer column
            
            if i < len(customer_info_data):
                row.extend(customer_info_data[i])
            else:
                row.extend(['', ''])
            
            info_table_data.append(row)
        
        info_table = Table(info_table_data, colWidths=[1.5*inch, 1.8*inch, 0.3*inch, 1.5*inch, 1.8*inch])
        info_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (3, 0), (3, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#374151')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        elements.append(info_table)
        elements.append(Spacer(1, 0.2*inch))
        
        # Items Table - Using your actual schema columns
        items_data = [['#', 'Item Description', 'Qty', 'Unit Price', 'Discount', 'Line Total']]
        
        for idx, item in enumerate(items, 1):
            items_data.append([
                str(idx),
                item['product_name'],
                f"{float(item['quantity']):.2f}",
                f"Rs {float(item['unit_price']):.2f}",
                f"Rs {float(item['item_discount'] or 0):.2f}",
                f"Rs {float(item['line_total']):.2f}"
            ])
        
        items_table = Table(items_data, colWidths=[0.4*inch, 3*inch, 0.8*inch, 1.1*inch, 1*inch, 1.2*inch])
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
            ('ALIGN', (0, 1), (0, -1), 'CENTER'),  # # column
            ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),  # Numeric columns
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
        
        # Summary Section
        summary_data = [
            ['Subtotal:', f"Rs {float(bill['subtotal']):.2f}"],
        ]
        
        if bill['discount_amount'] and float(bill['discount_amount']) > 0:
            discount_label = f"Discount ({bill['discount_type']} - {bill['discount_value']}):"
            summary_data.append([discount_label, f"- Rs {float(bill['discount_amount']):.2f}"])
        
        summary_data.append(['', ''])  # Spacer
        summary_data.append(['Total Amount:', f"Rs {float(bill['total_amount']):.2f}"])
        
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
        
        # Payment Details
        if bill['payment_split']:
            payment_split = json.loads(bill['payment_split'])
            elements.append(Paragraph("Payment Details", heading_style))
            
            payment_data = [['Payment Method', 'Amount']]
            
            payment_methods = {
                'cash': 'Cash',
                'upi': 'UPI',
                'card': 'Card',
                'bank_transfer': 'Bank Transfer',
                'cheque': 'Cheque',
                'credit': 'Credit',
                'credit_note': 'Credit Note'
            }
            
            for key, label in payment_methods.items():
                if key in payment_split and float(payment_split[key]) > 0:
                    payment_data.append([label, f"Rs {float(payment_split[key]):.2f}"])
            
            if len(payment_data) > 1:  # If there are any payments
                payment_table = Table(payment_data, colWidths=[3*inch, 2*inch])
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
        
        # Credit Note Info (if applicable)
        if credit_note:
            cn_style = ParagraphStyle(
                'CreditNote',
                parent=styles['Normal'],
                fontSize=10,
                textColor=colors.HexColor('#ef4444'),
                fontName='Helvetica-Bold'
            )
            elements.append(Paragraph(f"⚠ Credit Note Issued: {credit_note['credit_number']}", cn_style))
            elements.append(Spacer(1, 0.1*inch))
        
        # Notes
        if bill['notes']:
            elements.append(Paragraph("Notes", heading_style))
            elements.append(Paragraph(bill['notes'], normal_style))
            elements.append(Spacer(1, 0.15*inch))
        
        # Footer
        elements.append(Spacer(1, 0.3*inch))
        footer_style = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#6b7280'),
            alignment=TA_CENTER
        )
        elements.append(Paragraph("Thank you for your business!", footer_style))
        elements.append(Paragraph("This is a computer generated invoice", footer_style))
        
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
            download_name=f'Invoice_{bill["bill_number"]}.pdf'
        )
        
    except Exception as e:
        print(f"Error generating bill PDF: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/credit-notes/<int:credit_id>/print', methods=['GET'])
@staff_required
def api_print_credit_note(credit_id):
    """Generate printable PDF for a credit note with all details"""
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        # Get credit note details
        cursor.execute("""
            SELECT 
                cn.credit_id,
                cn.credit_number,
                cn.total_amount,
                cn.remaining_balance,
                cn.status,
                cn.notes,
                cn.created_at,
                c.customer_name,
                c.mobile,
                c.address as customer_address,
                b.bill_number,
                s.store_name,
                s.address as store_address,
                s.contact as store_contact,
                s.email as store_email,
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
        
        # Check if it's an advance payment (bill_number starts with ADV-)
        is_advance = credit_note['bill_number'] and credit_note['bill_number'].startswith('ADV-')
        
        # Get return items (if it's a return type credit note)
        return_items = []
        if not is_advance and credit_note['bill_number']:
            cursor.execute("""
                SELECT 
                    product_name,
                    quantity,
                    unit_price,
                    refund_amount
                FROM return_items
                WHERE credit_id = %s
                ORDER BY return_id
            """, (credit_id,))
            return_items = cursor.fetchall()
        
        # Get usage history
        cursor.execute("""
            SELECT 
                b.bill_number,
                cu.amount_used,
                cu.used_at
            FROM credit_note_usage cu
            INNER JOIN bills b ON cu.bill_id = b.bill_id
            WHERE cu.credit_id = %s
            ORDER BY cu.used_at DESC
        """, (credit_id,))
        usage_history = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        # Generate PDF
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4,
                              rightMargin=30, leftMargin=30,
                              topMargin=30, bottomMargin=30)
        
        elements = []
        
        # Styles
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#1e293b'),
            spaceAfter=12,
            alignment=TA_CENTER,
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
        
        # Store Header
        elements.append(Paragraph(credit_note['store_name'], title_style))
        
        store_info = f"""
        <para alignment="center">
        {credit_note['store_address'] or ''}<br/>
        Phone: {credit_note['store_contact'] or ''} | Email: {credit_note['store_email'] or ''}<br/>
        </para>
        """
        elements.append(Paragraph(store_info, normal_style))
        elements.append(Spacer(1, 0.2*inch))
        
        # Credit Note Title
        cn_title_style = ParagraphStyle(
            'CNTitle',
            parent=styles['Heading1'],
            fontSize=18,
            textColor=colors.HexColor('#8b5cf6') if is_advance else colors.HexColor('#10b981'),
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        cn_type = "ADVANCE PAYMENT RECEIPT" if is_advance else "CREDIT NOTE"
        elements.append(Paragraph(cn_type, cn_title_style))
        elements.append(Spacer(1, 0.15*inch))
        
        # Credit Note Info and Customer Info
        cn_info_data = [
            ['Credit Note #:', credit_note['credit_number']],
            ['Date:', credit_note['created_at'].strftime('%d-%b-%Y %I:%M %p')],
            ['Staff:', credit_note['staff_name']],
        ]
        
        if not is_advance:
            cn_info_data.append(['Original Bill #:', credit_note['bill_number'] or 'N/A'])
        
        customer_info_data = [
            ['Customer:', credit_note['customer_name'] or 'N/A'],
            ['Mobile:', credit_note['mobile'] or 'N/A'],
            ['Address:', credit_note['customer_address'] or 'N/A'],
        ]
        
        # Two-column layout
        info_table_data = []
        for i in range(max(len(cn_info_data), len(customer_info_data))):
            row = []
            if i < len(cn_info_data):
                row.extend(cn_info_data[i])
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
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#374151')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        elements.append(info_table)
        elements.append(Spacer(1, 0.2*inch))
        
        # Return Items (if applicable)
        if return_items:
            elements.append(Paragraph("Returned Items", heading_style))
            
            items_data = [['#', 'Item Description', 'Quantity', 'Unit Price', 'Refund Amount']]
            
            for idx, item in enumerate(return_items, 1):
                items_data.append([
                    str(idx),
                    item['product_name'],
                    f"{float(item['quantity']):.2f}",
                    f"Rs {float(item['unit_price']):.2f}",
                    f"Rs {float(item['refund_amount']):.2f}"
                ])
            
            items_table = Table(items_data, colWidths=[0.4*inch, 3*inch, 1*inch, 1.2*inch, 1.4*inch])
            items_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#10b981')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('ALIGN', (0, 1), (0, -1), 'CENTER'),
                ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9fafb')]),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ]))
            elements.append(items_table)
            elements.append(Spacer(1, 0.15*inch))
        
        # Credit Summary
        summary_data = [
            ['Total Credit Amount:', f"Rs {float(credit_note['total_amount']):.2f}"],
            ['Amount Used:', f"Rs {float(credit_note['total_amount']) - float(credit_note['remaining_balance']):.2f}"],
            ['Remaining Balance:', f"Rs {float(credit_note['remaining_balance']):.2f}"],
        ]
        
        summary_table = Table(summary_data, colWidths=[4.5*inch, 2.4*inch])
        summary_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 11),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('LINEABOVE', (0, 0), (-1, 0), 1, colors.HexColor('#e5e7eb')),
            ('LINEBELOW', (0, -1), (-1, -1), 2, colors.HexColor('#8b5cf6') if is_advance else colors.HexColor('#10b981')),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        elements.append(summary_table)
        elements.append(Spacer(1, 0.2*inch))
        
        # Usage History
        if usage_history:
            elements.append(Paragraph("Usage History", heading_style))
            
            usage_data = [['Date', 'Bill Number', 'Amount Used']]
            
            for usage in usage_history:
                usage_data.append([
                    usage['used_at'].strftime('%d-%b-%Y %I:%M %p'),
                    usage['bill_number'],
                    f"Rs {float(usage['amount_used']):.2f}"
                ])
            
            usage_table = Table(usage_data, colWidths=[2*inch, 2.5*inch, 1.5*inch])
            usage_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f3f4f6')),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('ALIGN', (2, 0), (2, -1), 'RIGHT'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ]))
            elements.append(usage_table)
            elements.append(Spacer(1, 0.15*inch))
        
        # Notes
        if credit_note['notes']:
            elements.append(Paragraph("Notes", heading_style))
            elements.append(Paragraph(credit_note['notes'], normal_style))
            elements.append(Spacer(1, 0.15*inch))
        
        # Status Badge
        status_color = {
            'active': colors.HexColor('#10b981'),
            'fully_used': colors.HexColor('#6b7280'),
            'expired': colors.HexColor('#ef4444')
        }.get(credit_note['status'], colors.HexColor('#6b7280'))
        
        status_style = ParagraphStyle(
            'Status',
            parent=styles['Normal'],
            fontSize=12,
            textColor=status_color,
            fontName='Helvetica-Bold',
            alignment=TA_CENTER
        )
        elements.append(Paragraph(f"Status: {credit_note['status'].upper().replace('_', ' ')}", status_style))
        
        # Footer
        elements.append(Spacer(1, 0.3*inch))
        footer_style = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#6b7280'),
            alignment=TA_CENTER
        )
        
        if is_advance:
            elements.append(Paragraph("This advance payment can be used for future purchases", footer_style))
        else:
            elements.append(Paragraph("This credit note can be redeemed for future purchases", footer_style))
        
        elements.append(Paragraph("Please keep this document for your records", footer_style))
        
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
            download_name=f'CreditNote_{credit_note["credit_number"]}.pdf'
        )
        
    except Exception as e:
        print(f"Error generating credit note PDF: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500
if __name__ == '__main__':

    app.run(debug=True, host='0.0.0.0', port=5000)
