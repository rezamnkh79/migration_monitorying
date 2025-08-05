-- Create database and user for Debezium
CREATE DATABASE IF NOT EXISTS inventory CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Grant privileges for Debezium user
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
GRANT ALL PRIVILEGES ON inventory.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

USE inventory;

-- Users table
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    full_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSON
);

-- Products table
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INT DEFAULT 0,
    category_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    status ENUM('active', 'inactive', 'discontinued') DEFAULT 'active'
);

-- Orders table
CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    order_number VARCHAR(50) NOT NULL UNIQUE,
    total_amount DECIMAL(12,2) NOT NULL,
    order_status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shipping_address TEXT,
    notes TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Order items table
CREATE TABLE order_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_price DECIMAL(12,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Insert sample data
INSERT INTO users (username, email, full_name, metadata) VALUES
('john_doe', 'john@example.com', 'John Doe', '{"preferences": {"language": "en", "notifications": true}}'),
('jane_smith', 'jane@example.com', 'Jane Smith', '{"preferences": {"language": "fa", "notifications": false}}'),
('admin_user', 'admin@example.com', 'Admin User', '{"role": "admin", "permissions": ["read", "write", "delete"]}');

INSERT INTO products (name, description, price, stock_quantity, category_id) VALUES
('Dell XPS 13 Laptop', 'Amazing laptop with 13-inch display', 1299.99, 50, 1),
('Logitech MX Master 3 Mouse', 'Wireless mouse with high precision', 99.99, 200, 2),
('Razer Mechanical Keyboard', 'Gaming keyboard with mechanical keys', 149.99, 75, 2),
('Samsung 27-inch Monitor', '4K monitor with excellent color rendering', 399.99, 30, 3);

INSERT INTO orders (user_id, order_number, total_amount, order_status, shipping_address) VALUES
(1, 'ORD-001', 1399.98, 'processing', 'Tehran, Valiasr Street, No. 123'),
(2, 'ORD-002', 249.98, 'shipped', 'Isfahan, Chahar Bagh Street, No. 456'),
(3, 'ORD-003', 399.99, 'delivered', 'Shiraz, Zand Street, No. 789');

INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(1, 1, 1, 1299.99, 1299.99),
(1, 2, 1, 99.99, 99.99),
(2, 3, 1, 149.99, 149.99),
(2, 2, 1, 99.99, 99.99),
(3, 4, 1, 399.99, 399.99); 