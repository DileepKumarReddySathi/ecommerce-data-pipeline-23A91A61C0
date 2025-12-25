-- ==================================================
-- Production Schema for E-Commerce Data Pipeline
-- Fully normalized (3NF)
-- Corrected for real-world data lengths
-- ==================================================

CREATE SCHEMA IF NOT EXISTS production;

-- ==================================================
-- Customers
-- ==================================================
DROP TABLE IF EXISTS production.customers CASCADE;

CREATE TABLE production.customers (
    customer_id        VARCHAR(20) PRIMARY KEY,
    first_name         VARCHAR(100) NOT NULL,
    last_name          VARCHAR(100) NOT NULL,
    email              VARCHAR(150) NOT NULL UNIQUE,
    phone              VARCHAR(30),
    registration_date  DATE NOT NULL,
    city               VARCHAR(150),
    state              VARCHAR(100),
    country             VARCHAR(120),
    age_group          VARCHAR(20),
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==================================================
-- Products
-- ==================================================
DROP TABLE IF EXISTS production.products CASCADE;

CREATE TABLE production.products (
    product_id        VARCHAR(20) PRIMARY KEY,
    product_name      VARCHAR(150) NOT NULL,
    category          VARCHAR(100),
    sub_category      VARCHAR(100),
    price             DECIMAL(12,2) NOT NULL CHECK (price >= 0),
    cost              DECIMAL(12,2) NOT NULL CHECK (cost >= 0),
    brand             VARCHAR(100),
    stock_quantity    INTEGER CHECK (stock_quantity >= 0),
    supplier_id       VARCHAR(30),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==================================================
-- Transactions
-- ==================================================
DROP TABLE IF EXISTS production.transactions CASCADE;

CREATE TABLE production.transactions (
    transaction_id    VARCHAR(20) PRIMARY KEY,
    customer_id       VARCHAR(20) NOT NULL,
    transaction_date  DATE NOT NULL,
    transaction_time  TIME NOT NULL,
    payment_method    VARCHAR(50),
    shipping_address  TEXT,
    total_amount      DECIMAL(12,2) CHECK (total_amount >= 0),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_txn_customer
        FOREIGN KEY (customer_id)
        REFERENCES production.customers(customer_id)
);

CREATE INDEX idx_transactions_date
    ON production.transactions(transaction_date);

CREATE INDEX idx_transactions_customer
    ON production.transactions(customer_id);

-- ==================================================
-- Transaction Items
-- ==================================================
DROP TABLE IF EXISTS production.transaction_items CASCADE;

CREATE TABLE production.transaction_items (
    item_id              VARCHAR(20) PRIMARY KEY,
    transaction_id       VARCHAR(20) NOT NULL,
    product_id           VARCHAR(20) NOT NULL,
    quantity             INTEGER CHECK (quantity > 0),
    unit_price           DECIMAL(12,2) CHECK (unit_price >= 0),
    discount_percentage  DECIMAL(5,2) CHECK (discount_percentage BETWEEN 0 AND 100),
    line_total           DECIMAL(12,2) CHECK (line_total >= 0),
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_item_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES production.transactions(transaction_id),

    CONSTRAINT fk_item_product
        FOREIGN KEY (product_id)
        REFERENCES production.products(product_id)
);

CREATE INDEX idx_items_transaction
    ON production.transaction_items(transaction_id);

CREATE INDEX idx_items_product
    ON production.transaction_items(product_id);
