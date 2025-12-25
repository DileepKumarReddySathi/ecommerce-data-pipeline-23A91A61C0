-- ==================================================
-- STAGING SCHEMA
-- Purpose: Raw landing zone for CSV ingestion
-- Design: Minimal constraints, wide columns
-- ==================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- --------------------------------------------------
-- Staging Customers Table
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id        VARCHAR(20),
    first_name         VARCHAR(50),
    last_name          VARCHAR(50),
    email              VARCHAR(150),
    phone              VARCHAR(100),
    registration_date  DATE,
    city               VARCHAR(100),
    state              VARCHAR(100),
    country            VARCHAR(150),
    age_group          VARCHAR(20),
    loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- --------------------------------------------------
-- Staging Products Table
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.products (
    product_id       VARCHAR(20),
    product_name     VARCHAR(150),
    category         VARCHAR(50),
    sub_category     VARCHAR(50),
    price            DECIMAL(12,2),
    cost             DECIMAL(12,2),
    brand            VARCHAR(100),
    stock_quantity   INTEGER,
    supplier_id      VARCHAR(50),
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- --------------------------------------------------
-- Staging Transactions Table
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id     VARCHAR(20),
    customer_id        VARCHAR(20),
    transaction_date   DATE,
    transaction_time   TIME,
    payment_method     VARCHAR(50),
    shipping_address   TEXT,
    total_amount       DECIMAL(12,2),
    loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- --------------------------------------------------
-- Staging Transaction Items Table
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.transaction_items (
    item_id              VARCHAR(20),
    transaction_id       VARCHAR(20),
    product_id           VARCHAR(20),
    quantity             INTEGER,
    unit_price           DECIMAL(12,2),
    discount_percentage  DECIMAL(5,2),
    line_total            DECIMAL(12,2),
    loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
