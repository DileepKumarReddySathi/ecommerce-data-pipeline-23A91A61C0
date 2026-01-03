BEGIN;

-- ----------------------------
-- 1️⃣ CUSTOMERS
-- ----------------------------
INSERT INTO production.customers (
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    registration_date,
    city,
    state,
    country,
    age_group
)
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    registration_date,
    city,
    state,
    country,
    age_group
FROM staging.customers;


-- ----------------------------
-- 2️⃣ PRODUCTS
-- ----------------------------
INSERT INTO production.products (
    product_id,
    product_name,
    category,
    sub_category,
    price,
    cost,
    brand,
    stock_quantity,
    supplier_id
)
SELECT
    product_id,
    product_name,
    category,
    sub_category,
    price,
    cost,
    brand,
    stock_quantity,
    supplier_id
FROM staging.products;


-- ----------------------------
-- 3️⃣ TRANSACTIONS
-- ----------------------------
INSERT INTO production.transactions (
    transaction_id,
    customer_id,
    transaction_date,
    transaction_time,
    payment_method,
    shipping_address,
    total_amount
)
SELECT
    transaction_id,
    customer_id,
    transaction_date,
    transaction_time,
    payment_method,
    shipping_address,
    total_amount
FROM staging.transactions;


-- ----------------------------
-- 4️⃣ TRANSACTION ITEMS
-- ----------------------------
INSERT INTO production.transaction_items (
    item_id,
    transaction_id,
    product_id,
    quantity,
    unit_price,
    discount_percentage,
    line_total
)
SELECT
    item_id,
    transaction_id,
    product_id,
    quantity,
    unit_price,
    discount_percentage,
    line_total
FROM staging.transaction_items;

COMMIT;
