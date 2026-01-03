BEGIN;

-- ============================
-- DIM CUSTOMERS
-- ============================
INSERT INTO warehouse.dim_customers (
    customer_id,
    email,
    city,
    state,
    country,
    age_group
)
SELECT DISTINCT
    customer_id,
    email,
    city,
    state,
    country,
    age_group
FROM production.customers
ON CONFLICT (customer_id) DO NOTHING;

-- ============================
-- DIM PRODUCTS (FIXED)
-- ============================
INSERT INTO warehouse.dim_products (
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    price_range,
    effective_date,
    is_current
)
SELECT DISTINCT
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    CASE
        WHEN price < 500 THEN 'Low'
        WHEN price BETWEEN 500 AND 1500 THEN 'Medium'
        ELSE 'High'
    END AS price_range,
    CURRENT_DATE,
    TRUE
FROM production.products
ON CONFLICT (product_id) DO NOTHING;

-- ============================
-- DIM DATE
-- ============================
INSERT INTO warehouse.dim_date (
    date_key,
    full_date,
    year,
    quarter,
    month,
    day,
    month_name,
    day_name,
    week_of_year,
    is_weekend
)
SELECT DISTINCT
    TO_CHAR(t.transaction_date, 'YYYYMMDD')::INT,
    t.transaction_date,
    EXTRACT(YEAR FROM t.transaction_date)::INT,
    EXTRACT(QUARTER FROM t.transaction_date)::INT,
    EXTRACT(MONTH FROM t.transaction_date)::INT,
    EXTRACT(DAY FROM t.transaction_date)::INT,
    TRIM(TO_CHAR(t.transaction_date, 'Month')),
    TRIM(TO_CHAR(t.transaction_date, 'Day')),
    EXTRACT(WEEK FROM t.transaction_date)::INT,
    CASE
        WHEN EXTRACT(DOW FROM t.transaction_date) IN (0,6) THEN TRUE
        ELSE FALSE
    END
FROM production.transactions t
ON CONFLICT (date_key) DO NOTHING;

-- ============================
-- FACT SALES (FINAL VERSION)
-- ============================
INSERT INTO warehouse.fact_sales (
    date_key,
    customer_key,
    product_key,
    payment_method_key,
    transaction_id,
    quantity,
    unit_price,
    discount_amount,
    line_total,
    profit
)
SELECT
    TO_CHAR(t.transaction_date, 'YYYYMMDD')::INT         AS date_key,
    dc.customer_key                                     AS customer_key,
    dp.product_key                                      AS product_key,
    pm.payment_method_key                               AS payment_method_key,
    ti.transaction_id,
    ti.quantity,
    ti.unit_price,
    (ti.unit_price * ti.quantity * ti.discount_percentage / 100) AS discount_amount,
    ti.line_total,
    ti.line_total                                       AS profit
FROM production.transaction_items ti
JOIN production.transactions t
    ON ti.transaction_id = t.transaction_id
JOIN warehouse.dim_customers dc
    ON dc.customer_id = t.customer_id
JOIN warehouse.dim_products dp
  ON dp.product_id = ti.product_id
 AND dp.is_current = TRUE
JOIN warehouse.dim_payment_method pm
    ON pm.payment_method_name = t.payment_method;


COMMIT;
