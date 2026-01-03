BEGIN;

-- =====================================
-- AGG CUSTOMER METRICS
-- =====================================
TRUNCATE TABLE warehouse.agg_customer_metrics;

INSERT INTO warehouse.agg_customer_metrics (
    customer_key,
    total_transactions,
    total_spent,
    avg_order_value,
    last_purchase_date
)
SELECT
    fs.customer_key,
    COUNT(DISTINCT fs.transaction_id),
    SUM(fs.line_total),
    AVG(fs.line_total),
    MAX(d.full_date)
FROM warehouse.fact_sales fs
JOIN warehouse.dim_date d
    ON fs.date_key = d.date_key
GROUP BY fs.customer_key;


-- =====================================
-- AGG DAILY SALES
-- =====================================
TRUNCATE TABLE warehouse.agg_daily_sales;

INSERT INTO warehouse.agg_daily_sales (
    date_key,
    total_transactions,
    total_revenue,
    total_profit,
    unique_customers
)
SELECT
    fs.date_key,
    COUNT(DISTINCT fs.transaction_id),
    SUM(fs.line_total),
    SUM(fs.profit),
    COUNT(DISTINCT fs.customer_key)
FROM warehouse.fact_sales fs
GROUP BY fs.date_key;


-- =====================================
-- AGG PRODUCT PERFORMANCE
-- =====================================
TRUNCATE TABLE warehouse.agg_product_performance;

INSERT INTO warehouse.agg_product_performance (
    product_key,
    total_quantity_sold,
    total_revenue,
    total_profit
)
SELECT
    fs.product_key,
    SUM(fs.quantity),
    SUM(fs.line_total),
    SUM(fs.profit)
FROM warehouse.fact_sales fs
GROUP BY fs.product_key;

COMMIT;
