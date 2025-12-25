-- =========================================================
-- Query 1: Top 10 Products by Revenue
-- Objective: Identify best-selling products by total revenue
-- =========================================================
SELECT
    dp.product_name,
    dp.category,
    SUM(fs.line_total) AS total_revenue,
    SUM(fs.quantity) AS units_sold,
    ROUND(AVG(fs.unit_price), 2) AS avg_price
FROM warehouse.fact_sales fs
JOIN warehouse.dim_products dp
    ON fs.product_key = dp.product_key
GROUP BY dp.product_name, dp.category
ORDER BY total_revenue DESC
LIMIT 10;


-- =========================================================
-- Query 2: Monthly Sales Trend
-- Objective: Analyze revenue trends over time
-- =========================================================
SELECT
    dd.year || '-' || LPAD(dd.month::TEXT, 2, '0') AS year_month,
    SUM(fs.line_total) AS total_revenue,
    COUNT(DISTINCT fs.transaction_id) AS total_transactions,
    ROUND(AVG(fs.line_total), 2) AS average_order_value,
    COUNT(DISTINCT fs.customer_key) AS unique_customers
FROM warehouse.fact_sales fs
JOIN warehouse.dim_date dd
    ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month
ORDER BY year_month;


-- =========================================================
-- Query 3: Customer Segmentation Analysis
-- Objective: Segment customers based on lifetime spend
-- =========================================================
WITH customer_totals AS (
    SELECT
        customer_key,
        SUM(line_total) AS total_spent,
        AVG(line_total) AS avg_transaction_value
    FROM warehouse.fact_sales
    GROUP BY customer_key
)
SELECT
    CASE
        WHEN total_spent < 1000 THEN '$0-$1,000'
        WHEN total_spent < 5000 THEN '$1,000-$5,000'
        WHEN total_spent < 10000 THEN '$5,000-$10,000'
        ELSE '$10,000+'
    END AS spending_segment,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_revenue,
    ROUND(AVG(avg_transaction_value), 2) AS avg_transaction_value
FROM customer_totals
GROUP BY spending_segment
ORDER BY total_revenue DESC;


-- =========================================================
-- Query 4: Category Performance
-- Objective: Compare revenue and profit across categories
-- =========================================================
SELECT
    dp.category,
    SUM(fs.line_total) AS total_revenue,
    SUM(fs.profit) AS total_profit,
    ROUND((SUM(fs.profit) / NULLIF(SUM(fs.line_total), 0)) * 100, 2) AS profit_margin_pct,
    SUM(fs.quantity) AS units_sold
FROM warehouse.fact_sales fs
JOIN warehouse.dim_products dp
    ON fs.product_key = dp.product_key
GROUP BY dp.category
ORDER BY total_revenue DESC;


-- =========================================================
-- Query 5: Payment Method Distribution
-- Objective: Understand customer payment preferences
-- =========================================================
SELECT
    pm.payment_method_name AS payment_method,
    COUNT(DISTINCT fs.transaction_id) AS transaction_count,
    SUM(fs.line_total) AS total_revenue,
    ROUND(
        COUNT(DISTINCT fs.transaction_id) * 100.0 /
        SUM(COUNT(DISTINCT fs.transaction_id)) OVER (), 2
    ) AS pct_of_transactions,
    ROUND(
        SUM(fs.line_total) * 100.0 /
        SUM(SUM(fs.line_total)) OVER (), 2
    ) AS pct_of_revenue
FROM warehouse.fact_sales fs
JOIN warehouse.dim_payment_method pm
    ON fs.payment_method_key = pm.payment_method_key
GROUP BY pm.payment_method_name;


-- =========================================================
-- Query 6: Geographic Analysis
-- Objective: Identify high-revenue locations
-- =========================================================
SELECT
    dc.state,
    SUM(fs.line_total) AS total_revenue,
    COUNT(DISTINCT fs.customer_key) AS total_customers,
    ROUND(SUM(fs.line_total) / COUNT(DISTINCT fs.customer_key), 2)
        AS avg_revenue_per_customer
FROM warehouse.fact_sales fs
JOIN warehouse.dim_customers dc
    ON fs.customer_key = dc.customer_key
GROUP BY dc.state
ORDER BY total_revenue DESC;


-- =========================================================
-- Query 7: Customer Lifetime Value (CLV)
-- Objective: Measure customer value and tenure
-- =========================================================
SELECT
    dc.customer_id,
    dc.full_name,
    SUM(fs.line_total) AS total_spent,
    COUNT(DISTINCT fs.transaction_id) AS transaction_count,
    CURRENT_DATE - dc.registration_date AS days_since_registration,
    ROUND(AVG(fs.line_total), 2) AS avg_order_value
FROM warehouse.fact_sales fs
JOIN warehouse.dim_customers dc
    ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_id, dc.full_name, dc.registration_date
ORDER BY total_spent DESC;


-- =========================================================
-- Query 8: Product Profitability Analysis
-- Objective: Identify most profitable products
-- =========================================================
SELECT
    dp.product_name,
    dp.category,
    SUM(fs.profit) AS total_profit,
    ROUND(SUM(fs.profit) / NULLIF(SUM(fs.line_total), 0), 2) AS profit_margin,
    SUM(fs.line_total) AS revenue,
    SUM(fs.quantity) AS units_sold
FROM warehouse.fact_sales fs
JOIN warehouse.dim_products dp
    ON fs.product_key = dp.product_key
GROUP BY dp.product_name, dp.category
ORDER BY total_profit DESC;


-- =========================================================
-- Query 9: Day of Week Sales Pattern
-- Objective: Identify temporal sales patterns
-- =========================================================
SELECT
    t.day_name,
    ROUND(AVG(t.daily_revenue), 2) AS avg_daily_revenue,
    ROUND(AVG(t.daily_transactions), 2) AS avg_daily_transactions,
    SUM(t.daily_revenue) AS total_revenue
FROM (
    SELECT
        dd.day_name,
        dd.date_key,
        SUM(fs.line_total) AS daily_revenue,
        COUNT(DISTINCT fs.transaction_id) AS daily_transactions
    FROM warehouse.fact_sales fs
    JOIN warehouse.dim_date dd
        ON fs.date_key = dd.date_key
    GROUP BY dd.day_name, dd.date_key
) t
GROUP BY t.day_name
ORDER BY total_revenue DESC;



-- =========================================================
-- Query 10: Discount Impact Analysis
-- Objective: Analyze discount effectiveness
-- =========================================================
SELECT
    CASE
        WHEN discount_amount = 0 THEN '0%'
        WHEN discount_amount <= 10 THEN '1-10%'
        WHEN discount_amount <= 25 THEN '11-25%'
        WHEN discount_amount <= 50 THEN '26-50%'
        ELSE '50%+'
    END AS discount_range,
    ROUND(AVG(discount_amount), 2) AS avg_discount_pct,
    SUM(quantity) AS total_quantity_sold,
    SUM(line_total) AS total_revenue,
    ROUND(AVG(line_total), 2) AS avg_line_total
FROM warehouse.fact_sales
GROUP BY discount_range
ORDER BY total_revenue DESC;
