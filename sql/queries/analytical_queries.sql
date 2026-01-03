SELECT p.product_id, p.product_name, SUM(ti.line_total) AS revenue
FROM production.transaction_items ti
JOIN production.products p ON ti.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY revenue DESC
LIMIT 10;

SELECT DATE_TRUNC('month', t.transaction_date) AS month, SUM(ti.line_total) AS revenue
FROM production.transactions t
JOIN production.transaction_items ti ON t.transaction_id = ti.transaction_id
GROUP BY month
ORDER BY month;

SELECT c.customer_id,
       SUM(ti.line_total) AS total_spent,
       CASE
         WHEN SUM(ti.line_total) < 5000 THEN 'Low'
         WHEN SUM(ti.line_total) BETWEEN 5000 AND 15000 THEN 'Medium'
         ELSE 'High'
       END AS segment
FROM production.customers c
JOIN production.transactions t ON c.customer_id = t.customer_id
JOIN production.transaction_items ti ON t.transaction_id = ti.transaction_id
GROUP BY c.customer_id;

SELECT p.category, SUM(ti.line_total) AS revenue
FROM production.products p
JOIN production.transaction_items ti ON p.product_id = ti.product_id
GROUP BY p.category
ORDER BY revenue DESC;

SELECT payment_method, COUNT(*) AS transaction_count
FROM production.transactions
GROUP BY payment_method;

SELECT c.state, SUM(ti.line_total) AS revenue
FROM production.customers c
JOIN production.transactions t ON c.customer_id = t.customer_id
JOIN production.transaction_items ti ON t.transaction_id = ti.transaction_id
GROUP BY c.state;

SELECT c.customer_id, SUM(ti.line_total) AS lifetime_value
FROM production.customers c
JOIN production.transactions t ON c.customer_id = t.customer_id
JOIN production.transaction_items ti ON t.transaction_id = ti.transaction_id
GROUP BY c.customer_id
ORDER BY lifetime_value DESC
LIMIT 10;

SELECT p.product_id,
       p.product_name,
       SUM(ti.line_total) - SUM(ti.quantity * p.cost) AS profit
FROM production.products p
JOIN production.transaction_items ti ON p.product_id = ti.product_id
GROUP BY p.product_id, p.product_name
ORDER BY profit DESC;

SELECT EXTRACT(DOW FROM t.transaction_date) AS day_of_week,
       SUM(ti.line_total) AS revenue
FROM production.transactions t
JOIN production.transaction_items ti ON t.transaction_id = ti.transaction_id
GROUP BY day_of_week
ORDER BY day_of_week;

SELECT
  CASE
    WHEN discount_percentage = 0 THEN 'No Discount'
    WHEN discount_percentage <= 10 THEN '0–10%'
    WHEN discount_percentage <= 20 THEN '10–20%'
    ELSE '20%+'
  END AS discount_bucket,
  SUM(line_total) AS revenue
FROM production.transaction_items
GROUP BY discount_bucket;
