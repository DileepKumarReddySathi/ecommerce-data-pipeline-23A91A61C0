-- ===============================
-- DATA QUALITY CHECK QUERIES
-- ===============================

-- 1. NULL CHECKS
SELECT 'customers.email' AS field, COUNT(*) 
FROM staging.customers WHERE email IS NULL;

SELECT 'products.price' AS field, COUNT(*) 
FROM staging.products WHERE price IS NULL;

-- 2. DUPLICATE CHECKS
SELECT email, COUNT(*) 
FROM staging.customers 
GROUP BY email HAVING COUNT(*) > 1;

-- 3. ORPHAN TRANSACTIONS
SELECT COUNT(*) 
FROM staging.transactions t
LEFT JOIN staging.customers c
ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- 4. ORPHAN TRANSACTION ITEMS (TRANSACTION)
SELECT COUNT(*) 
FROM staging.transaction_items ti
LEFT JOIN staging.transactions t
ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- 5. ORPHAN TRANSACTION ITEMS (PRODUCT)
SELECT COUNT(*) 
FROM staging.transaction_items ti
LEFT JOIN staging.products p
ON ti.product_id = p.product_id
WHERE p.product_id IS NULL;

-- 6. RANGE CHECKS
SELECT COUNT(*) FROM staging.products WHERE price <= 0;
SELECT COUNT(*) FROM staging.transaction_items WHERE quantity <= 0;
SELECT COUNT(*) FROM staging.transaction_items 
WHERE discount_percentage < 0 OR discount_percentage > 100;

-- 7. CONSISTENCY CHECK
SELECT COUNT(*) 
FROM staging.transaction_items
WHERE line_total <> ROUND(quantity * unit_price * (1 - discount_percentage/100), 2);
