-- ===============================
-- Query 1: Data Freshness
-- ===============================
SELECT
    MAX(created_at) AS latest_record,
    CURRENT_TIMESTAMP - MAX(created_at) AS lag
FROM production.transactions;

-- ===============================
-- Query 2: Volume Trend (30 days)
-- ===============================
SELECT
    transaction_date,
    COUNT(*) AS daily_transactions
FROM production.transactions
WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY transaction_date
ORDER BY transaction_date;

-- ===============================
-- Query 3: Data Quality (Orphans)
-- ===============================
SELECT COUNT(*) AS orphan_items
FROM production.transaction_items ti
LEFT JOIN production.transactions t
ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- ===============================
-- Query 4: Execution History
-- ===============================
-- Based on pipeline_execution_report.json
-- Used by Python monitoring logic

-- ===============================
-- Query 5: Database Statistics
-- ===============================
SELECT
    relname AS table_name,
    n_live_tup AS row_count
FROM pg_stat_user_tables;
