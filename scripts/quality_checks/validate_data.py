import json
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
load_dotenv()


# ==================================================
# Configuration
# ==================================================
OUTPUT_PATH = "data/processed"
os.makedirs(OUTPUT_PATH, exist_ok=True)

DB_URL = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    database=os.getenv("DB_NAME"),
)

engine = create_engine(DB_URL, future=True)

# ==================================================
# Utility
# ==================================================
def scalar(query):
    with engine.connect() as conn:
        return conn.execute(text(query)).scalar()

# ==================================================
# 1. COMPLETENESS CHECKS
# ==================================================
def completeness_checks():
    checks = {
        "customers.email": scalar("SELECT COUNT(*) FROM staging.customers WHERE email IS NULL OR email = ''"),
        "products.price": scalar("SELECT COUNT(*) FROM staging.products WHERE price IS NULL"),
        "transactions.customer_id": scalar("SELECT COUNT(*) FROM staging.transactions WHERE customer_id IS NULL"),
    }
    total = sum(checks.values())
    return {
        "status": "passed" if total == 0 else "failed",
        "tables_checked": ["customers", "products", "transactions"],
        "null_violations": total,
        "details": checks,
    }, total

# ==================================================
# 2. UNIQUENESS CHECKS
# ==================================================
def uniqueness_checks():
    dup_emails = scalar("""
        SELECT COUNT(*) FROM (
            SELECT email FROM staging.customers
            GROUP BY email HAVING COUNT(*) > 1
        ) t
    """)

    dup_txns = scalar("""
        SELECT COUNT(*) FROM (
            SELECT customer_id, transaction_date, total_amount
            FROM staging.transactions
            GROUP BY customer_id, transaction_date, total_amount
            HAVING COUNT(*) > 1
        ) t
    """)

    total = dup_emails + dup_txns
    return {
        "status": "passed" if total == 0 else "failed",
        "duplicates_found": total,
        "details": {
            "duplicate_emails": dup_emails,
            "duplicate_transactions": dup_txns,
        },
    }, total

# ==================================================
# 3. VALIDITY & RANGE CHECKS
# ==================================================
def validity_checks():
    checks = {
        "price_positive": scalar("SELECT COUNT(*) FROM staging.products WHERE price <= 0"),
        "quantity_positive": scalar("SELECT COUNT(*) FROM staging.transaction_items WHERE quantity <= 0"),
        "discount_range": scalar("""
            SELECT COUNT(*) FROM staging.transaction_items
            WHERE discount_percentage < 0 OR discount_percentage > 100
        """),
        "cost_less_than_price": scalar("""
            SELECT COUNT(*) FROM staging.products WHERE cost >= price
        """),
    }
    total = sum(checks.values())
    return {
        "status": "passed" if total == 0 else "failed",
        "violations": total,
        "details": checks,
    }, total

# ==================================================
# 4. CONSISTENCY CHECKS
# ==================================================
def consistency_checks():
    mismatches = scalar("""
        SELECT COUNT(*) FROM staging.transaction_items
        WHERE line_total <> ROUND(quantity * unit_price * (1 - discount_percentage / 100), 2)
    """)
    return {
        "status": "passed" if mismatches == 0 else "failed",
        "mismatches": mismatches,
        "details": {
            "line_total_mismatch": mismatches
        },
    }, mismatches

# ==================================================
# 5. REFERENTIAL INTEGRITY (CRITICAL)
# ==================================================
def referential_integrity_checks():
    orphan_txns = scalar("""
        SELECT COUNT(*) FROM staging.transactions t
        LEFT JOIN staging.customers c ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)

    orphan_items_txn = scalar("""
        SELECT COUNT(*) FROM staging.transaction_items ti
        LEFT JOIN staging.transactions t ON ti.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
    """)

    orphan_items_prod = scalar("""
        SELECT COUNT(*) FROM staging.transaction_items ti
        LEFT JOIN staging.products p ON ti.product_id = p.product_id
        WHERE p.product_id IS NULL
    """)

    total = orphan_txns + orphan_items_txn + orphan_items_prod
    return {
        "status": "passed" if total == 0 else "failed",
        "orphan_records": total,
        "details": {
            "transactions_customers": orphan_txns,
            "items_transactions": orphan_items_txn,
            "items_products": orphan_items_prod,
        },
    }, total

# ==================================================
# 6. ACCURACY / BUSINESS RULES
# ==================================================
def accuracy_checks():
    future_txns = scalar("""
        SELECT COUNT(*) FROM staging.transactions
        WHERE transaction_date > CURRENT_DATE
    """)

    invalid_registration = scalar("""
        SELECT COUNT(*) FROM staging.transactions t
        JOIN staging.customers c ON t.customer_id = c.customer_id
        WHERE c.registration_date > t.transaction_date
    """)

    total = future_txns + invalid_registration
    return {
        "status": "passed" if total == 0 else "failed",
        "violations": total,
        "details": {
            "future_transactions": future_txns,
            "registration_after_transaction": invalid_registration,
        },
    }, total

# ==================================================
# WEIGHTED SCORING (CRITICAL PART)
# ==================================================
WEIGHTS = {
    "completeness": 0.15,
    "uniqueness": 0.10,
    "validity": 0.15,
    "consistency": 0.15,
    "referential": 0.30,   # MOST CRITICAL
    "accuracy": 0.15,
}

def dimension_score(violations, total_records):
    if total_records == 0:
        return 100
    score = max(0, (1 - (violations / total_records)) * 100)
    return round(score, 2)

def grade(score):
    if score >= 90: return "A"
    if score >= 80: return "B"
    if score >= 70: return "C"
    if score >= 60: return "D"
    return "F"

# ==================================================
# MAIN
# ==================================================
def run_quality_checks():
    checks = {}
    scores = {}

    checks["null_checks"], v1 = completeness_checks()
    checks["duplicate_checks"], v2 = uniqueness_checks()
    checks["range_checks"], v3 = validity_checks()
    checks["data_consistency"], v4 = consistency_checks()
    checks["referential_integrity"], v5 = referential_integrity_checks()
    checks["accuracy_checks"], v6 = accuracy_checks()

    total_rows = scalar("SELECT COUNT(*) FROM staging.transaction_items")

    scores["completeness"] = dimension_score(v1, total_rows)
    scores["uniqueness"] = dimension_score(v2, total_rows)
    scores["validity"] = dimension_score(v3, total_rows)
    scores["consistency"] = dimension_score(v4, total_rows)
    scores["referential"] = dimension_score(v5, total_rows)
    scores["accuracy"] = dimension_score(v6, total_rows)

    overall = sum(scores[k] * WEIGHTS[k] for k in WEIGHTS)
    overall = round(overall, 2)

    report = {
        "check_timestamp": datetime.utcnow().isoformat(),
        "checks_performed": checks,
        "overall_quality_score": overall,
        "quality_grade": grade(overall),
    }

    with open(f"{OUTPUT_PATH}/quality_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print(f"✅ Data quality checks completed | Score: {overall}% | Grade: {grade(overall)}")

if __name__ == "__main__":
    run_quality_checks()
