import os
import time
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL





# --------------------------------------------------
# Resolve project root
# --------------------------------------------------
BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
)

SQL_FILE = os.path.join(BASE_DIR, "sql", "queries", "analytical_queries.sql")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "processed", "analytics")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --------------------------------------------------
# DB Connection
# --------------------------------------------------
db_url = URL.create(
    drivername="postgresql+psycopg2",
    username="postgres",
    password="Dileep@2025",
    host="localhost",
    port=5433,
    database="ecommerce_db",
)

engine = create_engine(db_url)

with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM production.transaction_items"))
    print("PYTHON SEES ROWS:", result.scalar())

# --------------------------------------------------
# Helpers
# --------------------------------------------------
def execute_query(conn, sql):
    start = time.time()
    df = pd.read_sql_query(text(sql), conn)
    elapsed = round((time.time() - start) * 1000, 2)
    return df, elapsed

# --------------------------------------------------
# Main
# --------------------------------------------------
def generate_analytics():

    with open(SQL_FILE, "r", encoding="utf-8") as f:
        sql_text = f.read()

    # Split SQL file into individual SELECTs
    queries = [
        q.strip()
        for q in sql_text.split(";")
        if q.strip().lower().startswith(("select", "with"))
    ]

    if len(queries) != 10:
        raise ValueError(f"Expected 10 queries, found {len(queries)}")

    output_files = [
        "query1_top_products.csv",
        "query2_monthly_trend.csv",
        "query3_customer_segmentation.csv",
        "query4_category_performance.csv",
        "query5_payment_distribution.csv",
        "query6_geographic_analysis.csv",
        "query7_customer_lifetime_value.csv",
        "query8_product_profitability.csv",
        "query9_day_of_week_pattern.csv",
        "query10_discount_impact.csv",
    ]

    summary = {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "queries_executed": 0,
        "query_results": {},
        "total_execution_time_seconds": 0
    }

    total_start = time.time()

    with engine.connect() as conn:
        for i, sql in enumerate(queries):
            print(f"▶ Executing query {i+1}...")
            df, exec_time = execute_query(conn, sql)

            output_path = os.path.join(OUTPUT_DIR, output_files[i])
            df.to_csv(output_path, index=False)

            summary["query_results"][f"query{i+1}"] = {
                "rows": len(df),
                "columns": len(df.columns),
                "execution_time_ms": exec_time
            }

            summary["queries_executed"] += 1

    summary["total_execution_time_seconds"] = round(
        time.time() - total_start, 2
    )

    with open(os.path.join(OUTPUT_DIR, "analytics_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print("✅ Analytics generation completed successfully")

# --------------------------------------------------
# Entry point
# --------------------------------------------------
if __name__ == "__main__":
    generate_analytics()
