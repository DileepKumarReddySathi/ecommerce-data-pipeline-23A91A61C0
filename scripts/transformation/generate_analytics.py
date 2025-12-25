import os
import time
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# --------------------------------------------------
# Paths
# --------------------------------------------------
SQL_FILE = "/app/sql/queries/analytical_queries.sql"
OUTPUT_DIR = "/app/data/processed/analytics"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --------------------------------------------------
# DB Connection
# --------------------------------------------------
db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    database=os.getenv("DB_NAME"),
)

engine = create_engine(db_url, future=True)

# --------------------------------------------------
# Helpers
# --------------------------------------------------
def execute_query(query_name, sql):
    start = time.time()
    df = pd.read_sql(sql, engine)
    elapsed = (time.time() - start) * 1000
    return df, round(elapsed, 2)


def export_to_csv(df, filename):
    df.to_csv(filename, index=False)


# --------------------------------------------------
# Main
# --------------------------------------------------
def generate_analytics():
    with open(SQL_FILE, "r") as f:
        sql_text = f.read()

    # 🔴 CORRECT SPLIT: split ONLY on semicolon
    queries = [
        q.strip() for q in sql_text.split(";")
        if q.strip().lower().startswith("select")
    ]

    summary = {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "queries_executed": 0,
        "query_results": {},
        "total_execution_time_seconds": 0
    }

    total_start = time.time()

    for i, query in enumerate(queries, start=1):
        query_name = f"query{i}"
        print(f"▶ Executing {query_name}...")

        df, exec_time = execute_query(query_name, query)

        output_file = f"{OUTPUT_DIR}/query{i}.csv"
        export_to_csv(df, output_file)

        summary["query_results"][query_name] = {
            "rows": len(df),
            "columns": len(df.columns),
            "execution_time_ms": exec_time
        }

        summary["queries_executed"] += 1

    summary["total_execution_time_seconds"] = round(
        time.time() - total_start, 2
    )

    with open(f"{OUTPUT_DIR}/analytics_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("✅ Analytics generation completed successfully")


# --------------------------------------------------
# Entry point
# --------------------------------------------------
if __name__ == "__main__":
    generate_analytics()
