import pandas as pd
import json
import time
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv()

DATA_PATH = "data/raw"
REPORT_PATH = "data/staging"
os.makedirs(REPORT_PATH, exist_ok=True)

db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT", "5433")),
    database=os.getenv("DB_NAME"),
)

print("▶ Starting ingestion")
engine = create_engine(db_url, future=True)
print("▶ Database connected")

TABLES = {
    "customers": "staging.customers",
    "products": "staging.products",
    "transactions": "staging.transactions",
    "transaction_items": "staging.transaction_items"
}

start_time = time.time()

summary = {
    "ingestion_timestamp": datetime.utcnow().isoformat(),
    "tables_loaded": {}
}

with engine.begin() as conn:
    for file, table in TABLES.items():
        try:
            file_path = f"{DATA_PATH}/{file}.csv"
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Missing input file: {file_path}")

            df = pd.read_csv(file_path)

            conn.execute(
                text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")
            )

            df.to_sql(
    name=table.split(".")[1],
    con=conn,
    schema="staging",
    if_exists="append",
    index=False,
    method="multi",
    chunksize=5000
)

            summary["tables_loaded"][table] = {
                "rows_loaded": len(df),
                "status": "success"
            }

            print(f"✅ Loaded {table} ({len(df)} rows)")

        except Exception as e:
            summary["tables_loaded"][table] = {
                "rows_loaded": 0,
                "status": "failed",
                "error_message": str(e)
            }
            raise

summary["total_execution_time_seconds"] = round(time.time() - start_time, 2)

with open(f"{REPORT_PATH}/ingestion_summary.json", "w") as f:
    json.dump(summary, f, indent=2)

print("🎉 Data ingestion to staging completed successfully")
