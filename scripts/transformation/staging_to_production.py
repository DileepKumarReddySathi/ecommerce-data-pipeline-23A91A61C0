# scripts/transformation/staging_to_production.py

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from datetime import datetime, timezone
import os

# --------------------------------------------------
# DB connection
# --------------------------------------------------
db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT", "5432")),
    database=os.getenv("DB_NAME"),
)

engine = create_engine(db_url, future=True)


# --------------------------------------------------
# Dimension loader (SAFE)
# --------------------------------------------------
def load_dimension(df, table_name, conn):
    """
    Full reload dimension tables safely.
    Enforces NOT NULL constraints explicitly.
    """

    # ---------- CRITICAL FIX ----------
    if table_name == "products":
        df["product_name"] = df["product_name"].fillna("UNKNOWN_PRODUCT")
        df["category"] = df["category"].fillna("UNKNOWN")
        df["sub_category"] = df["sub_category"].fillna("UNKNOWN")

        # Drop rows that still violate core constraints
        df = df.dropna(subset=["product_id", "product_name", "price", "cost"])

    if table_name == "customers":
        df = df.dropna(subset=["customer_id", "email"])

    # Idempotent reload
    conn.execute(text(f"TRUNCATE TABLE production.{table_name} CASCADE;"))

    df.to_sql(
        name=table_name,
        schema="production",
        con=conn,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )


# --------------------------------------------------
# Incremental fact loader
# --------------------------------------------------
def load_fact_incremental(df, table_name, conn, pk):
    """
    Incremental load for fact tables.
    Safe against schema drift.
    """

    # Detect columns in target table
    existing_cols = pd.read_sql(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'production'
          AND table_name = '{table_name}'
        """,
        conn,
    )["column_name"].tolist()

    # Add loaded_at ONLY if column exists
    if "loaded_at" in existing_cols:
        from datetime import datetime, timezone
        df["loaded_at"] = datetime.now(timezone.utc)

    # Incremental logic
    existing_ids = pd.read_sql(
        f"SELECT {pk} FROM production.{table_name}", conn
    )[pk].tolist()

    df_new = df[~df[pk].isin(existing_ids)]

    if df_new.empty:
        return 0

    # Drop any extra columns not present in DB
    df_new = df_new[[c for c in df_new.columns if c in existing_cols]]

    df_new.to_sql(
        name=table_name,
        schema="production",
        con=conn,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    return len(df_new)


# --------------------------------------------------
# Main pipeline
# --------------------------------------------------
def run_staging_to_production():
    with engine.begin() as conn:

        # ----------------------------
        # Customers
        # ----------------------------
        cust = pd.read_sql("SELECT * FROM staging.customers", conn)
        load_dimension(cust, "customers", conn)

        # ----------------------------
        # Products (FIXED)
        # ----------------------------
        prod = pd.read_sql("SELECT * FROM staging.products", conn)
        load_dimension(prod, "products", conn)

        # ----------------------------
        # Transactions (fact)
        # ----------------------------
        txn = pd.read_sql("SELECT * FROM staging.transactions", conn)
        load_fact_incremental(txn, "transactions", conn, "transaction_id")

        # ----------------------------
        # Transaction items (fact)
        # ----------------------------
        items = pd.read_sql("SELECT * FROM staging.transaction_items", conn)
        load_fact_incremental(items, "transaction_items", conn, "item_id")


if __name__ == "__main__":
    run_staging_to_production()
    print("✅ Staging → Production transformation completed successfully")
