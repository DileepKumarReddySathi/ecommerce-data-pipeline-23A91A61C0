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
# Helper: get table columns
# --------------------------------------------------
def get_table_columns(table_name, conn):
    return pd.read_sql(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'production'
          AND table_name = '{table_name}'
        """,
        conn,
    )["column_name"].tolist()


# --------------------------------------------------
# Dimension loader (SCHEMA-SAFE)
# --------------------------------------------------
def load_dimension(df, table_name, conn):
    """
    Full reload dimension tables.
    Automatically drops columns not present in target table.
    """

    # Enforce critical NOT NULLs
    if table_name == "customers":
        df = df.dropna(subset=["customer_id", "email"])

    if table_name == "products":
        df["product_name"] = df["product_name"].fillna("UNKNOWN_PRODUCT")
        df = df.dropna(subset=["product_id", "product_name", "price", "cost"])

    # Get actual DB columns
    target_cols = get_table_columns(table_name, conn)

    # Drop extra columns (like loaded_at)
    df = df[[c for c in df.columns if c in target_cols]]

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
# Fact loader (INCREMENTAL + loaded_at)
# --------------------------------------------------
def load_fact_incremental(df, table_name, conn, pk):
    """
    Incremental load for fact tables.
    Adds loaded_at only if column exists.
    """

    target_cols = get_table_columns(table_name, conn)

    if "loaded_at" in target_cols:
        df["loaded_at"] = datetime.now(timezone.utc)

    # Remove rows already loaded
    existing_ids = pd.read_sql(
        f"SELECT {pk} FROM production.{table_name}", conn
    )[pk].tolist()

    df_new = df[~df[pk].isin(existing_ids)]

    if df_new.empty:
        return 0

    # Drop non-existing columns
    df_new = df_new[[c for c in df_new.columns if c in target_cols]]

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

        # Customers
        cust = pd.read_sql("SELECT * FROM staging.customers", conn)
        load_dimension(cust, "customers", conn)

        # Products
        prod = pd.read_sql("SELECT * FROM staging.products", conn)
        load_dimension(prod, "products", conn)

        # Transactions (fact)
        txn = pd.read_sql("SELECT * FROM staging.transactions", conn)
        load_fact_incremental(txn, "transactions", conn, "transaction_id")

        # Transaction items (fact)
        items = pd.read_sql("SELECT * FROM staging.transaction_items", conn)
        load_fact_incremental(items, "transaction_items", conn, "item_id")


if __name__ == "__main__":
    run_staging_to_production()
    print("✅ Staging → Production transformation completed successfully")
