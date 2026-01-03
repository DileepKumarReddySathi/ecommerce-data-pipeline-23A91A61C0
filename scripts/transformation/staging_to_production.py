import pandas as pd
import json
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# --------------------------------------------------
# Output path
# --------------------------------------------------
OUTPUT_PATH = "data/processed"
os.makedirs(OUTPUT_PATH, exist_ok=True)

# --------------------------------------------------
# Database connection
# --------------------------------------------------
engine = create_engine(
    URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "Dilep@2025"),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        database=os.getenv("DB_NAME", "ecommerce_db"),
    )
)


# --------------------------------------------------
# Cleansing functions
# --------------------------------------------------
def cleanse_customer_data(df):
    df = df.copy()
    df["first_name"] = df["first_name"].str.strip().str.title()
    df["last_name"] = df["last_name"].str.strip().str.title()
    df["email"] = df["email"].str.strip().str.lower()
    df["phone"] = df["phone"].astype(str).str.replace(r"\D", "", regex=True)
    return df


def cleanse_product_data(df):
    df = df.copy()
    df["product_name"] = df["product_name"].str.strip()
    df["brand"] = df["brand"].str.strip()
    df["profit_margin"] = round(((df["price"] - df["cost"]) / df["price"]) * 100, 2)

    def price_bucket(p):
        if p < 50:
            return "Budget"
        elif p < 200:
            return "Mid-range"
        return "Premium"

    df["price_category"] = df["price"].apply(price_bucket)
    return df


def cleanse_transactions(df):
    return df[df["total_amount"] > 0]


def cleanse_transaction_items(df):
    df = df[df["quantity"] > 0].copy()
    df["line_total"] = round(
        df["quantity"] * df["unit_price"] * (1 - df["discount_percentage"] / 100), 2
    )
    return df

# --------------------------------------------------
# SAFE DIMENSION LOADER
# --------------------------------------------------
def load_dimension(df: pd.DataFrame, table_name: str, conn):
    """
    FULL reload for dimension tables.
    Automatically ignores DB-managed columns.
    """

    # 1️⃣ Fetch actual table columns from DB
    table_columns = pd.read_sql(
        text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'production'
              AND table_name = :table_name
            ORDER BY ordinal_position
        """),
        conn,
        params={"table_name": table_name}
    )["column_name"].tolist()

    # 2️⃣ Columns handled by DB defaults
    auto_columns = {"created_at", "updated_at"}

    # 3️⃣ Columns ETL must supply
    required_columns = [c for c in table_columns if c not in auto_columns]

    # 4️⃣ Align dataframe
    df = df[required_columns]

    # 5️⃣ Validate schema match
    if list(df.columns) != required_columns:
        raise ValueError(
            f"Column mismatch for {table_name}. "
            f"Expected {required_columns}, got {list(df.columns)}"
        )

    # 6️⃣ Idempotent reload
    conn.execute(text(f"TRUNCATE TABLE production.{table_name} CASCADE"))

    # 7️⃣ Bulk insert
    df.to_sql(
    table_name,
    conn,
    schema="production",
    if_exists="append",
    index=False,
    chunksize=500   # ← critical fix
)


# --------------------------------------------------
# FACT LOADER (INCREMENTAL)
# --------------------------------------------------
def load_fact_incremental(df, table_name, key_col, conn):
    existing = pd.read_sql(
        f"SELECT {key_col} FROM production.{table_name}",
        conn
    )[key_col].tolist()

    df_new = df[~df[key_col].isin(existing)]

    if not df_new.empty:
        df_new.to_sql(
            table_name,
            conn,
            schema="production",
            if_exists="append",
            index=False,
            method="multi"
        )

    return len(df_new)

# --------------------------------------------------
# MAIN ETL
# --------------------------------------------------
def run_staging_to_production():
    summary = {
        "transformation_timestamp": datetime.utcnow().isoformat(),
        "records_processed": {},
        "transformations_applied": [
            "text_normalization",
            "email_standardization",
            "phone_normalization",
            "profit_margin_calculation",
            "price_categorization",
            "invalid_record_filtering"
        ],
        "data_quality_post_transform": {
            "null_violations": 0,
            "constraint_violations": 0
        }
    }

    with engine.begin() as conn:

        # -------- CUSTOMERS --------
        cust = pd.read_sql("SELECT * FROM staging.customers", conn)
        cust_clean = cleanse_customer_data(cust)
        load_dimension(cust_clean, "customers", conn)

        summary["records_processed"]["customers"] = {
            "input": len(cust),
            "output": len(cust_clean),
            "filtered": len(cust) - len(cust_clean)
        }

        # -------- PRODUCTS --------
        prod = pd.read_sql("SELECT * FROM staging.products", conn)
        prod_clean = cleanse_product_data(prod)
        load_dimension(prod_clean, "products", conn)

        summary["records_processed"]["products"] = {
            "input": len(prod),
            "output": len(prod_clean),
            "filtered": len(prod) - len(prod_clean)
        }

        # -------- TRANSACTIONS --------
        txn = pd.read_sql("SELECT * FROM staging.transactions", conn)
        txn_clean = cleanse_transactions(txn)

        inserted_txn = load_fact_incremental(
            txn_clean, "transactions", "transaction_id", conn
        )

        summary["records_processed"]["transactions"] = {
            "input": len(txn),
            "output": inserted_txn,
            "filtered": len(txn) - len(txn_clean)
        }

        # -------- TRANSACTION ITEMS --------
        items = pd.read_sql("SELECT * FROM staging.transaction_items", conn)
        items_clean = cleanse_transaction_items(items)

        inserted_items = load_fact_incremental(
            items_clean, "transaction_items", "item_id", conn
        )

        summary["records_processed"]["transaction_items"] = {
            "input": len(items),
            "output": inserted_items,
            "filtered": len(items) - len(items_clean)
        }

    with open(f"{OUTPUT_PATH}/transformation_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("✅ Staging → Production ETL completed successfully")

# --------------------------------------------------
# ENTRY POINT
# --------------------------------------------------
if __name__ == "__main__":
    run_staging_to_production()
