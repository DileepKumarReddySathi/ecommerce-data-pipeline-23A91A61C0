import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import os
from dotenv import load_dotenv
load_dotenv()


# --------------------------------------------------
# Database connection
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
# BUILD DIM DATE
# --------------------------------------------------
def build_dim_date(start_date, end_date, conn):
    dates = pd.date_range(start=start_date, end=end_date)
    df = pd.DataFrame({"full_date": dates})

    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["full_date"].dt.year
    df["quarter"] = df["full_date"].dt.quarter
    df["month"] = df["full_date"].dt.month
    df["day"] = df["full_date"].dt.day
    df["month_name"] = df["full_date"].dt.month_name()
    df["day_name"] = df["full_date"].dt.day_name()
    df["week_of_year"] = df["full_date"].dt.isocalendar().week.astype(int)
    df["is_weekend"] = df["day_name"].isin(["Saturday", "Sunday"])

    conn.execute(text("TRUNCATE warehouse.dim_date CASCADE"))
    df.to_sql("dim_date", conn, schema="warehouse", if_exists="append", index=False)

# --------------------------------------------------
# BUILD DIM PAYMENT METHOD
# --------------------------------------------------
def build_dim_payment_method(conn):
    conn.execute(text("TRUNCATE warehouse.dim_payment_method CASCADE"))

    methods = [
        ("Credit Card", "Online"),
        ("Debit Card", "Online"),
        ("UPI", "Online"),
        ("Net Banking", "Online"),
        ("Cash on Delivery", "Offline"),
    ]

    for name, ptype in methods:
        conn.execute(
            text("""
                INSERT INTO warehouse.dim_payment_method
                (payment_method_name, payment_type)
                VALUES (:name, :ptype)
            """),
            {"name": name, "ptype": ptype},
        )

# --------------------------------------------------
# BUILD DIM CUSTOMERS (SCD TYPE 2 – FULL REFRESH)
# --------------------------------------------------
def build_dim_customers(conn):
    customers = pd.read_sql("SELECT * FROM production.customers", conn)
    conn.execute(text("TRUNCATE warehouse.dim_customers CASCADE"))

    customers["full_name"] = customers["first_name"] + " " + customers["last_name"]
    customers["effective_date"] = date.today()
    customers["end_date"] = None
    customers["is_current"] = True

    customers[[
        "customer_id", "full_name", "email", "city", "state",
        "country", "age_group", "registration_date",
        "effective_date", "end_date", "is_current"
    ]].to_sql(
        "dim_customers",
        conn,
        schema="warehouse",
        if_exists="append",
        index=False
    )

# --------------------------------------------------
# BUILD DIM PRODUCTS (✅ FIXED)
# --------------------------------------------------
def build_dim_products(conn):
    products = pd.read_sql(
        """
        SELECT
            product_id,
            product_name,
            category,
            sub_category,
            brand,
            price,
            cost
        FROM production.products
        """,
        conn
    )

    # 🔹 Derive price_category INSIDE warehouse
    def price_bucket(price):
        if price < 50:
            return "Budget"
        elif price < 200:
            return "Mid-range"
        return "Premium"

    products["price_category"] = products["price"].apply(price_bucket)

    products["effective_date"] = date.today()
    products["end_date"] = None
    products["is_current"] = True

    conn.execute(text("TRUNCATE warehouse.dim_products CASCADE"))

    products[[
        "product_id", "product_name", "category",
        "sub_category", "brand",
        "effective_date", "end_date", "is_current"
    ]].to_sql(
        "dim_products",
        conn,
        schema="warehouse",
        if_exists="append",
        index=False
    )

# --------------------------------------------------
# BUILD FACT SALES
# --------------------------------------------------
def build_fact_sales(conn):
    conn.execute(text("TRUNCATE warehouse.fact_sales CASCADE"))

    conn.execute(text("""
        INSERT INTO warehouse.fact_sales (
            date_key, customer_key, product_key, payment_method_key,
            transaction_id, quantity, unit_price,
            discount_amount, line_total, profit, created_at
        )
        SELECT
            dd.date_key,
            dc.customer_key,
            dp.product_key,
            pm.payment_method_key,
            ti.transaction_id,
            ti.quantity,
            ti.unit_price,
            ROUND(ti.unit_price * ti.quantity * (ti.discount_percentage / 100.0), 2),
            ti.line_total,
            ti.line_total - (p.cost * ti.quantity),
            CURRENT_TIMESTAMP
        FROM production.transaction_items ti
        JOIN production.transactions t ON ti.transaction_id = t.transaction_id
        JOIN production.products p ON ti.product_id = p.product_id
        JOIN warehouse.dim_customers dc
            ON t.customer_id = dc.customer_id AND dc.is_current = TRUE
        JOIN warehouse.dim_products dp
            ON p.product_id = dp.product_id AND dp.is_current = TRUE
        JOIN warehouse.dim_payment_method pm
            ON t.payment_method = pm.payment_method_name
        JOIN warehouse.dim_date dd
            ON t.transaction_date = dd.full_date
    """))

# --------------------------------------------------
# BUILD AGGREGATES
# --------------------------------------------------
def build_aggregates(conn):
    conn.execute(text("TRUNCATE warehouse.agg_daily_sales CASCADE"))
    conn.execute(text("""
        INSERT INTO warehouse.agg_daily_sales
        SELECT
            date_key,
            COUNT(DISTINCT transaction_id),
            SUM(line_total),
            SUM(profit),
            COUNT(DISTINCT customer_key)
        FROM warehouse.fact_sales
        GROUP BY date_key
    """))

    conn.execute(text("TRUNCATE warehouse.agg_product_performance CASCADE"))
    conn.execute(text("""
        INSERT INTO warehouse.agg_product_performance
        SELECT
            product_key,
            SUM(quantity),
            SUM(line_total),
            SUM(profit)
        FROM warehouse.fact_sales
        GROUP BY product_key
    """))

    conn.execute(text("TRUNCATE warehouse.agg_customer_metrics CASCADE"))
    conn.execute(text("""
        INSERT INTO warehouse.agg_customer_metrics
        SELECT
            customer_key,
            COUNT(DISTINCT transaction_id),
            SUM(line_total),
            AVG(line_total),
            MAX(created_at::date)
        FROM warehouse.fact_sales
        GROUP BY customer_key
    """))

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def run_warehouse_load():
    with engine.begin() as conn:
        build_dim_date("2024-01-01", "2024-12-31", conn)
        build_dim_payment_method(conn)
        build_dim_customers(conn)
        build_dim_products(conn)
        build_fact_sales(conn)
        build_aggregates(conn)

    print("✅ Warehouse load completed successfully")

if __name__ == "__main__":
    run_warehouse_load()
