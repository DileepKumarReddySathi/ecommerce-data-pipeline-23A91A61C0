import pandas as pd
import os
import re
from scripts.data_generation.generate_data import generate_all_data


DATA_DIR = "data/raw"

def test_csv_files_exist():
    for f in ["customers.csv", "products.csv", "transactions.csv", "transaction_items.csv"]:
        assert os.path.exists(f"{DATA_DIR}/{f}")

def test_customers_schema():
    df = pd.read_csv(f"{DATA_DIR}/customers.csv")
    required = ["customer_id", "email", "registration_date"]
    for col in required:
        assert col in df.columns
    assert df["customer_id"].isnull().sum() == 0

def test_email_format():
    df = pd.read_csv(f"{DATA_DIR}/customers.csv")
    pattern = re.compile(r".+@.+\..+")
    assert df["email"].apply(lambda x: bool(pattern.match(x))).all()

def test_referential_integrity():
    cust = pd.read_csv(f"{DATA_DIR}/customers.csv")
    txn = pd.read_csv(f"{DATA_DIR}/transactions.csv")
    assert txn["customer_id"].isin(cust["customer_id"]).all()

def test_line_total_calculation():
    generate_all_data() 
    items = pd.read_csv(f"{DATA_DIR}/transaction_items.csv")

    calc = (
        items["quantity"]
        * items["unit_price"]
        * (1 - items["discount_percentage"] / 100)
    ).round(2)

    assert (items["line_total"].round(2) == calc).all()

