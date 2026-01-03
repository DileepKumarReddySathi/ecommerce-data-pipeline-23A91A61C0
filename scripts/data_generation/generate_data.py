import csv
import random
import json
import os
import yaml
import pandas as pd
from faker import Faker
from datetime import datetime

# ----------------------------
# Load config
# ----------------------------
with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

NUM_CUSTOMERS = config.get("USER_RECORD_COUNT", 1000)
NUM_PRODUCTS = config.get("PRODUCT_RECORD_COUNT", 500)
NUM_TRANSACTIONS = config.get("ORDER_RECORD_COUNT", 10000)

START_DATE = datetime.strptime(
    config.get("TRANSACTION_START_DATE", "2023-01-01"), "%Y-%m-%d"
)
END_DATE = datetime.strptime(
    config.get("TRANSACTION_END_DATE", "2024-12-31"), "%Y-%m-%d"
)

faker = Faker()

OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def generate_all_data():
    # ----------------------------
    # Customers
    # ----------------------------
    customers = []

    for i in range(1, NUM_CUSTOMERS + 1):
        customers.append([
            f"CUST{i:04d}",
            faker.first_name(),
            faker.last_name(),
            f"user{i}@example.com",
            faker.phone_number(),
            faker.date_between(start_date=START_DATE, end_date=END_DATE).strftime("%Y-%m-%d"),
            faker.city(),
            faker.state(),
            faker.country(),
            random.choice(["18-25", "26-35", "36-45", "46-60", "60+"]),
        ])

    with open(f"{OUTPUT_DIR}/customers.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "customer_id", "first_name", "last_name", "email",
            "phone", "registration_date", "city", "state",
            "country", "age_group"
        ])
        writer.writerows(customers)

    # ----------------------------
    # Products
    # ----------------------------
    products = []
    categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports", "Beauty"]

    for i in range(1, NUM_PRODUCTS + 1):
        price = round(random.uniform(10, 1000), 2)
        products.append([
            f"PROD{i:04d}",
            faker.word().capitalize(),
            random.choice(categories),
            faker.word().capitalize(),
            price,
            round(random.uniform(5, price - 1), 2),
            faker.company(),
            random.randint(10, 500),
            f"SUPP{random.randint(1,100):03d}",
        ])

    with open(f"{OUTPUT_DIR}/products.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "product_id", "product_name", "category", "sub_category",
            "price", "cost", "brand", "stock_quantity", "supplier_id"
        ])
        writer.writerows(products)

    # ----------------------------
    # Transactions & Items
    # ----------------------------
    transactions = []
    items = []

    for i in range(1, NUM_TRANSACTIONS + 1):
        txn_id = f"TXN{i:05d}"
        customer_id = random.choice(customers)[0]

        txn_total = 0.0

        for _ in range(random.randint(1, 5)):
            product = random.choice(products)
            quantity = random.randint(1, 3)
            unit_price = float(product[4])
            discount = random.choice([0, 5, 10, 15])

            raw_value = quantity * unit_price * (1 - discount / 100)

            # ✅ CRITICAL FIX — pandas rounding
            line_total = float(pd.Series([raw_value]).round(2).iloc[0])

            txn_total += line_total

            items.append([
                f"ITEM{len(items)+1:05d}",
                txn_id,
                product[0],
                quantity,
                unit_price,
                discount,
                line_total,
            ])

        transactions.append([
            txn_id,
            customer_id,
            faker.date_between(start_date=START_DATE, end_date=END_DATE).strftime("%Y-%m-%d"),
            faker.time(),
            random.choice(["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]),
            faker.address().replace("\n", ", "),
            float(pd.Series([txn_total]).round(2).iloc[0]),
        ])

    with open(f"{OUTPUT_DIR}/transactions.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "transaction_id", "customer_id", "transaction_date",
            "transaction_time", "payment_method",
            "shipping_address", "total_amount"
        ])
        writer.writerows(transactions)

    with open(f"{OUTPUT_DIR}/transaction_items.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "item_id", "transaction_id", "product_id",
            "quantity", "unit_price",
            "discount_percentage", "line_total"
        ])
        writer.writerows(items)

    with open(f"{OUTPUT_DIR}/generation_metadata.json", "w") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "num_customers": len(customers),
            "num_products": len(products),
            "num_transactions": len(transactions),
            "num_transaction_items": len(items),
        }, f, indent=4)


if __name__ == "__main__":
    generate_all_data()
    print("✅ Raw data generation completed successfully")
