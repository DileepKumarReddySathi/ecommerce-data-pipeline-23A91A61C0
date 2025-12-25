# scripts/data_generation/generate_data.py
import csv
import random
import json
from faker import Faker
from datetime import datetime, timedelta
import os
import yaml

# ----------------------------
# Load config
# ----------------------------
with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

# Configurable parameters
NUM_CUSTOMERS = config.get("USER_RECORD_COUNT", 1000)
NUM_PRODUCTS = config.get("PRODUCT_RECORD_COUNT", 500)
NUM_TRANSACTIONS = config.get("ORDER_RECORD_COUNT", 10000)
START_DATE = datetime.strptime(config.get("TRANSACTION_START_DATE", "2023-01-01"), "%Y-%m-%d")
END_DATE = datetime.strptime(config.get("TRANSACTION_END_DATE", "2024-12-31"), "%Y-%m-%d")

faker = Faker()
output_dir = "data/raw"
os.makedirs(output_dir, exist_ok=True)

# ----------------------------
# Customers
# ----------------------------
customers = []
for i in range(1, NUM_CUSTOMERS + 1):
    customer_id = f"CUST{i:04d}"
    first_name = faker.first_name()
    last_name = faker.last_name()
    email = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"
    phone = faker.phone_number()
    registration_date = faker.date_between(start_date=START_DATE, end_date=END_DATE).strftime("%Y-%m-%d")
    city = faker.city()
    state = faker.state()
    country = faker.country()
    age_group = random.choice(["18-25", "26-35", "36-45", "46-60", "60+"])
    customers.append([customer_id, first_name, last_name, email, phone, registration_date, city, state, country, age_group])

with open(os.path.join(output_dir, "customers.csv"), "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["customer_id", "first_name", "last_name", "email", "phone", "registration_date", "city", "state", "country", "age_group"])
    writer.writerows(customers)

# ----------------------------
# Products
# ----------------------------
categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports", "Beauty"]
products = []

for i in range(1, NUM_PRODUCTS + 1):
    product_id = f"PROD{i:04d}"
    product_name = faker.word().capitalize() + " " + faker.word().capitalize()
    category = random.choice(categories)
    sub_category = faker.word().capitalize()
    price = round(random.uniform(10, 1000), 2)
    cost = round(random.uniform(5, price - 1), 2)  # ensure positive profit margin
    brand = faker.company()
    stock_quantity = random.randint(10, 500)
    supplier_id = f"SUPP{random.randint(1,100):03d}"
    products.append([product_id, product_name, category, sub_category, price, cost, brand, stock_quantity, supplier_id])

with open(os.path.join(output_dir, "products.csv"), "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["product_id", "product_name", "category", "sub_category", "price", "cost", "brand", "stock_quantity", "supplier_id"])
    writer.writerows(products)

# ----------------------------
# Transactions
# ----------------------------
transactions = []
transaction_items = []

for i in range(1, NUM_TRANSACTIONS + 1):
    transaction_id = f"TXN{i:05d}"
    customer = random.choice(customers)
    customer_id = customer[0]
    transaction_date = faker.date_between(start_date=START_DATE, end_date=END_DATE).strftime("%Y-%m-%d")
    transaction_time = faker.time()
    payment_method = random.choice(["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"])
    shipping_address = faker.address().replace("\n", ", ")
    
    # Items for this transaction
    num_items = random.randint(1, 5)
    total_amount = 0
    for j in range(num_items):
        item_id = f"ITEM{len(transaction_items)+1:05d}"
        product = random.choice(products)
        product_id = product[0]
        quantity = random.randint(1, 3)
        unit_price = product[4]
        discount_percentage = random.choice([0, 5, 10, 15])
        line_total = round(quantity * unit_price * (1 - discount_percentage/100), 2)
        total_amount += line_total
        transaction_items.append([item_id, transaction_id, product_id, quantity, unit_price, discount_percentage, line_total])
    
    total_amount = round(total_amount, 2)
    transactions.append([transaction_id, customer_id, transaction_date, transaction_time, payment_method, shipping_address, total_amount])

with open(os.path.join(output_dir, "transactions.csv"), "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["transaction_id", "customer_id", "transaction_date", "transaction_time", "payment_method", "shipping_address", "total_amount"])
    writer.writerows(transactions)

with open(os.path.join(output_dir, "transaction_items.csv"), "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["item_id", "transaction_id", "product_id", "quantity", "unit_price", "discount_percentage", "line_total"])
    writer.writerows(transaction_items)

# ----------------------------
# Metadata
# ----------------------------
metadata = {
    "generated_at": datetime.now().isoformat(),
    "num_customers": len(customers),
    "num_products": len(products),
    "num_transactions": len(transactions),
    "num_transaction_items": len(transaction_items),
    "transaction_date_range": [START_DATE.strftime("%Y-%m-%d"), END_DATE.strftime("%Y-%m-%d")]
}

with open(os.path.join(output_dir, "generation_metadata.json"), "w") as f:
    json.dump(metadata, f, indent=4)

# ----------------------------
# Referential Integrity Validation
# ----------------------------
def validate_referential_integrity():
    orphan_transactions = [t for t in transactions if t[1] not in {c[0] for c in customers}]
    orphan_items = [item for item in transaction_items if item[1] not in {t[0] for t in transactions} or item[2] not in {p[0] for p in products}]
    
    violations = len(orphan_transactions) + len(orphan_items)
    dq_score = 100 if violations == 0 else max(0, 100 - violations)
    
    return {
        "orphan_transactions": len(orphan_transactions),
        "orphan_items": len(orphan_items),
        "violations": violations,
        "data_quality_score": dq_score
    }
def generate_all_data():
    """
    Wrapper function for test and pipeline compatibility.
    Generates all required CSV files.
    """
    generate_customers()
    generate_products()
    generate_transactions()
    generate_transaction_items()


# Run validation
validation_results = validate_referential_integrity()
print("Referential Integrity Check:", validation_results)
