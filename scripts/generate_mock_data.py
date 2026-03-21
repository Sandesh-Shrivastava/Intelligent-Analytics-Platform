"""
Olist E-Commerce Data Generator
-------------------------------
This script generates a perfectly structurally-identical, statistically-realistic 
replica of the Brazilian Olist E-Commerce dataset using Faker and NumPy.

It builds 6 interconnected tables ensuring referential mathematical integrity:
- Customers
- Orders
- Products
- Order Items
- Payments
- Reviews

Usage:
    pip install faker pandas numpy
    python scripts/generate_mock_data.py
"""

import os
import random
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from faker import Faker

# Fast, reproducible random numbers
np.random.seed(42)
fake = Faker('pt_BR')
Faker.seed(42)

# Config
NUM_CUSTOMERS = 3000
NUM_PRODUCTS = 500
NUM_ORDERS = 5000
OUTPUT_DIR = "mock_data"

CATEGORIES = [
    'health_beauty', 'computers_accessories', 'auto', 'bed_bath_table',
    'furniture_decor', 'sports_leisure', 'perfumery', 'housewares',
    'telephony', 'watches_gifts', 'food_drink', 'baby', 'stationery'
]
STATES = ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'DF', 'ES', 'GO', 'PE', 'CE', 'PA']
STATE_WEIGHTS = [0.4, 0.15, 0.1, 0.08, 0.07, 0.05, 0.04, 0.03, 0.02, 0.02, 0.02, 0.01, 0.01]

def generate():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("🚀 Starting synthetic Olist data generation...")

    # 1. Customers
    print("⏳ Building Customers table...")
    customer_ids = [fake.uuid4() for _ in range(NUM_CUSTOMERS)]
    # Make some repeat customers by making unique customer IDs smaller than the total customer pool
    customer_unique_ids = [fake.uuid4() for _ in range(int(NUM_CUSTOMERS * 0.8))]
    
    customers = []
    for cid in customer_ids:
        state = np.random.choice(STATES, p=STATE_WEIGHTS)
        customers.append({
            "customer_id": cid,
            "customer_unique_id": random.choice(customer_unique_ids),
            "customer_zip_code_prefix": random.randint(1000, 99999),
            "customer_city": fake.city(),
            "customer_state": state
        })
    pd.DataFrame(customers).to_csv(f"{OUTPUT_DIR}/olist_customers_dataset.csv", index=False)

    # 2. Products
    print("⏳ Building Products table...")
    product_ids = [fake.uuid4() for _ in range(NUM_PRODUCTS)]
    products = []
    for pid in product_ids:
        products.append({
            "product_id": pid,
            "product_category_name": random.choice(CATEGORIES),
            "product_name_lenght": random.randint(30, 60),
            "product_description_lenght": random.randint(200, 2000),
            "product_photos_qty": random.randint(1, 5),
            "product_weight_g": random.randint(50, 5000),
            "product_length_cm": random.randint(15, 100),
            "product_height_cm": random.randint(5, 50),
            "product_width_cm": random.randint(10, 50)
        })
    pd.DataFrame(products).to_csv(f"{OUTPUT_DIR}/olist_products_dataset.csv", index=False)

    # 3. Orders
    print("⏳ Building Orders table...")
    order_ids = [fake.uuid4() for _ in range(NUM_ORDERS)]
    orders = []
    statuses = ['delivered', 'shipped', 'canceled', 'invoiced']
    status_weights = [0.95, 0.02, 0.02, 0.01]
    
    start_date = datetime(2017, 1, 1)
    
    for oid in order_ids:
        purchased_at = start_date + timedelta(days=random.randint(0, 600), minutes=random.randint(0, 1440))
        status = np.random.choice(statuses, p=status_weights)
        
        approved_at = purchased_at + timedelta(minutes=random.randint(5, 120))
        carrier_date = approved_at + timedelta(days=random.randint(1, 3))
        delivered_date = carrier_date + timedelta(days=random.randint(2, 10)) if status == 'delivered' else None
        estimated_date = purchased_at + timedelta(days=random.randint(10, 25))
        
        orders.append({
            "order_id": oid,
            "customer_id": random.choice(customer_ids),
            "order_status": status,
            "order_purchase_timestamp": purchased_at.strftime("%Y-%m-%d %H:%M:%S"),
            "order_approved_at": approved_at.strftime("%Y-%m-%d %H:%M:%S"),
            "order_delivered_carrier_date": carrier_date.strftime("%Y-%m-%d %H:%M:%S"),
            "order_delivered_customer_date": delivered_date.strftime("%Y-%m-%d %H:%M:%S") if delivered_date else "",
            "order_estimated_delivery_date": estimated_date.strftime("%Y-%m-%d %H:%M:%S")
        })
    pd.DataFrame(orders).to_csv(f"{OUTPUT_DIR}/olist_orders_dataset.csv", index=False)

    # 4. Order Items
    print("⏳ Building Order Items table...")
    prices = np.random.lognormal(mean=4.0, sigma=0.8, size=NUM_PRODUCTS)
    product_price_map = dict(zip(product_ids, prices))
    
    items = []
    for oid in order_ids:
        # random 1 to 4 items per order (mostly 1)
        num_items = np.random.choice([1, 2, 3, 4], p=[0.85, 0.10, 0.03, 0.02])
        for i in range(num_items):
            pid = random.choice(product_ids)
            items.append({
                "order_id": oid,
                "order_item_id": i + 1,
                "product_id": pid,
                "seller_id": fake.uuid4(),
                "shipping_limit_date": (start_date + timedelta(days=random.randint(0, 600))).strftime("%Y-%m-%d %H:%M:%S"),
                "price": round(product_price_map[pid], 2),
                "freight_value": round(random.uniform(5.0, 50.0), 2)
            })
    pd.DataFrame(items).to_csv(f"{OUTPUT_DIR}/olist_order_items_dataset.csv", index=False)

    # 5. Payments
    print("⏳ Building Payments table...")
    payments = []
    payment_types = ['credit_card', 'boleto', 'voucher', 'debit_card']
    for oid in order_ids:
        payments.append({
            "order_id": oid,
            "payment_sequential": 1,
            "payment_type": np.random.choice(payment_types, p=[0.75, 0.15, 0.05, 0.05]),
            "payment_installments": random.randint(1, 10),
            "payment_value": round(random.uniform(20.0, 500.0), 2)
        })
    pd.DataFrame(payments).to_csv(f"{OUTPUT_DIR}/olist_order_payments_dataset.csv", index=False)

    # 6. Reviews
    print("⏳ Building Reviews table...")
    reviews = []
    scores = [1, 2, 3, 4, 5]
    score_weights = [0.1, 0.05, 0.1, 0.2, 0.55] # mostly 5 stars like real Olist
    for oid in order_ids:
        score = np.random.choice(scores, p=score_weights)
        reviews.append({
            "review_id": fake.uuid4(),
            "order_id": oid,
            "review_score": score,
            "review_comment_title": fake.word() if random.random() > 0.8 else "",
            "review_comment_message": fake.sentence(nb_words=6) if random.random() > 0.6 else "",
            "review_creation_date": (start_date + timedelta(days=random.randint(10, 600))).strftime("%Y-%m-%d 00:00:00"),
            "review_answer_timestamp": (start_date + timedelta(days=random.randint(10, 600), hours=random.randint(1, 24))).strftime("%Y-%m-%d %H:%M:%S")
        })
    pd.DataFrame(reviews).to_csv(f"{OUTPUT_DIR}/olist_order_reviews_dataset.csv", index=False)

    print(f"✅ Success! Generated all files in the '{OUTPUT_DIR}' directory.")

if __name__ == "__main__":
    generate()
