"""
Demo Data Generator — mimics the  Brazilian E-Commerce schema.

Usage:
    python ingestion/generate_demo_data.py
    python ingestion/generate_demo_data.py --records 5000 --seed 99
"""

import argparse
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ── Constants ─────────────────────────────────────────────────────────────────

STATES = ["SP","RJ","MG","RS","PR","SC","BA","GO","PE","CE","PA","MA","MS","PB","ES","AM","RN","MT","AL","PI"]

STATE_CITIES = {
    "SP": ["sao paulo", "campinas", "guarulhos"],
    "RJ": ["rio de janeiro", "niteroi", "nova iguacu"],
    "MG": ["belo horizonte", "uberlandia", "contagem"],
    "RS": ["porto alegre", "caxias do sul", "pelotas"],
    "PR": ["curitiba", "londrina", "maringa"],
    "SC": ["florianopolis", "joinville", "blumenau"],
    "BA": ["salvador", "feira de santana", "ilheus"],
    "GO": ["goiania", "anapolis", "rio verde"],
    "PE": ["recife", "caruaru", "olinda"],
    "CE": ["fortaleza", "caucaia", "sobral"],
}
for s in STATES:
    if s not in STATE_CITIES:
        STATE_CITIES[s] = [f"cidade {i} {s.lower()}" for i in range(1, 4)]

CATEGORIES_PT = [
    "cama_mesa_banho", "beleza_saude", "esporte_lazer", "informatica_acessorios",
    "moveis_decoracao", "utilidades_domesticas", "relogios_presentes", "telefonia",
    "ferramentas_jardim", "automotivo", "brinquedos", "cool_stuff",
    "eletrodomesticos", "bebes", "fashion_bolsas_e_acessorios", "livros_tecnicos",
    "musica", "construcao_ferramentas_seguranca", "papelaria", "alimentos",
]

CATEGORIES_EN = [
    "bed_bath_table", "health_beauty", "sports_leisure", "computers_accessories",
    "furniture_decor", "housewares", "watches_gifts", "telephony",
    "garden_tools", "auto", "toys", "cool_stuff",
    "home_appliances", "baby", "fashion_bags_accessories", "books_technical",
    "music", "construction_tools_safety", "stationery", "food",
]

PAYMENT_TYPES   = ["credit_card", "boleto", "voucher", "debit_card"]
PAYMENT_WEIGHTS = [0.74, 0.19, 0.05, 0.02]

ORDER_STATUSES = ["delivered", "shipped", "canceled", "processing", "invoiced", "approved"]
ORDER_WEIGHTS  = [0.80, 0.08, 0.05, 0.03, 0.02, 0.02]

REVIEW_COMMENTS = {
    5: ["Produto excelente!", "Muito bom!", "Recomendo!", "Otima compra!"],
    4: ["Bom produto.", "Gostei bastante.", "Chegou antes do prazo."],
    3: ["Produto ok.", "Esperava mais.", "Entrega demorou um pouco."],
    2: ["Produto com defeito.", "Qualidade inferior.", "Decepcionante."],
    1: ["Pessimo!", "Nao recomendo.", "Produto chegou quebrado."],
}

START_DATE = datetime(2017, 1, 1)
END_DATE   = datetime(2018, 8, 31)


# ── Helpers ───────────────────────────────────────────────────────────────────

def uid() -> str:
    return uuid.uuid4().hex

def rand_date(start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def rand_state_city() -> tuple[str, str]:
    state = random.choices(STATES)[0]
    return state, random.choice(STATE_CITIES[state])


# ── Table Generators ──────────────────────────────────────────────────────────

def gen_customers(n: int) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        state, city = rand_state_city()
        rows.append({
            "customer_id":              uid(),
            "customer_unique_id":       uid(),
            "customer_zip_code_prefix": str(random.randint(10000, 99999)),
            "customer_city":            city,
            "customer_state":           state,
        })
    return pd.DataFrame(rows)


def gen_sellers(n: int) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        state, city = rand_state_city()
        rows.append({
            "seller_id":              uid(),
            "seller_zip_code_prefix": str(random.randint(10000, 99999)),
            "seller_city":            city,
            "seller_state":           state,
        })
    return pd.DataFrame(rows)


def gen_products(n: int) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        rows.append({
            "product_id":                 uid(),
            "product_category_name":      random.choice(CATEGORIES_PT),
            "product_name_lenght":        random.randint(20, 60),   # typo matches real schema
            "product_description_lenght": random.randint(100, 1000),
            "product_photos_qty":         random.randint(1, 6),
            "product_weight_g":           random.randint(100, 30000),
            "product_length_cm":          random.randint(10, 100),
            "product_height_cm":          random.randint(5, 50),
            "product_width_cm":           random.randint(10, 80),
        })
    return pd.DataFrame(rows)


def gen_orders(customer_ids: list, n: int) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        status    = random.choices(ORDER_STATUSES, weights=ORDER_WEIGHTS)[0]
        purchased = rand_date(START_DATE, END_DATE)
        approved  = purchased + timedelta(minutes=random.randint(5, 60)) if status != "processing" else None
        carrier   = approved  + timedelta(days=random.randint(1, 5))     if status in ("shipped", "delivered") else None
        delivered = carrier   + timedelta(days=random.randint(1, 15))    if status == "delivered" else None

        rows.append({
            "order_id":                      uid(),
            "customer_id":                   random.choice(customer_ids),
            "order_status":                  status,
            "order_purchase_timestamp":      purchased,
            "order_approved_at":             approved,
            "order_delivered_carrier_date":  carrier,
            "order_delivered_customer_date": delivered,
            "order_estimated_delivery_date": purchased + timedelta(days=random.randint(10, 40)),
        })
    return pd.DataFrame(rows)


def gen_order_items(orders: pd.DataFrame, product_ids: list, seller_ids: list) -> pd.DataFrame:
    rows = []
    for _, order in orders.iterrows():
        n_items = random.choices([1, 2, 3, 4, 5], weights=[0.60, 0.20, 0.10, 0.06, 0.04])[0]
        for item_id in range(1, n_items + 1):
            rows.append({
                "order_id":           order["order_id"],
                "order_item_id":      item_id,
                "product_id":         random.choice(product_ids),
                "seller_id":          random.choice(seller_ids),
                "shipping_limit_date": order["order_purchase_timestamp"] + timedelta(days=random.randint(3, 10)),
                "price":              round(min(max(np.random.lognormal(4.0, 0.8), 9.9), 5000.0), 2),
                "freight_value":      round(random.uniform(5, 80), 2),
            })
    return pd.DataFrame(rows)


def gen_order_payments(orders: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    totals = (items["price"] + items["freight_value"]).groupby(items["order_id"]).sum().to_dict()
    rows = []
    for _, order in orders.iterrows():
        total     = totals.get(order["order_id"], round(random.uniform(20, 500), 2))
        n_pay     = 1 if random.random() < 0.85 else 2
        remaining = total
        for seq in range(1, n_pay + 1):
            ptype = random.choices(PAYMENT_TYPES, weights=PAYMENT_WEIGHTS)[0]
            value = round(remaining if seq == n_pay else remaining * random.uniform(0.3, 0.7), 2)
            remaining -= value
            rows.append({
                "order_id":             order["order_id"],
                "payment_sequential":   seq,
                "payment_type":         ptype,
                "payment_installments": random.choices([1,2,3,6,10,12], weights=[0.30,0.15,0.15,0.20,0.10,0.10])[0] if ptype == "credit_card" else 1,
                "payment_value":        value,
            })
    return pd.DataFrame(rows)


def gen_order_reviews(orders: pd.DataFrame) -> pd.DataFrame:
    delivered = orders[orders["order_status"] == "delivered"].sample(frac=0.80)
    rows = []
    for _, order in delivered.iterrows():
        score   = random.choices([1,2,3,4,5], weights=[0.05,0.05,0.10,0.25,0.55])[0]
        created = order["order_delivered_customer_date"] + timedelta(days=random.randint(1, 10))
        rows.append({
            "review_id":               uid(),
            "order_id":                order["order_id"],
            "review_score":            score,
            "review_comment_title":    "",
            "review_comment_message":  random.choice(REVIEW_COMMENTS[score]),
            "review_creation_date":    created,
            "review_answer_timestamp": created + timedelta(hours=random.randint(1, 72)),
        })
    return pd.DataFrame(rows)


def gen_geolocation(customers: pd.DataFrame, sellers: pd.DataFrame) -> pd.DataFrame:
    zips = pd.concat([
        customers[["customer_zip_code_prefix","customer_city","customer_state"]].rename(
            columns={"customer_zip_code_prefix":"zip","customer_city":"city","customer_state":"state"}),
        sellers[["seller_zip_code_prefix","seller_city","seller_state"]].rename(
            columns={"seller_zip_code_prefix":"zip","seller_city":"city","seller_state":"state"}),
    ]).drop_duplicates("zip")

    rows = []
    for _, row in zips.iterrows():
        for _ in range(random.randint(1, 3)):
            rows.append({
                "geolocation_zip_code_prefix": row["zip"],
                "geolocation_lat":             round(random.uniform(-33.7, -1.4), 6),
                "geolocation_lng":             round(random.uniform(-73.9, -34.8), 6),
                "geolocation_city":            row["city"],
                "geolocation_state":           row["state"],
            })
    return pd.DataFrame(rows)


def gen_category_translation() -> pd.DataFrame:
    return pd.DataFrame({
        "product_category_name":         CATEGORIES_PT,
        "product_category_name_english": CATEGORIES_EN,
    })


# ── Main ──────────────────────────────────────────────────────────────────────

def generate(n_orders: int = 2000, seed: int = 42, output_dir: Path = Path("data/raw")) -> None:
    random.seed(seed)
    np.random.seed(seed)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating {n_orders:,} orders (seed={seed})...\n")

    customers = gen_customers(n_orders)
    sellers   = gen_sellers(max(50, n_orders // 10))
    products  = gen_products(max(100, n_orders // 4))
    orders    = gen_orders(customers["customer_id"].tolist(), n_orders)
    items     = gen_order_items(orders, products["product_id"].tolist(), sellers["seller_id"].tolist())
    payments  = gen_order_payments(orders, items)
    reviews   = gen_order_reviews(orders)
    geo       = gen_geolocation(customers, sellers)
    cat_trans = gen_category_translation()

    tables = {
        "customers_dataset":           customers,
        "sellers_dataset":             sellers,
        "products_dataset":            products,
        "orders_dataset":              orders,
        "order_items_dataset":         items,
        "order_payments_dataset":      payments,
        "order_reviews_dataset":       reviews,
        "geolocation_dataset":         geo,
        "product_category_name_translation": cat_trans,
    }

    print(f"{'Table':<45} {'Rows':>8}")
    print("-" * 55)
    for name, df in tables.items():
        df.to_csv(output_dir / f"{name}.csv", index=False)
        print(f"{name:<45} {len(df):>8,}")
    print("-" * 55)
    print(f"\nSaved {len(tables)} files to '{output_dir}/'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=2000)
    parser.add_argument("--seed",    type=int, default=42)
    parser.add_argument("--output",  type=str, default="data/raw")
    args = parser.parse_args()
    generate(n_orders=args.records, seed=args.seed, output_dir=Path(args.output))