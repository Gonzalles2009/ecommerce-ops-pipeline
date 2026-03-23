"""Extract: read raw CSVs into DataFrames with minimal type coercion."""

from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

TABLES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

DATE_COLUMNS = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "order_reviews": ["review_creation_date", "review_answer_timestamp"],
}


def read_table(name: str) -> pd.DataFrame:
    path = RAW_DIR / TABLES[name]
    df = pd.read_csv(path)
    for col in DATE_COLUMNS.get(name, []):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def read_all() -> dict[str, pd.DataFrame]:
    return {name: read_table(name) for name in TABLES}
