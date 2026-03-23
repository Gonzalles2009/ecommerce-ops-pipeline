"""Pipeline: orchestrate extract → transform → load."""

import time

from src.extract import read_all
from src.transform import (
    build_dim_customers,
    build_dim_geography,
    build_dim_products,
    build_dim_sellers,
    build_fact_orders,
    build_order_items_detail,
)
from src.load import load_all


def run() -> None:
    t0 = time.time()
    print("=" * 50)
    print("E-COMMERCE OPS — ETL PIPELINE")
    print("=" * 50)

    # --- Extract ---
    print("\n[1/3] Extracting raw data...")
    raw = read_all()
    for name, df in raw.items():
        print(f"  {name}: {len(df):,} rows")

    # --- Transform ---
    print("\n[2/3] Transforming...")

    dim_products = build_dim_products(raw["products"], raw["category_translation"])
    print(f"  dim_products: {len(dim_products):,} rows, {dim_products['category'].nunique()} categories")

    dim_customers = build_dim_customers(raw["customers"])
    print(f"  dim_customers: {len(dim_customers):,} rows, {dim_customers['customer_state'].nunique()} states")

    dim_sellers = build_dim_sellers(raw["sellers"])
    print(f"  dim_sellers: {len(dim_sellers):,} rows")

    dim_geography = build_dim_geography(raw["geolocation"])
    print(f"  dim_geography: {len(dim_geography):,} rows (deduplicated from {len(raw['geolocation']):,})")

    fact_orders = build_fact_orders(
        raw["orders"], raw["order_items"], raw["order_payments"], raw["order_reviews"]
    )
    delivered = fact_orders["delivery_days"].notna()
    avg_delivery = fact_orders.loc[delivered, "delivery_days"].mean()
    late_pct = fact_orders.loc[delivered, "delivered_late"].mean() * 100
    print(f"  fact_orders: {len(fact_orders):,} rows, avg delivery {avg_delivery:.0f} days, {late_pct:.1f}% late")

    order_items = build_order_items_detail(raw["order_items"], raw["orders"])
    print(f"  order_items_detail: {len(order_items):,} rows")

    # --- Load ---
    print("\n[3/3] Loading into DuckDB warehouse...")
    tables = {
        "dim_products": dim_products,
        "dim_customers": dim_customers,
        "dim_sellers": dim_sellers,
        "dim_geography": dim_geography,
        "fact_orders": fact_orders,
        "order_items_detail": order_items,
    }
    counts = load_all(tables)
    for name, count in counts.items():
        print(f"  {name}: {count:,} rows loaded")

    elapsed = time.time() - t0
    total_rows = sum(counts.values())
    print(f"\nDone. {total_rows:,} total rows in {elapsed:.1f}s")
    print("Warehouse: data/warehouse.duckdb")


if __name__ == "__main__":
    run()
