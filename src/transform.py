"""Transform: clean, normalize, build dimensional model."""

import pandas as pd


def build_dim_products(
    products: pd.DataFrame, translation: pd.DataFrame
) -> pd.DataFrame:
    df = products.merge(translation, on="product_category_name", how="left")
    df["category"] = (
        df["product_category_name_english"]
        .fillna(df["product_category_name"])
        .fillna("unknown")
        .str.replace("_", " ")
        .str.title()
    )
    return df[
        [
            "product_id",
            "category",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ]
    ].copy()


def build_dim_customers(customers: pd.DataFrame) -> pd.DataFrame:
    df = customers.copy()
    df["customer_city"] = df["customer_city"].str.title()
    return df[
        [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ]
    ]


def build_dim_sellers(sellers: pd.DataFrame) -> pd.DataFrame:
    df = sellers.copy()
    df["seller_city"] = df["seller_city"].str.title()
    return df[
        [
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        ]
    ]


def build_dim_geography(geolocation: pd.DataFrame) -> pd.DataFrame:
    """One row per zip code — median lat/lng to remove duplicates."""
    df = geolocation.copy()
    df["geolocation_city"] = df["geolocation_city"].str.title()
    return (
        df.groupby("geolocation_zip_code_prefix", as_index=False)
        .agg(
            lat=("geolocation_lat", "median"),
            lng=("geolocation_lng", "median"),
            city=("geolocation_city", "first"),
            state=("geolocation_state", "first"),
        )
        .rename(columns={"geolocation_zip_code_prefix": "zip_code_prefix"})
    )


def build_fact_orders(
    orders: pd.DataFrame,
    items: pd.DataFrame,
    payments: pd.DataFrame,
    reviews: pd.DataFrame,
) -> pd.DataFrame:
    """One row per order with aggregated items, payment, and review."""
    order_revenue = (
        items.groupby("order_id", as_index=False)
        .agg(
            total_items=("order_item_id", "max"),
            revenue=("price", "sum"),
            freight=("freight_value", "sum"),
        )
    )
    order_revenue["total_value"] = order_revenue["revenue"] + order_revenue["freight"]

    order_payment = (
        payments.groupby("order_id", as_index=False)
        .agg(
            payment_value=("payment_value", "sum"),
            payment_installments=("payment_installments", "max"),
            payment_type=("payment_type", "first"),
        )
    )

    order_review = (
        reviews.groupby("order_id", as_index=False)
        .agg(review_score=("review_score", "mean"))
    )

    df = orders.copy()
    df = df.merge(order_revenue, on="order_id", how="left")
    df = df.merge(order_payment, on="order_id", how="left")
    df = df.merge(order_review, on="order_id", how="left")

    # delivery metrics
    df["delivery_days"] = (
        df["order_delivered_customer_date"] - df["order_purchase_timestamp"]
    ).dt.days
    df["estimated_days"] = (
        df["order_estimated_delivery_date"] - df["order_purchase_timestamp"]
    ).dt.days
    df["delivery_delta"] = df["delivery_days"] - df["estimated_days"]
    df["delivered_late"] = df["delivery_delta"] > 0

    # time dimensions
    df["order_date"] = df["order_purchase_timestamp"].dt.date
    df["order_month"] = df["order_purchase_timestamp"].dt.to_period("M").astype(str)
    df["order_dow"] = df["order_purchase_timestamp"].dt.day_name()
    df["order_hour"] = df["order_purchase_timestamp"].dt.hour

    return df


def build_order_items_detail(
    items: pd.DataFrame, orders: pd.DataFrame
) -> pd.DataFrame:
    """One row per order item — for product/seller level analysis."""
    df = items.merge(
        orders[["order_id", "customer_id", "order_purchase_timestamp", "order_status"]],
        on="order_id",
        how="left",
    )
    df["order_month"] = df["order_purchase_timestamp"].dt.to_period("M").astype(str)
    df["item_value"] = df["price"] + df["freight_value"]
    return df
