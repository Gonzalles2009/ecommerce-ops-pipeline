-- Analytical views on top of the warehouse tables.
-- These would be the data sources for Looker Studio / Metabase / any BI tool.

-- Monthly revenue summary
CREATE OR REPLACE VIEW v_monthly_revenue AS
SELECT
    order_month,
    count(*) AS orders,
    sum(revenue) AS revenue,
    sum(freight) AS freight,
    sum(total_value) AS total_value,
    avg(total_value) AS avg_order_value,
    avg(review_score) AS avg_review_score,
    avg(delivery_days) AS avg_delivery_days
FROM fact_orders
WHERE order_status = 'delivered'
GROUP BY order_month
ORDER BY order_month;

-- Revenue by customer state
CREATE OR REPLACE VIEW v_revenue_by_state AS
SELECT
    c.customer_state AS state,
    count(*) AS orders,
    sum(f.total_value) AS total_value,
    avg(f.review_score) AS avg_review_score,
    avg(f.delivery_days) AS avg_delivery_days
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
WHERE f.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY total_value DESC;

-- Top product categories
CREATE OR REPLACE VIEW v_top_categories AS
SELECT
    p.category,
    count(*) AS items_sold,
    sum(oi.price) AS revenue,
    avg(oi.price) AS avg_price
FROM order_items_detail oi
JOIN dim_products p ON oi.product_id = p.product_id
WHERE oi.order_status = 'delivered'
GROUP BY p.category
ORDER BY revenue DESC;

-- Seller performance
CREATE OR REPLACE VIEW v_seller_performance AS
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    count(DISTINCT oi.order_id) AS orders,
    sum(oi.price) AS revenue,
    avg(oi.price) AS avg_item_price
FROM order_items_detail oi
JOIN dim_sellers s ON oi.seller_id = s.seller_id
WHERE oi.order_status = 'delivered'
GROUP BY s.seller_id, s.seller_city, s.seller_state
ORDER BY revenue DESC;

-- Delivery performance
CREATE OR REPLACE VIEW v_delivery_performance AS
SELECT
    order_month,
    count(*) AS delivered_orders,
    avg(delivery_days) AS avg_delivery_days,
    avg(estimated_days) AS avg_estimated_days,
    avg(delivery_delta) AS avg_delta,
    sum(CASE WHEN delivered_late THEN 1 ELSE 0 END) AS late_orders,
    round(100.0 * sum(CASE WHEN delivered_late THEN 1 ELSE 0 END) / count(*), 1) AS late_pct
FROM fact_orders
WHERE order_status = 'delivered' AND delivery_days IS NOT NULL
GROUP BY order_month
ORDER BY order_month;

-- Payment method breakdown
CREATE OR REPLACE VIEW v_payment_methods AS
SELECT
    payment_type,
    count(*) AS orders,
    sum(payment_value) AS total_paid,
    avg(payment_installments) AS avg_installments
FROM fact_orders
WHERE order_status = 'delivered'
GROUP BY payment_type
ORDER BY total_paid DESC;

-- Order status funnel
CREATE OR REPLACE VIEW v_order_funnel AS
SELECT
    order_status,
    count(*) AS orders,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM fact_orders
GROUP BY order_status
ORDER BY orders DESC;

-- Customer geography with coordinates (for heatmaps)
CREATE OR REPLACE VIEW v_customer_geo AS
SELECT
    c.customer_state AS state,
    g.lat,
    g.lng,
    count(*) AS orders,
    sum(f.total_value) AS total_value
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
JOIN dim_geography g ON c.customer_zip_code_prefix = g.zip_code_prefix
WHERE f.order_status = 'delivered'
GROUP BY c.customer_state, g.lat, g.lng;
