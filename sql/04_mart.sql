USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_ETL;
USE DATABASE OLIST_DW;

-- =========================
-- DIMENSIONS
-- =========================

CREATE OR REPLACE TABLE MART.DIM_CUSTOMER AS
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM STG.CUSTOMERS;

CREATE OR REPLACE TABLE MART.DIM_SELLER AS
SELECT
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state
FROM STG.SELLERS;

CREATE OR REPLACE TABLE MART.DIM_PRODUCT AS
SELECT
  p.product_id,
  p.product_category_name,
  COALESCE(t.product_category_name_english, p.product_category_name) AS category_english,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm
FROM STG.PRODUCTS p
LEFT JOIN STG.CATEGORY_TRANSLATION t
  ON p.product_category_name = t.product_category_name;


CREATE OR REPLACE TABLE MART.DIM_GEOLOCATION_ZIP AS
SELECT
  geolocation_zip_code_prefix AS zip_code_prefix,
  AVG(geolocation_lat) AS avg_lat,
  AVG(geolocation_lng) AS avg_lng,
  MAX(geolocation_city) AS city,
  MAX(geolocation_state) AS state
FROM STG.GEOLOCATION
GROUP BY 1;

-- =========================
-- FACT TABLES
-- =========================

-- Order-level fact: one row per order
CREATE OR REPLACE TABLE MART.FCT_ORDERS AS
WITH item_totals AS (
  SELECT
    order_id,
    SUM(price) AS items_value,
    SUM(freight_value) AS freight_value,
    SUM(price + freight_value) AS order_total_value
  FROM STG.ORDER_ITEMS
  GROUP BY order_id
),
pay_totals AS (
  SELECT
    order_id,
    SUM(payment_value) AS payment_total_value
  FROM STG.ORDER_PAYMENTS
  GROUP BY order_id
),
review_avg AS (
  SELECT
    order_id,
    AVG(review_score) AS avg_review_score
  FROM STG.ORDER_REVIEWS
  GROUP BY order_id
)
SELECT
  o.order_id,
  o.customer_id,
  o.order_status,
  o.purchase_ts,
  o.approved_ts,
  o.delivered_carrier_ts,
  o.delivered_customer_ts,
  o.estimated_delivery_ts,

  -- delivery metrics
  DATEDIFF('day', o.purchase_ts::date, o.delivered_customer_ts::date) AS delivery_days,
  IFF(o.delivered_customer_ts IS NOT NULL AND o.estimated_delivery_ts IS NOT NULL
      AND o.delivered_customer_ts > o.estimated_delivery_ts, 1, 0) AS late_delivery_flag,

  -- money metrics
  it.items_value,
  it.freight_value,
  it.order_total_value,
  pt.payment_total_value,

  -- customer satisfaction
  rv.avg_review_score

FROM STG.ORDERS o
LEFT JOIN item_totals it ON o.order_id = it.order_id
LEFT JOIN pay_totals pt ON o.order_id = pt.order_id
LEFT JOIN review_avg rv ON o.order_id = rv.order_id;

-- Item-level fact: one row per order item
CREATE OR REPLACE TABLE MART.FCT_ORDER_ITEMS AS
SELECT
  oi.order_id,
  oi.order_item_id,
  oi.product_id,
  oi.seller_id,
  oi.shipping_limit_ts,
  oi.price,
  oi.freight_value
FROM STG.ORDER_ITEMS oi;


SELECT 'dim_customer' t, COUNT(*) c FROM MART.DIM_CUSTOMER
UNION ALL SELECT 'dim_seller', COUNT(*) FROM MART.DIM_SELLER
UNION ALL SELECT 'dim_product', COUNT(*) FROM MART.DIM_PRODUCT
UNION ALL SELECT 'dim_geo_zip', COUNT(*) FROM MART.DIM_GEOLOCATION_ZIP
UNION ALL SELECT 'fct_orders', COUNT(*) FROM MART.FCT_ORDERS
UNION ALL SELECT 'fct_order_items', COUNT(*) FROM MART.FCT_ORDER_ITEMS;
