USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_ETL;
USE DATABASE OLIST_DW;

-- 1) Customers (clean text, remove duplicates)
CREATE OR REPLACE TABLE STG.CUSTOMERS AS
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  UPPER(TRIM(customer_city)) AS customer_city,
  TRIM(customer_state) AS customer_state
FROM RAW.CUSTOMERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) = 1;

-- 2) Orders (convert timestamps)
CREATE OR REPLACE TABLE STG.ORDERS AS
SELECT
  order_id,
  customer_id,
  order_status,
  TRY_TO_TIMESTAMP_NTZ(order_purchase_timestamp) AS purchase_ts,
  TRY_TO_TIMESTAMP_NTZ(order_approved_at) AS approved_ts,
  TRY_TO_TIMESTAMP_NTZ(order_delivered_carrier_date) AS delivered_carrier_ts,
  TRY_TO_TIMESTAMP_NTZ(order_delivered_customer_date) AS delivered_customer_ts,
  TRY_TO_TIMESTAMP_NTZ(order_estimated_delivery_date) AS estimated_delivery_ts
FROM RAW.ORDERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id) = 1;

-- 3) Order items (timestamp conversion)
CREATE OR REPLACE TABLE STG.ORDER_ITEMS AS
SELECT
  order_id,
  order_item_id,
  product_id,
  seller_id,
  TRY_TO_TIMESTAMP_NTZ(shipping_limit_date) AS shipping_limit_ts,
  price,
  freight_value
FROM RAW.ORDER_ITEMS;

-- 4) Payments
CREATE OR REPLACE TABLE STG.ORDER_PAYMENTS AS
SELECT
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value
FROM RAW.ORDER_PAYMENTS;

-- 5) Reviews (date + timestamp conversion)
CREATE OR REPLACE TABLE STG.ORDER_REVIEWS AS
SELECT
  review_id,
  order_id,
  review_score,
  review_comment_title,
  review_comment_message,
  TRY_TO_DATE(review_creation_date) AS review_creation_date,
  TRY_TO_TIMESTAMP_NTZ(review_answer_timestamp) AS review_answer_ts
FROM RAW.ORDER_REVIEWS;

-- 6) Products
CREATE OR REPLACE TABLE STG.PRODUCTS AS
SELECT
  product_id,
  product_category_name,
  product_name_lenght,
  product_description_lenght,
  product_photos_qty,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm
FROM RAW.PRODUCTS;

-- 7) Sellers (clean text, remove duplicates)
CREATE OR REPLACE TABLE STG.SELLERS AS
SELECT
  seller_id,
  seller_zip_code_prefix,
  UPPER(TRIM(seller_city)) AS seller_city,
  TRIM(seller_state) AS seller_state
FROM RAW.SELLERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY seller_id) = 1;

-- 8) Category translation
CREATE OR REPLACE TABLE STG.CATEGORY_TRANSLATION AS
SELECT
  product_category_name,
  product_category_name_english
FROM RAW.CATEGORY_TRANSLATION;

-- 9) Geolocation 
CREATE OR REPLACE TABLE STG.GEOLOCATION AS
SELECT
  geolocation_zip_code_prefix,
  geolocation_lat,
  geolocation_lng,
  UPPER(TRIM(geolocation_city)) AS geolocation_city,
  TRIM(geolocation_state) AS geolocation_state
FROM RAW.GEOLOCATION;


SELECT 'stg_customers' t, COUNT(*) c FROM STG.CUSTOMERS
UNION ALL SELECT 'stg_orders', COUNT(*) FROM STG.ORDERS
UNION ALL SELECT 'stg_items', COUNT(*) FROM STG.ORDER_ITEMS
UNION ALL SELECT 'stg_payments', COUNT(*) FROM STG.ORDER_PAYMENTS
UNION ALL SELECT 'stg_reviews', COUNT(*) FROM STG.ORDER_REVIEWS
UNION ALL SELECT 'stg_products', COUNT(*) FROM STG.PRODUCTS
UNION ALL SELECT 'stg_sellers', COUNT(*) FROM STG.SELLERS
UNION ALL SELECT 'stg_geo', COUNT(*) FROM STG.GEOLOCATION;
