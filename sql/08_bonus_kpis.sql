USE WAREHOUSE WH_BI;
USE DATABASE OLIST_DW;

-- Repeat customer rate (by month)
CREATE OR REPLACE VIEW MART.V_REPEAT_CUSTOMERS_MONTHLY AS
WITH cust_orders AS (
  SELECT
    customer_id,
    DATE_TRUNC('month', purchase_ts) AS month,
    COUNT(*) AS orders_in_month
  FROM MART.FCT_ORDERS
  WHERE purchase_ts IS NOT NULL
  GROUP BY 1,2
)
SELECT
  month,
  COUNT(DISTINCT customer_id) AS active_customers,
  COUNT(DISTINCT CASE WHEN orders_in_month >= 2 THEN customer_id END) AS repeat_customers,
  (COUNT(DISTINCT CASE WHEN orders_in_month >= 2 THEN customer_id END) / NULLIF(COUNT(DISTINCT customer_id),0)) AS repeat_customer_rate
FROM cust_orders
GROUP BY 1
ORDER BY 1;

-- Late delivery impact on review score
CREATE OR REPLACE VIEW MART.V_LATE_DELIVERY_REVIEW_IMPACT AS
SELECT
  late_delivery_flag,
  COUNT(*) AS orders,
  AVG(avg_review_score) AS avg_review_score,
  AVG(delivery_days) AS avg_delivery_days
FROM MART.FCT_ORDERS
WHERE avg_review_score IS NOT NULL
GROUP BY 1
ORDER BY 1;


SELECT COUNT(*) FROM OLIST_DW.MART.FCT_ORDERS;

-- Quick checks
SELECT * FROM MART.V_REPEAT_CUSTOMERS_MONTHLY LIMIT 12;
SELECT * FROM MART.V_LATE_DELIVERY_REVIEW_IMPACT;
