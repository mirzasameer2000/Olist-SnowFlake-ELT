USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_BI;
USE DATABASE OLIST_DW;

-- 1) Monthly KPIs (orders, revenue, AOV, review, late delivery)
CREATE OR REPLACE VIEW MART.V_KPI_MONTHLY AS
SELECT
  DATE_TRUNC('month', purchase_ts) AS month,
  COUNT(DISTINCT order_id) AS orders,
  SUM(order_total_value) AS revenue,
  AVG(order_total_value) AS avg_order_value,
  AVG(avg_review_score) AS avg_review_score,
  AVG(late_delivery_flag) AS late_delivery_pct
FROM MART.FCT_ORDERS
WHERE purchase_ts IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- 2) Top product categories by revenue (top 20)
CREATE OR REPLACE VIEW MART.V_TOP_CATEGORIES AS
SELECT
  p.category_english,
  SUM(oi.price + oi.freight_value) AS revenue
FROM MART.FCT_ORDER_ITEMS oi
JOIN MART.DIM_PRODUCT p ON oi.product_id = p.product_id
GROUP BY 1
ORDER BY revenue DESC
LIMIT 20;

-- 3) Top customer states by revenue
CREATE OR REPLACE VIEW MART.V_TOP_CUSTOMER_STATES AS
SELECT
  c.customer_state,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(f.order_total_value) AS revenue,
  AVG(f.order_total_value) AS avg_order_value
FROM MART.FCT_ORDERS f
JOIN MART.DIM_CUSTOMER c ON f.customer_id = c.customer_id
GROUP BY 1
ORDER BY revenue DESC;

-- 4) Delivery performance (late delivery rate + avg delivery days) by month
CREATE OR REPLACE VIEW MART.V_DELIVERY_BY_MONTH AS
SELECT
  DATE_TRUNC('month', purchase_ts) AS month,
  AVG(delivery_days) AS avg_delivery_days,
  AVG(late_delivery_flag) AS late_delivery_pct
FROM MART.FCT_ORDERS
WHERE purchase_ts IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- 5) Payments summary by type
CREATE OR REPLACE VIEW MART.V_PAYMENT_TYPE_SUMMARY AS
SELECT
  payment_type,
  COUNT(*) AS payment_rows,
  SUM(payment_value) AS total_payment_value,
  AVG(payment_value) AS avg_payment_value
FROM STG.ORDER_PAYMENTS
GROUP BY 1
ORDER BY total_payment_value DESC;


SELECT * FROM MART.V_KPI_MONTHLY LIMIT 24;
SELECT * FROM MART.V_TOP_CATEGORIES;
SELECT * FROM MART.V_TOP_CUSTOMER_STATES LIMIT 10;
SELECT * FROM MART.V_DELIVERY_BY_MONTH LIMIT 24;
SELECT * FROM MART.V_PAYMENT_TYPE_SUMMARY;
