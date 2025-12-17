# Olist-SnowFlake-ELT
End-to-end ELT data warehouse in Snowflake using the Olist e-commerce dataset (RAW→STG→MART) with KPI views, cost governance (Resource Monitor), and zero-copy cloning + UNDROP recovery demos.

# Olist E-commerce ELT Warehouse in Snowflake (RAW → STG → MART)

An end-to-end ELT mini data warehouse built in **Snowflake** using the **Olist Brazilian E-commerce** public dataset. The pipeline loads raw CSVs into a **RAW** layer, cleans and types data in **STG**, and produces analytics-ready **MART** tables (dimensions + facts) with KPI views.

## What this project demonstrates
- **ELT architecture:** RAW → STG → MART
- **Snowflake ingestion:** Internal **Stage** + `COPY INTO`
- **Analytics modeling:** Dimensions + Fact tables
- **KPI views:** Revenue, orders, AOV, review score, delivery performance, top categories/states, payment summary
- **Cost governance:** XSMALL warehouses + auto-suspend + **Resource Monitor**
- **Safe development workflow:** **Zero-Copy Cloning** + **UNDROP** recovery demo
- **Performance concept:** Query Result Cache (run the same query twice)

---

## Dataset
**Brazilian E-Commerce Public Dataset by Olist** (CSV files).  
Typical input files used:
- `olist_customers_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `product_category_name_translation.csv`
- *(Optional)* `olist_geolocation_dataset.csv`

---

## Architecture
### 1) RAW (landing)
Raw tables loaded from staged CSVs with minimal typing to prevent ingestion failures.

### 2) STG (clean + typed)
- Timestamp parsing (`TRY_TO_TIMESTAMP_NTZ`)
- Text cleanup (`TRIM`, `UPPER`)
- Basic de-duplication via `ROW_NUMBER()`

### 3) MART (analytics)
**Dimensions**
- `MART.DIM_CUSTOMER`
- `MART.DIM_SELLER`
- `MART.DIM_PRODUCT`
- *(Optional)* `MART.DIM_GEOLOCATION_ZIP` (zip prefix level)

**Facts**
- `MART.FCT_ORDERS` (order-level metrics: revenue, delivery, late flag, avg review)
- `MART.FCT_ORDER_ITEMS` (item-level line details)

---

## KPI Views
Core KPI views:
- `MART.V_KPI_MONTHLY` → monthly orders, revenue, AOV, avg review, late delivery %
- `MART.V_TOP_CATEGORIES` → top categories by revenue
- `MART.V_TOP_CUSTOMER_STATES` → revenue/orders by customer state
- `MART.V_DELIVERY_BY_MONTH` → avg delivery days + late delivery % by month
- `MART.V_PAYMENT_TYPE_SUMMARY` → payment totals by payment type

Bonus KPI views:
- `MART.V_REPEAT_CUSTOMERS_MONTHLY` → repeat customer rate (month-level)
- `MART.V_LATE_DELIVERY_REVIEW_IMPACT` → compares review score + delivery time for late vs on-time deliveries

### Example insight (from the bonus view)
Late deliveries strongly correlate with worse customer experience:
- On-time orders: avg review score ≈ **4.21**, avg delivery days ≈ **10.8**
- Late orders: avg review score ≈ **2.57**, avg delivery days ≈ **31.3**

---

## Snowflake features showcased
- **Virtual Warehouses:** `WH_ETL` (loading/transforms) and `WH_BI` (analytics)
- **Auto-suspend / Auto-resume:** reduces compute spend
- **Resource Monitor:** daily credit quota guardrail
- **Zero-copy clone:** `OLIST_DW_DEV CLONE OLIST_DW`
- **UNDROP:** recovery demonstration in DEV
- **Query Result Cache:** repeat-query performance concept

---

## Repo structure
