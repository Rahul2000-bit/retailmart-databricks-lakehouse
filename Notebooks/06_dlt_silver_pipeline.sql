-- Databricks notebook source

CREATE OR REFRESH STREAMING LIVE TABLE orders_silver_dlt(
    CONSTRAINT valid_amount EXPECT (total_amount > 0) ON VIOLATION DROP ROW,
    CONSTRAINT valid_order_date EXPECT(order_date IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
order_id,
customer_id,
product_id,
quantity,
unit_price,
CAST(total_amount AS DECIMAL(10,2)) AS total_amount,
CAST(discount_pct AS DECIMAL(10,2)) AS discount_pct,
CAST(order_date AS DATE) AS order_date,
CAST(delivery_date AS DATE) AS delivery_date,
status,
payment_mode,
store_id,
_ingested_at,
_source_file,
CAST(total_amount AS DECIMAL(10,2)) - (CAST(total_amount AS DECIMAL(10,2)) * CAST(discount_pct AS DECIMAL(10,2)) / 100) AS discounted_amount,
current_timestamp() AS _processed_at 
FROM STREAM(retailmart_databricks.bronze.orders_raw)


-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE products_silver_dlt(
    CONSTRAINT valid_product_id  EXPECT(product_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_price  EXPECT(price > 0) ON VIOLATION DROP ROW
)
AS SELECT
product_id,
product_name,
category,
brand,
CAST(price AS DECIMAL(10,2)) AS price,
CAST(cost_price AS DECIMAL(10,2)) AS cost_price,
stock_qty,
supplier_id,
launch_date,
_ingested_at,
_source_file,
(CAST(price AS DECIMAL(10,2)) - CAST(cost_price AS DECIMAL(10,2)))/ CAST(price AS DECIMAL(10,2))* 100 AS profit_margin_pct,
current_timestamp() AS _processed_at
FROM STREAM(retailmart_databricks.bronze.products_raw)


-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers_silver_dlt(
    CONSTRAINT valid_customer_id EXPECT(customer_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_phone EXPECT(LENGTH(phone) = 10) ON VIOLATION DROP ROW
)
AS SELECT
customer_id,
name,
email,
RIGHT(REGEXP_REPLACE(phone, '[^0-9]', ''), 10) AS phone,
city,
state,
pincode,
CAST(signup_date AS DATE) AS signup_date,
segment,
_ingested_at,
_source_file,
current_timestamp() AS _processed_at
FROM STREAM(retailmart_databricks.bronze.customers_raw)
