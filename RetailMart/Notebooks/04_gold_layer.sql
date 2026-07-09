# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retailmart_databricks.gold.sales_summary_by_category AS
# MAGIC Select p.category, SUM(o.discounted_amount) as total_revenue , COUNT(o.order_id) as total_orders, AVG(o.discount_pct) as avg_discount_pct, SUM(o.discounted_amount - (o.quantity * p.cost_price)) as total_profit FROM retailmart_databricks.silver.products_cleaned p INNER JOIN retailmart_databricks.silver.orders_enriched o ON p.product_id = o.product_id GROUP BY p.category

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retailmart_databricks.gold.customer_order_summary AS
# MAGIC SELECT c.customer_id, c.name, c.city, c.state, c.segment, COUNT(o.order_id)  as total_orders_placed, SUM(o.discounted_amount)  as total_amount_spent, CAST(SUM(o.discounted_amount) / COUNT(o.order_id) AS  DECIMAL(10,2)) AS avg_order_value, MAX(o.order_date) as last_order_date,
# MAGIC row_number() OVER (PARTITION BY c.segment ORDER BY SUM(o.discounted_amount) DESC ) as spent_rank
# MAGIC FROM retailmart_databricks.silver.customers_cleaned c INNER JOIN retailmart_databricks.silver.orders_enriched o ON c.customer_id = o.customer_id 
# MAGIC GROUP BY c.customer_id, c.name, c.city, c.state, c.segment

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE retailmart_databricks.gold.monthly_sales_trend AS
# MAGIC SELECT year(o.order_date) as year, month(o.order_date) as month, SUM(o.discounted_amount) as total_revenue, COUNT(o.order_id) as total_orders,
# MAGIC ROUND((SUM(o.discounted_amount) - LAG(SUM(o.discounted_amount)) OVER (ORDER BY year(o.order_date), month(o.order_date))) / 
# MAGIC LAG(SUM(o.discounted_amount)) OVER (ORDER BY year(o.order_date), month(o.order_date)) * 100,2)  as mom_growth_pct
# MAGIC FROM retailmart_databricks.silver.orders_enriched o GROUP BY year(o.order_date), month(o.order_date)
