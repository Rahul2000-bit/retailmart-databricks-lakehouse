# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE Table   EXTENDED retailmart_databricks.gold.sales_summary_by_category

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM retailmart_databricks.gold.sales_summary_by_category VERSION AS OF 0
# MAGIC -- this will provide the table's state at the stage VERSION 0

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE retailmart_databricks.silver.orders_enriched
# MAGIC -- this will compact the data files od the table, which helps in speed up the querying of table

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM retailmart_databricks.silver.orders_enriched RETAIN 168 HOURS
# MAGIC
# MAGIC -- As we know each time we do any operations to the delta table it will be versioned and it will act  work only according to the current version of the table that delta log  provided , so the unwanted data files are not  getting removed. So if we not required those data files in future then we permenently delete those with vaccum command. here it will get deleted after 168 hours.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE retailmart_databricks.silver.customers_cleaned ADD COLUMN (region STRING)
# MAGIC
# MAGIC -- Here,  we are using ALTER TABLE to do the schema evolution, but we preferred ALTER TABLE only because we are not inputting any data to that table currently. That is why ALTER TABLE is used. But in the case, once we are doing any PySpark data frame, and at that time any new data is coming, new column with the data is coming from the source and we need to write it to the target table, that time we would be using the option merge schema equal to true. This will help for automatic schema evolution. But in this case, we are using ALTER TABLE with the data type and the column name to create a new column without any data.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY retailmart_databricks.silver.orders_enriched
# MAGIC
# MAGIC --Describe history provides the entire table history that how the table started with zero versions and it will show all the versions of table that have done any operations on that table. In this we have version till 2. For zero to version it is create table as select. Version 1 was vacuum start operation. Version 2 was vacuum end operation.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM retailmart_databricks.silver.products_cleaned RETAIN 144 HOURS
