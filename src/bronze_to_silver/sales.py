# Databricks notebook source
# MAGIC %run ./customer

# COMMAND ----------

raw_sales_df = spark.read.csv('dbfs:/mnt/bronze/sales/20240105_sales_data.csv', header=True, inferSchema=True)



# COMMAND ----------

sales_df = snakecase(raw_sales_df)


# COMMAND ----------

writeTo = f'dbfs:/mnt/silver/sales_view/customer_sale'
sales_df.write.format('delta').mode('overwrite').save(writeTo)