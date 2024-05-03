# Databricks notebook source
# MAGIC %run ./customer

# COMMAND ----------

store_df = spark.read.csv('dbfs:/mnt/bronze/store/20240105_sales_store.csv' , header=True , inferSchema=True)


# COMMAND ----------

store_df1 = snakecase(store_df)

# COMMAND ----------

store_category_df = store_df1.withColumn("domain", split('email_address', '@')[1])\
    .withColumn("store_category", split('domain', '\.')[0]).drop('domain')




# COMMAND ----------


formated_date_df = store_category_df.withColumn('created_at', to_date('created_at', 'dd-MM-yyyy'))\
    .withColumn('updated_at', to_date('updated_at', 'dd-MM-yyyy'))




# COMMAND ----------


writeTo = f'dbfs:/mnt/silver/sales_view/store'
write_delta_upsert(formated_date_df, writeTo)