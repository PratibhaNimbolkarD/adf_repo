# Databricks notebook source
# MAGIC %run ./customer

# COMMAND ----------

product = spark.read.csv('dbfs:/mnt/bronze/products/20240105_sales_product.csv' , header=True , inferSchema=True)


# COMMAND ----------

product_snakecase = snakecase(product)


# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import when, col
def create_subcategory(df):
    sub_category_df = df.withColumn("sub_category", when(col('category_id') == 1, "phone")\
        .when(col('category_id') == 2 , "laptop")\
        .when(col('category_id') == 3, "playstation")\
        .when(col('category_id') == 4, "e-device"))
    return sub_category_df
    

subcategory_df = create_subcategory(product_snakecase)


# COMMAND ----------

writeTo = f'dbfs:/mnt/silver/sales_view/product'
write_delta_upsert(subcategory_df, writeTo)