# Databricks notebook source


# COMMAND ----------

dbutils.fs.ls('/mnt/bronze')

# COMMAND ----------

customer = spark.read.csv('dbfs:/mnt/bronze/customer/20240105_sales_customer.csv', header=True,inferSchema=True)


# COMMAND ----------

def snakecase(df):
    for cols in df.columns:
        df = df.withColumnRenamed(cols, cols.lower().replace(" ", "_"))
    return df

customerdf = snakecase(customer)



# COMMAND ----------

from pyspark.sql.functions import split

def split_columns(df):
    df = df.withColumn("first_name", split(df["name"], " ")[0])
    df = df.withColumn("last_name", split(df["name"], " ")[1])
    
    # Drop the original "Name" column
    df = df.drop("name")
    
    return df

customerdf = split_columns(customerdf)


# COMMAND ----------

def extract_domain(df):
 
    # Split the email addresses by "@" symbol
    df = df.withColumn("domain", split(df["email_id"], "@").getItem(1))
 
    # Split the domain name by "." symbol and extract the first part
    df = df.withColumn("domain", split(df["domain"], "\.").getItem(0))
 
    return df
 
 
 
extractdf = extract_domain(customerdf)


# COMMAND ----------

from pyspark.sql.functions import when
 
def create_gender_column(df):
 
    # Define conditions for assigning values to the new 'gender' column
 
    # Create the new 'gender' column based on the defined conditions
    df = df.withColumn("gender", when(df["gender"] == "male", "M").when(df["gender"] == "female", "F").otherwise("Unknown"))
 
    return df
    
withgender = create_gender_column(extractdf)


# COMMAND ----------

def split_joining_date(df):
 
 
    # Create new columns 'date' and 'time' based on the split result
    df = df.withColumn("date", split(df["joining_date"], " ").getItem(0))
    df = df.withColumn("time",split(df["joining_date"], " ").getItem(1))
 
    return df


split_joining_date1 = split_joining_date(withgender)
 


# COMMAND ----------

from pyspark.sql.functions import to_date

def format_date_column(df):
    df = df.withColumn("date", to_date(df["date"], "dd-MM-yyyy"))
    return df

fomated_date_df= format_date_column(split_joining_date1)

# COMMAND ----------

def create_expenditure_status_column(df):
    df = df.withColumn("expenditure_status",when(df["spent"] < 200, "MINIMUM").otherwise("MAXIMUM"))
    return df
 
expenditure_df = create_expenditure_status_column(split_joining_date1)

# COMMAND ----------


# COMMAND ----------

def write_delta_upsert(df, delta_path):
    df.write.format("delta").mode("overwrite").save(delta_path)


writeTo = f'dbfs:/mnt/silver/sales_view/customer'
write_delta_upsert(expenditure_df, writeTo)