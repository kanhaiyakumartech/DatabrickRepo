# Databricks notebook source
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, StringType, IntegerType
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Reading the csv files.
#This Function is  to read the csv file

def read_csv(spark, file_path, mode='permissive', custom_schema=None):
    if custom_schema:
        return spark.read.option("mode", mode).schema(custom_schema).csv(file_path, header=True)
    else:
        return spark.read.option("mode", mode).csv(file_path, header=True)

# COMMAND ----------

# DBTITLE 1,Writing the csv files.
#This is function towrite the csv to a specific path
def write_to_csv(df, file_path, header=True, mode="overwrite"):
    df.write.csv(file_path, header=header, mode=mode)

# COMMAND ----------

# DBTITLE 1,Write to delta table.
def write_delta(df,file_format,output_path,table_name):
    df.write.format(file_format).mode("overwrite").option("path", output_path).saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Reading from delta table.
def read_delta(spark,file_format,delta_location):
    df = spark.read.format(file_format).load(delta_location)
    df.display()
    return df

# COMMAND ----------

# DBTITLE 1,Function to rename columns.
def rename_columns(df, column_mapping):
    for old_col, new_col in column_mapping.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df

# COMMAND ----------

# DBTITLE 1,Function to convert column names to lower
 # Convert column names to lowercase
def convert_column_names_to_lowercase(df):
    df = df.toDF(*[col.lower() for col in df.columns])
    return df

# COMMAND ----------

# DBTITLE 1,Function to add date column
# Load_date column with current date
def add_load_date_column(df, colum_name):
    df = df.withColumn(colum_name, F.current_date())
    return df

# COMMAND ----------

# DBTITLE 1,Function to drop columns
def drop_columns(df, columns_to_drop):
    return df.drop(*columns_to_drop)

def add_at_load_date(df, new_column_name):
    return df.withColumn(new_column_name, F.current_date())

def order_salary_by_department(df, salary_column, department_column):
    return df.orderBy(department_column, F.desc(salary_column))

# COMMAND ----------

# DBTITLE 1,Function to order salary by department.
def order_salary_by_department(df, salary_column, department_column):
    return df.orderBy(department_column, F.desc(salary_column))
