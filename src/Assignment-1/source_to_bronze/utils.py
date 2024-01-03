# Databricks notebook source
def read_data(spark, file_path):
    data_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
    return data_df
   
def write_to_csv(df, file_path, header=True, mode="overwrite"):
    df.write.csv(file_path, header=header, mode=mode)
    
    

# COMMAND ----------



# COMMAND ----------

# utils_notebook

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def get_custom_schema():
    return StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department_name", StringType(), True),
        StructField("employee_state", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("age", IntegerType(), True)
    ])

