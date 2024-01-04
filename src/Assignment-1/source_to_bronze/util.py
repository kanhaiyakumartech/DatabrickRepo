# Databricks notebook source
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, StringType, IntegerType
from pyspark.sql import functions as F


#function to read the csv file
def read_csv( file_path, mode='permissive', custom_schema=None):
    if custom_schema:
        return spark.read.option("mode", mode).schema(custom_schema).csv(file_path, header=True)
    else:
        return spark.read.option("mode", mode).csv(file_path, header=True)



#write the csv to a specific path
def write_to_csv(df, file_path, header=True, mode="overwrite"):
    df.write.csv(file_path, header=header, mode=mode)


# DBTITLE 1,Write to delta table
def write_delta(df,file_format,output_path,table_name):
    df.write.format(file_format).mode("overwrite").option("path", output_path).saveAsTable(table_name)


# DBTITLE 1,Read from delta table
def read_delta(spark,file_format,delta_location):
    df = spark.read.format(file_format).load(delta_location)
    df.display()
    return df
