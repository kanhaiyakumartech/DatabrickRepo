 from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType
from pyspark.sql.functions import explode_outer, col, split, lit
import pyspark.sql.functions as F

def process_data(api_response):
    spark = SparkSession.builder.getOrCreate()

    # Create a DataFrame from the API response
    db = spark.sparkContext.parallelize([api_response])
    site_info_df = spark.read.option("multiline", True).json(db)

    # Drop unnecessary columns
    columns_to_drop = ["page", "per_page", "total", "total_pages", "support"]
    site_info_df = site_info_df.drop(*columns_to_drop)

    # Define the schema for the DataFrame
    element_schema = StructType([
        StructField("avatar", StringType(), True),
        StructField("email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("id", LongType(), True),
        StructField("last_name", StringType(), True)
    ])

    data_schema = StructType([
        StructField("data", ArrayType(StructType(element_schema)), True)
    ])

    # Convert array type to struct type for data
    df_explode = site_info_df.withColumn("data", explode_outer(col("data")))
    site_info_df = df_explode.select('data.*')

    # Derive a new column 'site_address' from email with values 'reqres.in'
    site_info_df = site_info_df.withColumn("site_address", F.lit("reqres.in"))

    return site_info_df

def write_to_delta(df, delta_path, delta_table_name):
    # Write DataFrame to DBFS location in Delta format with overwrite mode
    df.write.format("delta").option('mergeschema', True).mode("overwrite").save(delta_path)

    # Create or replace a Delta table in the specified database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS site_info")
    df.write.format("delta").option('mergeschema', True).mode("overwrite").saveAsTable(delta_table_name)
