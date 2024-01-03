# Databricks notebook source
# MAGIC %run "/DataBricksRepo/Assignment-1/source_to_bronze/utils"

# COMMAND ----------

# Read from DBFS
employee_df = read_data(spark, "dbfs:/FileStore/employee.csv").show()


# COMMAND ----------


# 4.Get custom schema from utils
custom_schema = get_custom_schema()

# Read the file using different read methods with custom schema

file_path = "dbfs:/FileStore/employee.csv"

# Using CSV format with custom schema
employee_df_csv = spark.read.format("csv").schema(custom_schema).load(file_path)

# Using Parquet format with custom schema
employee_df_parquet = spark.read.format("parquet").schema(custom_schema).load(file_path)

# Using Delta format with custom schema
employee_df_delta =spark.read.format("delta").schema(custom_schema).load(file_path)


# COMMAND ----------

#5. convert all the column names into lower caseHere
#here i convert column of employee table
bronze_df = read_data(spark, "dbfs:/FileStore/employee.csv")
bronze_df = bronze_df.toDF(*[col.lower() for col in bronze_df.columns])#.show()

#Here i am converting column in lower case of country tables
bronze_df1 = read_data(spark, "dbfs:/FileStore/country.csv")
bronze_df1 = bronze_df1.toDF(*[col.lower() for col in bronze_df1.columns]).show()

#here i convert departement table
bronze_df2 = read_data(spark, "dbfs:/FileStore/department.csv")
bronze_df2 = bronze_df2.toDF(*[col.lower() for col in bronze_df2.columns]).show()
#
#6 Rename columns
bronze_df = bronze_df.withColumnRenamed("employee id", "employee_id")\
    .withColumnRenamed("employee_name", "employee_name")\
        .withColumnRenamed("department","department_name")\
            .withColumnRenamed("state", "employee_state")
#Here i verify the data After rename
bronze_df.show()




# COMMAND ----------

#7 Add load_date column
from pyspark.sql import functions as F

current_date = F.current_date()

# display(current_date)
# bronze_df_add=bronze_df.withColumn("load_date",current_date
# bronze_df_Add.show()
# Write to Delta table in silver layer
# bronze_df_delta= bronze_df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/employee.csv")
# Write to Delta table in silver layer
# delta =bronze_df.write.format("delta").mode("overwrite").save("/silver/employee_info/dim_employee")

# COMMAND ----------

# Add load_date column with the current date
df = bronze_df.withColumn("load_date", F.current_date())

# Define the Delta table location
db_name = "employee_info"
table_name = "dim_employee"
delta_table_location = f"/silver/{db_name}/{table_name}"

# Write the DataFrame as a Delta table
bronze_df.write.format("delta").mode("overwrite").save(delta_table_location)

# Optionally, create DeltaTable instance for managing Delta table operations
delta_table = DeltaTable.forPath(spark, delta_table_location)

# Optimize the table
delta_table.optimize()

# Vacuum the table to remove old versions
delta_table.vacuum()

# Display Delta table history
delta_table.history().show(truncate=False)




