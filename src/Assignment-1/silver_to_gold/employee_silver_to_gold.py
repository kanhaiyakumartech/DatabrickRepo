# Databricks notebook source
# MAGIC %run "/DataBricksRepo/Assignment-1/source_to_bronze/utils"


# COMMAND ----------


#8.In gold notebook employee_silver_to_gold, call utils notebook in this notebook Read the table stored in silver layer as dataframe and drop load_date
# DBTITLE 1,Reading the delta table stored


filepath = "/silver/employee_info/dim_employee/Employees_table"
silver_df = spark.read.format("delta").load(filepath)



# COMMAND ----------

# DBTITLE 1,9finding salary of each department
from pyspark.sql import functions as F

# Drop load_date column10

silver_df = silver_df.drop("load_date")

# 9 Find salary of each department in desc order
salary_by_department= silver_df.groupBy("department_name").agg(F.("salary").alias("desc_salary"))
salary_by_department = salary_by_department.orderBy(F.desc("desc_salary"))

# 10 Add at_load_date
salary_by_department = salary_by_department.withColumn("at_load_date", F.current_date())

# Update the original DataFrame
silver_df = salary_by_department


# COMMAND ----------

# DBTITLE 1,Salary of each department
silver_df.display()

# COMMAND ----------

file_format='delta'
output_path="/gold/employee/fact_employee"
table_name="fact_employee"
write_delta(silver_df,file_format,output_path,table_name)


