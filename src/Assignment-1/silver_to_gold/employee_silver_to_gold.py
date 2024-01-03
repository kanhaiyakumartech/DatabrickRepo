# Databricks notebook source
# MAGIC %run "/DataBricksRepo/Assignment-1/source_to_bronze/utils"

# COMMAND ----------

#8.In gold notebook employee_silver_to_gold, call utils notebook in this notebook Read the table stored in silver layer as dataframe and drop load_date

# Read from Delta table in silver layer
silver_df = spark.read.format("delta").load("/silver/employee_info/dim_employee")

# Drop load_date
silver_df = silver_df.drop("load_date")

# 9.find the salary of each department in desc order
salary_by_department = silver_df.groupBy("department_name").agg({"salary": "max"}).orderBy("max(salary)", ascending=False)
#10 Add at_load_date
salary_by_department_add = salary_by_department.withColumn("at_load_date", current_date())

#11.Write the df to dbfs location /gold/employee/table_name(fact_employee) with overwrite and replace where condition on at_load_date
 salary_by_department.write.format("delta").mode("overwrite").option("replaceWhere", "1=1").save("/gold/employee/fact_employee"))


# COMMAND ----------


