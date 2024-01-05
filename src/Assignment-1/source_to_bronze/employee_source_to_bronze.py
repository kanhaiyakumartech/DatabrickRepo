# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Reading the employee dataframe.
employee_path="dbfs:/FileStore/employee.csv"
mode="permissive"
employee_df = read_csv(spark, employee_path, mode)

# COMMAND ----------

# DBTITLE 1,Reading department dataframe.
department_path="dbfs:/FileStore/department.csv"
department_df = read_csv(spark, department_path)

# COMMAND ----------

# DBTITLE 1,Reading country dataframe
country_path="dbfs:/FileStore/country.csv"
country_df = read_csv(spark, country_path)

# COMMAND ----------

# DBTITLE 1,Writing Country_df to source_to_bronze
path="dbfs:/source_to_bronze/country_df.csv"
write_to_csv(country_df, path)

# COMMAND ----------

# DBTITLE 1,Writing department_df to source_to_bronze
path="dbfs:/source_to_bronze/department_df.csv"
write_to_csv(department_df, path)

# COMMAND ----------

# DBTITLE 1,Writing employee_df to source_to_bronze
path="dbfs:/source_to_bronze/employee_df.csv"
write_to_csv(employee_df, path)
