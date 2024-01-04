

# DBTITLE 1,Reading the dataframes with custom schema, from dbfs location friom source_to_bronze
country_schema=StructType([
    StructField("country_code",StringType(),True),
    StructField("country_name",StringType(),True)
])

employee_schema=StructType([
    StructField("employee_id",IntegerType(),True),
    StructField("employee_name",StringType(),True),
    StructField("department", StringType(), True),
    StructField("State",StringType(), True),
    StructField("Salary",IntegerType(), True),
    StructField("Age",IntegerType(), True)
])

department_schema=StructType([
    StructField("dept_id",StringType(),True),
    StructField("dept_name",StringType(),True),
])


country_path="dbfs:/source_to_bronze/country_df.csv"
country_df=read_csv( file_path=country_path , mode='permissive', custom_schema=country_schema)

employee_path="dbfs:/source_to_bronze/employee_df.csv"
employee1_df=read_csv( file_path=employee_path , mode='failfast', custom_schema=employee_schema)

department_path="dbfs:/source_to_bronze/employee_df.csv"
department1_df=read_csv( file_path=department_path , mode='permissive', custom_schema=department_schema)

# COMMAND ----------

from pyspark.sql import functions as F

# Convert column names to lowercase
employee1_df = employee1_df.toDF(*[col.lower() for col in employee1_df.columns])

# Rename columns
column_mapping = {
    "employee id": "employee_id",
    "employee name": "employee_name",
    "department": "department_name",
    "state": "employee_state",
    "salary": "salary",
    "age": "age"
}
for old_col, new_col in column_mapping.items():
    employee1_df = employee1_df.withColumnRenamed(old_col, new_col)

# Add load_date column with current date
employee1_df = employee1_df.withColumn("load_date", F.current_date())


# COMMAND ----------

# DBTITLE 1,Write dataframe to delta table

file_format='delta'
output_path="/silver/employee_info/dim_employee/Employees_table"
table_name="Employees_table"
write_delta(employee1_df,file_format,output_path,table_name)


