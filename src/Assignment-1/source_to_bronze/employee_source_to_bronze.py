# Databricks notebook source
# MAGIC %run "/DataBricksRepo/Assignment-1/source_to_bronze/utils"


# COMMAND ----------

#here i am creating dataFrame and writ in dbfs
dataset1=[(11,"james","D101","ny",9000,34),
(12,"michel", "D101","ny",8900,32),
(13,"robert","D102","ca",7900,29),
(14,"scott","D103","ca",8000,36),
(15,"jen","D102","ny",9500,38),
(16,"jeff","D103","uk",9100,35),
(17,"maria","D101","ny",7900,40)
]
column=["employee id","employee_name","department","State","salary","Age"]
df=spark.createDataFrame(dataset1,column)
df.write.csv("dbfs:/FileStore/employee.csv", header=True, mode="overwrite")
# df.show()

Dataset2=[("D101","sales"),
("D102","finance"),
("D103","marketing"),
("D104","hr"),
("D105","support")]
Column_names=["dept_id", "dept_name"]
df1=spark.createDataFrame(Dataset2,Column_names)
df1.write.csv("dbfs:/FileStore/department.csv", header=True, mode="overwrite")
#df.show()
Dataset3=[("ny","newyork"),
("ca","California"),
("uk","Russia")]
Column_names=["country_code", "country_name"]
df2=spark.createDataFrame(Dataset3,Column_names)
df2.write.csv("dbfs:/FileStore/country.csv", header=True, mode="overwrite")
#df.show()



# COMMAND ----------

#Here i am reading employeetable
employee_path="dbfs:/FileStore/employee1.csv"
employee_df=read_data(spark,employee_path).show()

# COMMAND ----------

#Here i am reading depertement table
department_path="dbfs:/FileStore/department1.csv"
department_df = read_data(spark, department_path).show()

# COMMAND ----------


#Here i am reading country table
country_path="dbfs:/FileStore/country1.csv"
country_df = read_data(spark, country_path).show()

# COMMAND ----------


