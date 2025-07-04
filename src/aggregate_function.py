from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("aggregate function").getOrCreate()
data=[("chan","hr",1500),("nidhi","hr",4000),("chai","bi",1500),("sudha","hod",3000)]
column=["name","dept","salary"]
df=spark.createDataFrame(data,column)
df.select(approx_count_distinct("salary").alias("distinct_salary_count")).show()
df.select(avg("salary").alias("avgsalary")).show()
df.select(collect_list("salary")).show()
df.select(collect_list("salary")).show(truncate=False)
df.select(collect_set("salary")).show()
df.select(count("salary")).show()
df.select(count_distinct("salary")).show()
df.select(first("salary").alias("first_salary")).show()
df.groupBy("dept").agg(first("salary").alias("first_salary")).show()
df.select(last("salary").alias("last_salary")).show()
df.groupBy("dept").agg(last("salary").alias("last_salary")).show()
df.select(max("salary").alias("max_salary")).show()
df.groupBy("dept").agg(max("salary").alias("Max_salary")).show()
df.select(min("salary").alias("min_salary")).show()
df.groupBy("dept").agg(min("salary").alias("min_salary")).show()
df.groupBy("dept").agg(sum("salary").alias("sum_salary")).show()
df.select(sum("salary")).show()