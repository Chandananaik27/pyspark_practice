from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName('windowfunction').getOrCreate()

simpleData = [("James", "Sales", 3000),
              ("Michael", "Sales", 4600),
              ("Robert", "Sales", 4100),
              ("Maria", "Finance", 3000),
              ("James", "Sales", 3000),
              ("Scott", "Finance", 3300),
              ("Jen", "Finance", 3900),
              ("Jeff", "Marketing", 3000),
              ("Kumar", "Marketing", 2000),
              ("Saif", "Sales", 4100)
              ]

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
# df.printSchema()
# df.show(truncate=False)

window=Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(window))\
    .withColumn("rank",rank().over(window))\
    .withColumn("dense_rank",dense_rank().over(window)).show()

df.withColumn("ntile",ntile(4).over(window)).show()

df.withColumn("lag_salary",lag("salary",2).over(window)).show()

df.withColumn("lead_salary",lead("salary",1).over(window)).show()