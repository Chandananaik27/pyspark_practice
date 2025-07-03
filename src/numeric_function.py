from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("numeric operation").getOrCreate()
data=[("chan",56,78),("shreya",98,67),("stew",67,90),("edwin",78,87),("diva",78,55)]
column=["name","science_marks","maths_marks"]
df=spark.createDataFrame(data,column)
df.select("name",df.maths_marks,abs(df.maths_marks-70).alias("abs difference")).show()

df.select("name","maths_marks",df.science_marks,round(df.science_marks/8,1).alias("rounding of values")).show()

df.select("name","maths_marks",ceil(df.maths_marks/7).alias("ceilingvalue")).show()

df.select("name","maths_marks",floor(df.maths_marks/7).alias("floorvalue")).show()

df.select("name","maths_marks",sqrt(df.maths_marks).alias("sqrtvalue")).show()
df.select("name","maths_marks",round(sqrt(df.maths_marks),3).alias("sqrtvalue")).show()

df.select("name","maths_marks",pow(df.maths_marks,2).alias("powervalue")).show()

df.select("name","maths_marks","science_marks",greatest("maths_marks","science_marks").alias("greatestvalue")).show()
df.select("name","maths_marks","science_marks",least("maths_marks","science_marks").alias("greatestvalue")).show()