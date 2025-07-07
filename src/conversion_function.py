from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("conversion").getOrCreate()
data=[("James",34,"2006-01-01","true","M",3000.60),
    ("Michael",33,"1980-01-10","true","F",3300.80),
    ("Robert",37,"06-01-1992","false","M",5000.50)
  ]

columns = ["firstname","age","jobStartDate","isGraduated","gender","salary"]
df= spark.createDataFrame(data ,columns)
# df.printSchema()
# df.show()
# df2=df.withColumn("age",df.age.cast("string"))\
#       .withColumn("firstname",col("firstname").cast(IntegerType()))
# print("withcolumn")
# df2.printSchema()
# df3=df.select(col("isGraduated").cast("boolean"))
# print("select")
# df3.printSchema()
df.createOrReplaceTempView("CastExample")
df4 = spark.sql("SELECT STRING(age),BOOLEAN(isGraduated),DATE(jobStartDate) from CastExample")
df4.printSchema()