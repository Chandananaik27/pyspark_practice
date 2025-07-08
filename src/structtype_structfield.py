from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark=SparkSession.builder.appName("numeric operation").getOrCreate()
# data=[("chan",56,78),("shreya",98,67),("stew",67,90),("edwin",78,87),("diva",78,55)]
data=[(('chan','naik'),56,78),(('shreya','nayak'),98,67),(('chandu','naik'),67,90),(('chandana','naik'),78,87)]
column=["name","science_marks","maths_marks"]
df=spark.createDataFrame(data,column)
df.printSchema()
schema=StructType([StructField('name', StringType(), True),
                   StructField('science_marks', IntegerType(), True),
                   StructField('maths_marks', IntegerType(), True),])
df=spark.createDataFrame(data,schema)
df.printSchema()
#nested columns
struct=StructType([StructField('first_name', StringType(), True),
                   StructField('last_name', StringType(), True),])
schema=StructType([StructField('name', struct, True),
                   StructField('science_marks', IntegerType(), True),
                   StructField('maths_marks', IntegerType(), True),])
df=spark.createDataFrame(data,schema)
df.printSchema()
df.show()