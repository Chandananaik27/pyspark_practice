from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import StorageLevel
spark=SparkSession.builder\
    .appName("transformation and action")\
    .getOrCreate()
data=[(2,"chandu",67,"cse"),
    (2,"shreya",98,"aiml"),
    (3,"diva",91,"ece"),
    (4,"stewart",87,"ece"),
    (5,"Edwin",69,"eee")]
column_name=["usn","name","marks","branch"]
df=spark.createDataFrame(data,column_name)

#Filter rows based on a condition.
df1=df.filter(df.marks>80)
print("to check wewther the cahe is working properly or not")
df1.cache()
#Select specific columns from the DataFrame.
df1.select("usn","name","marks").show()

#Add a new column or modify an existing one.
df1.withColumn("marks > 80",df1["marks"]>80).show()

#persist
df1=df.filter(df.marks>80)
print("to check weather the persist is working properly or not")
df1.persist(StorageLevel.MEMORY_AND_DISK)
#Select specific columns from the DataFrame.
df1.select("usn","name","marks").show()

#Add a new column or modify an existing one.
df1.withColumn("marks > 80",df1["marks"]>80).show()




