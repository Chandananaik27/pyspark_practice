from pyspark import SparkContext
sc=SparkContext(appName="frst sparkcontext creation",master="local[*]")
rdd=sc.parallelize([1,2,34,3])
print(rdd.collect())

# Using SparkSession (preferred modern method)
from pyspark.sql import SparkSession
spark=SparkSession.builder\
    .appName("sparkcontext creation")\
    .getOrCreate()
sc=spark.sparkContext
rdd=sc.parallelize([3,4,6,78])
rdd1=rdd.map(lambda x:x**2).collect()
print("rdd elements",rdd.collect())
print("rdd1 square elemnts",rdd1)

