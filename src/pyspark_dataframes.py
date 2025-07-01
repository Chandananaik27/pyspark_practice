from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("DataFrameExample").master("local[*]").getOrCreate()
data = [
    (1, "Alice", 90),
    (2, "Bob", 85),
    (3, "Charlie", 95),
    (4,"mike",78),
    (5,"manu",90),
    (6,"kiccha",67)
]
# Column names
columns = ["id", "name", "score"]
# Create DataFrame
df = spark.createDataFrame(data, columns)
df.select("id","name").show()
df.filter(df.score<80).show()
df.groupBy("score").agg(sum(df.score)).show()
df.withColumn("score increased by 5",df.score+5).show()
