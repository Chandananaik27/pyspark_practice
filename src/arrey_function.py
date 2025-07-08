from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Array Function") \
    .getOrCreate()
df=spark.range(1)
hobbies_df = df.select(array(lit("Singing"), lit("Dancing"), lit("Coding")).alias("hobbies"))

hobbies_df.show(truncate=False)
data = [("Chandana",), ("Ravi",), ("Priya",)]
columns = ["name"]
df = spark.createDataFrame(data, columns)

df_with_skills = df.withColumn("skills", array(lit("Python"), lit("SQL"), lit("Git")))
df_with_skills.withColumn("array_contains_python",array_contains(df_with_skills.skills,"Python")).show()
df_with_skills.show(truncate=False)
df_with_skills.withColumn("count_skills",size("skills")).show()
df_with_skills.withColumn("position",array_position("skills","Python")).show()
df_with_skills.withColumn("updated_skills",array_remove("skills","Python")).show()