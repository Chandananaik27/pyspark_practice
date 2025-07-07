from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark=SparkSession.builder.appName("date and Timestamp").getOrCreate()

# datetype default format yyyy-mm-dd
data=[["1","2025-02-01"],["2","2025-03-01"],["3","2025-04-01"]]
#hour, minute,second
data=[["1","2020-02-01 11:01:19.06"],["2","2019-03-01 12:01:19.406"],["3","2021-03-01 12:01:19.406"]]
data=[["1","02-01-2020 11 01 19 06"],["2","03-01-2019 12 01 19 406"],["3","03-01-2021 12 01 19 406"]]
df=spark.createDataFrame(data,["id","input"])

df1=df.withColumn("current_date",current_date())
df1.show()

df1=df.select("input",date_format("input","dd-mm-yyyy").alias("date_format"))
df1.show()

df.withColumn("to_date",to_date("input","yyyy-mm-dd")).show()

df.withColumn("date_diff",date_diff(current_date(),"input")).show()

df.withColumn("monthbetween",months_between(current_date(),"input")).show()

df.select("input",add_months("input",3).alias("add_months"),
          date_add("input",5).alias("adding_date"),
          date_sub("input",2).alias("subtracting_date"),
          add_months("input",-3).alias("subtracting_month")).show()

df.withColumn("year", year("input"))\
              .withColumn("month", month("input")) \
              .withColumn("day", day("input")).show()

df.select("input",year("input").alias("year")).show()

df.select(col("input"),
     dayofweek(col("input")).alias("dayofweek"),
     dayofmonth(col("input")).alias("dayofmonth"),
     dayofyear(col("input")).alias("dayofyear"),
  ).show()

df.withColumn("currentdatetime",current_timestamp()).show(truncate=False)

df.withColumn("hour",hour("input"))\
   .withColumn("minute",minute("input"))\
    .withColumn("seconds",second("input")).show()

df.select("input",
    to_timestamp("input", "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp")
  ).show(truncate=False)