from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("explode array").getOrCreate()
data=(("chandana",["mumbai","delhi"]),
      ("chandu",None),
      ("chan",["udupi","manglore"]),
      ("dana",["bunder","bhatkal"]),)
column=("name","location")
df=spark.createDataFrame(data,column)

df.withColumn("Explode",explode("location")).show()

df.select("name",posexplode("location").alias("position","location")).show()

df.select("name",posexplode_outer("location").alias("position","location")).show()

#this is using map
data=(("chandana",{"id":1,"last_name":"naik"}),
      ("chandu",None),
      ("chan",{"id":2,"last_name":"abs"}),
      ("dana",{"id":3,"last_name":"hhj"}))
column=("name","id_last_name")
df=spark.createDataFrame(data,column)
df.select("name",explode_outer("id_last_name")).show()