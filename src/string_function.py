from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("string").getOrCreate()
data = [
    (" alice ", "alice@example.com", "India"),
    ("  Bob", "bob.smith@work.org", "USA"),
    ("CHARLIE  ", "charlie@edu.co.uk", "UK"),
    ("DaViD", "david123@site.net", "Australia"),
    ("eve", "eve99@web.io", "Canada")
]

columns = ["name", "email", "country"]

df = spark.createDataFrame(data, columns)

df.select(upper("name").alias("upper_case")).show()
df.withColumn("upper_name",upper("name")).show()
df.select("name",lower("name").alias("lower_name")).show()

df.select(trim("name").alias("trimmed_name")).show()
df.withColumn("trimmed_name",trim("name")).show()

df.select("name",ltrim("name").alias("ltrimmed_name")).show()
df.withColumn("rtrimmed_name",rtrim("name")).show()

df.select("email",substring_index("email","@",1)).show()
df.withColumn("substring",substring_index("email","@",-1)).show()

df.withColumn("substring",substring("email",1,6)).show()
df.select("email",split("email","@")).show(truncate=False)
df.select("email",split("email","@")[0].alias("username"),
          split("email","@")[1].alias("domain")).show()

df.select("name",repeat("name",3).alias("repeatedname")).show(truncate=False)

df.select("name",rpad("name",10,"-").alias("rpaddedd")).show()

df.select("name",lpad("name",10,"-").alias("lpaddedd")).show()

df.select("email",regexp_extract("email","@([a-zA-Z0-9.]+)",0)
          .alias("regularexp")).show()
df.select("email",regexp_extract("email","@([a-zA-Z0-9.]+)",1)
          .alias("regularexp")).show()

df.select("email",length("email").alias("email_length")).show(truncate=False)
df.filter(length("email") > 15).show(truncate=False)

df.select("email",instr("email", "example").alias("at_position")).show(truncate=False)
df.select("email",instr("email", "@").alias("at_position")).show(truncate=False)

df.select("name",initcap("name").alias("name_initcap")).show(truncate=False)

# Replaces all substrings in the column that match the regex pattern with the replacement string.
df.select("email",regexp_replace("email", ".*@", "hidden@").alias("masked_email")).show(truncate=False)
