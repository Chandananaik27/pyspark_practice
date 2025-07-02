from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("reading files").getOrCreate()
df=spark.read.csv("../sample_files/Department-Q1.csv",header=True)
df1 = spark.read.format("csv").load("../sample_files/Department-Q1.csv",header=True)
df2= spark.read.format("csv").option("header","True").load("../sample_files/Department-Q1.csv")
df.show()
df=spark.read.format("csv").load(["../sample_files/Department-Q1.csv","../sample_files/Country-Q1.csv"]
                                 ,header=True,mergeSchema=True)

df3=spark.read.json("../sample_files/employer_new_1.json",multiLine=True)
df.show()

df3=spark.read.parquet("../sample_files/sampleuserdata 1.parquet")
df3.show()

