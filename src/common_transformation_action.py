from pyspark.sql import SparkSession
spark=SparkSession.builder\
    .appName("transformation and action")\
    .getOrCreate()
data=[(1,"chandu",67,"cse"),
    (2,"shreya",98,"aiml"),
    (3,"diva",91,"ise"),
    (4,"stewart",87,"ece"),
    (5,"Edwin",69,"eee")]
column_name=["usn","name","marks","branch"]
df=spark.createDataFrame(data,column_name)
#ACTIONS
# to show the dataframe output
df.show()
#for count the number of rows
print("count of rows",df.count())
#Returns all rows as a list of Row objects.
row=df.collect()
print("rows",row)

#TRANSFORMATION
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

#Select specific columns from the DataFrame.
df2=df1.select("usn","name","marks")

#Add a new column or modify an existing one.
greater_than_80=df2.withColumn("marks > 80",df2["marks"]>80)
greater_than_80.show()

#Group rows and compute aggregates.
df1=df.groupBy("branch").agg(avg("marks"))
df1.show()

#distinct() removes duplicate rows — meaning rows where all columns are exactly the same.
df.distinct().show()

#if you want distinct values based on specific columns (e.g., usn)
df.select("usn").distinct().show()
