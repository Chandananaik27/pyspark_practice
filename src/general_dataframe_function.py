from pyspark.sql import SparkSession
spark=SparkSession.builder\
    .appName("dataframe")\
    .getOrCreate()
data = [
    (1, "Alice chand naik", 90),
    (2, "Bob gsh jdhbd", 85),
    (3, "Charlie bbh hbh", 95),
    (4,"mikebhbhbjnjnjnjnj",78),
    (5,"manunbnbnnjhjjh",90),
    (6,"kicchajjbjbhfghj",67)
]
# Column names
columns = ["id", "name", "score"]
df=spark.createDataFrame(data,columns)
df.show()
df.show(n=2)
df.show(truncate=5)
df.show(truncate=True)
df.show(truncate=False)
df.show(vertical=True)
rows=df.collect()
print("collect:")
for row in rows :
    print(row)
rows=df.take(3)
for row in rows:
    print(row)
df.printSchema()
print("count of rows",df.count())
df.select("id","name").show()
df.filter(df.score<80).show()

data=[("chandan",),("manoj",),("divakar",),("chandr",),("dhanya",),("charan",)]
colums=["name"]
df=spark.createDataFrame(data,colums)
df.filter(df.name.like("c%")).show() #Starts with c
df.filter(df.name.like("%r")).show() #ends with r
df.filter(df.name.like("%d%")).show() # Contains d
df.filter(df.name.like("_h%")).show() #Find names where the second letter is h
df.filter(df.name.like("c%n")).show() # Starts with c and ends with n

data=[("chandan",56),("manoj",78),("divakar",56),("chandr",90),("dhanya",87),("charan",60)]
colums=["name","marks"]
df=spark.createDataFrame(data,colums)
df.sort("marks").show() # ascending by default
df.sort(df.marks.desc()).show() # descending

# First sort by marks descending (highest marks first)
# If marks are the same, then sort by name ascending (alphabetical)
df.sort(df.marks.desc(),df.name.asc()).show() #mixed sort

df.describe().show()
df.describe("marks").show()