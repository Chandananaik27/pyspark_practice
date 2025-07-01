from pyspark.sql import SparkSession
# Step 1: Create a SparkSession
spark = SparkSession.builder \
    .appName("Beginner PySpark Example") \
    .master("local[*]") \
    .getOrCreate()
# Step 2: Create sample data
data = [
    ("Alice", "Math", 85),
    ("Bob", "Math", 90),
    ("Alice", "English", 78),
    ("Bob", "English", 83),
    ("Cathy", "Math", 72),
    ("Cathy", "English", 88)
]
columns = ["Student", "Subject", "Score"]
# Step 3: Create a DataFrame
df = spark.createDataFrame(data, columns)
# Step 4: Show the DataFrame
print("🔹 Full DataFrame:")
df.show()
