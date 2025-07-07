from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("PySparkSQL").getOrCreate()

data = [
    (1, "Alice", 5000),
    (2, "Bob", 6000),
    (3, "Charlie", 4000),
    (4, "David", 7000)
]
columns = ["emp_id", "name", "salary"]
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()

# Register as SQL table
df.createOrReplaceTempView("employees")

# Run SQL queries
# Select all employees with salary > 5000
spark.sql("SELECT * FROM employees WHERE salary > 5000").show()

# Find average salary
spark.sql("SELECT AVG(salary) AS avg_salary FROM employees").show()

# Group by salary and count
spark.sql("SELECT salary, COUNT(*) AS emp_count FROM employees GROUP BY salary").show()
