from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("joins").getOrCreate()
employees = spark.createDataFrame([
    (1, "Alice", 10),
    (2, "Bob", 20),
    (3, "Charlie", 30),
    (4, "David", 40),
    (5, "Eva", None)
], ["emp_id", "name", "dept_id"])
departments = spark.createDataFrame([
    (10, "HR"),
    (20, "Engineering"),
    (50, "Marketing")
], ["dept_id", "dept_name"])
departments.show()
employees.show()
df=employees.join(departments,"dept_id","inner")
df.show()
print("left join")
employees.join(departments,"dept_id","left").show()
print("right join")
employees.join(departments,"dept_id","right").show()
print("full join")
employees.join(departments,"dept_id","full").show()
print("left_semi join")
employees.join(departments,"dept_id","left_semi").show()
print("left_anti join")
employees.join(departments,"dept_id","leftanti").show()



