from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

spark = (
    SparkSession
    .builder
    .appName("Spark Introducion")
    .master("local[*]")
    .getOrCreate()
)

_schema = "first_name string, last_name string, job_title string, dob string, email string, phone string, salary double, department_id int"

emp = spark.read.format("csv").schema(_schema).option("header", True).load("datasets/employee_records.csv")
print(f"Número de partições: {emp.rdd.getNumPartitions()}")

def bonus(salary):
    return int(salary) * 0.1

bonus_udf = udf(bonus)

emp.withColumn("bonus", bonus_udf("salary")).show()
