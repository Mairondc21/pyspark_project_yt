from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import spark_partition_id

spark = (
    SparkSession
    .builder
    .appName("Spark Introducion")
    .master("local[*]")
    .getOrCreate()
)


df = spark.read.format("csv").option("header", True).option("inferschema",True).load("datasets/emp.csv")

df.printSchema()

emp_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date, bad_record string"

df_schema = spark.read.format("csv").schema(emp_schema).load("datasets/emp.csv")

df_schema.printSchema()

df_p = spark.read.format("csv").schema(emp_schema).option("columnNameOfCorruptRecord","bad_record").option("header",True).load("datasets/emp_new.csv")

df_p.show()