from pyspark.sql import SparkSession


spark = (
    SparkSession
    .builder
    .appName("Spark Introducion")
    .master("local[*]")
    .getOrCreate()
)

df_parquet = spark.read.format("parquet").load("datasets/sales_total_parquet/*.parquet")


df_orc = spark.read.format("orc").load("datasets/sales_data.orc")

df_parquet.show()