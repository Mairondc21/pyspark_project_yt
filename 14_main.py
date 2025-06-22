from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, explode


spark = (
    SparkSession
    .builder
    .appName("Spark Introducion")
    .master("local[*]")
    .getOrCreate()
)

df_single = spark.read.format("json").load("datasets/order_singleline.json")

df_multi = spark.read.format("json").option("multiLine",True).load("datasets/order_multiline.json")

df = spark.read.format("text").load("datasets/order_singleline.json")

_schema = "customer_id string, order_id string, contact array<long>"

df_schema = spark.read.format("json").schema(_schema).load("datasets/order_singleline.json")

_schema_new = "contact array<string>, customer_id string, order_id string, order_line_items array<struct<amount double, item_id string, qty long>>"

df_expanded = df.withColumn("parsed", from_json(df.value,_schema_new)).drop("value")

df_unparsed = df_expanded.withColumn("unparsed", to_json(df_expanded.parsed))

df_expanded.show(truncate=False)

df_1 = df_expanded.select("parsed.*")


df_2 = df_1.withColumn("expanded_line_items", explode("order_line_items"))

df_2.show(truncate=False)

df_3 = df_2.select("contact", "customer_id", "order_id", "expanded_line_items.*")

df_3.show(truncate=False)
