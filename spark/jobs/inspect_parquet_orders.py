from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("inspect_bronze_orders").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

path = "s3a://de-olist-thiago-dev/bronze/olist/orders/ingestion_date=2026-02-04/"
df = spark.read.parquet(path)

print("=== SCHEMA ===")
df.printSchema()

print("=== COLUMNS ===")
print(df.columns)

print("=== ROW COUNT ===")
print(df.count())

print("=== SAMPLE ===")
df.show(10, truncate=False)

spark.stop()
