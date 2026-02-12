import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("inspect_one_parquet").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

path = os.environ.get("PARQUET_PATH")
if not path:
    raise SystemExit("Set PARQUET_PATH to a full s3a://.../part-*.parquet")

df = spark.read.parquet(path)

print("PATH =", path)
print("COLUMNS_COUNT =", len(df.columns))
print("COLUMNS =", df.columns)
df.printSchema()

spark.stop()
