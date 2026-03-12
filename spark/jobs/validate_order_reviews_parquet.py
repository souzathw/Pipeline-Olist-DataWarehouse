import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("validate_order_reviews_parquet").getOrCreate()

bucket = os.getenv("S3_BUCKET", "de-olist-thiago-dev")
ing = os.getenv("INGESTION_DATE")
if not ing:
    raise ValueError("INGESTION_DATE is required")

path = f"s3a://{bucket}/bronze/olist/order_reviews/ingestion_date={ing}/"
df = spark.read.parquet(path)

print("PATH =", path)
print("PARQUET_COUNT =", df.count())
df.printSchema()

spark.stop()
