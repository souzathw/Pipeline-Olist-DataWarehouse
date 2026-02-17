from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import os, sys

S3_BUCKET = os.environ.get("S3_BUCKET", "de-olist-thiago-dev")
RAW_PREFIX = "raw/olist"
BRONZE_PREFIX = "bronze/olist/order_items"

INGESTION_DATE = os.environ.get("INGESTION_DATE")
if not INGESTION_DATE:
    print("ERROR: set INGESTION_DATE (ex: 2026-02-04)")
    sys.exit(1)

spark = SparkSession.builder.appName("bronze_order_items").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw_path = f"s3a://{S3_BUCKET}/{RAW_PREFIX}/ingestion_date={INGESTION_DATE}/olist_order_items_dataset.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path)
)

df_clean = (
    df.select(
        col("order_id").cast("string").alias("order_id"),
        col("order_item_id").cast("int").alias("order_item_id"),
        col("product_id").cast("string").alias("product_id"),
        col("seller_id").cast("string").alias("seller_id"),
        to_timestamp(col("shipping_limit_date")).alias("shipping_limit_date"),
        col("price").cast("double").alias("price"),
        col("freight_value").cast("double").alias("freight_value"),
    )
)

output_path = f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/ingestion_date={INGESTION_DATE}"
df_clean.write.mode("overwrite").parquet(output_path)

spark.stop()