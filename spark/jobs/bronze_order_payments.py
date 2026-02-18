from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os, sys

S3_BUCKET = os.environ.get("S3_BUCKET", "de-olist-thiago-dev")
RAW_PREFIX = "raw/olist"
BRONZE_PREFIX = "bronze/olist/order_payments"

INGESTION_DATE = os.environ.get("INGESTION_DATE")
if not INGESTION_DATE:
    print("ERROR: set INGESTION_DATE (ex: 2026-02-04)")
    sys.exit(1)

spark = SparkSession.builder.appName("bronze_order_payments").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw_path = f"s3a://{S3_BUCKET}/{RAW_PREFIX}/ingestion_date={INGESTION_DATE}/olist_order_payments_dataset.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path)
)

df_clean = (
    df.select(
        col("order_id").cast("string").alias("order_id"),
        col("payment_sequential").cast("int").alias("payment_sequential"),
        col("payment_type").cast("string").alias("payment_type"),
        col("payment_installments").cast("int").alias("payment_installments"),
        col("payment_value").cast("double").alias("payment_value"),
    )
)

output_path = f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/ingestion_date={INGESTION_DATE}"
df_clean.write.mode("overwrite").parquet(output_path)

spark.stop()