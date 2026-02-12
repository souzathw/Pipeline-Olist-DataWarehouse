from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os, sys

S3_BUCKET = os.environ.get("S3_BUCKET", "de-olist-thiago-dev")
RAW_PREFIX = "raw/olist"
BRONZE_PREFIX = "bronze/olist/customers"

INGESTION_DATE = os.environ.get("INGESTION_DATE")
if not INGESTION_DATE:
    print("ERROR: set INGESTION_DATE (ex: 2026-02-04)")
    sys.exit(1)

spark = SparkSession.builder.appName("bronze_customers").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw_path = f"s3a://{S3_BUCKET}/{RAW_PREFIX}/ingestion_date={INGESTION_DATE}/olist_customers_dataset.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path)
)

df_clean = (
    df
    .select(
        col("customer_id").cast("string").alias("customer_id"),
        col("customer_unique_id").cast("string").alias("customer_unique_id"),
        col("customer_zip_code_prefix").cast("int").alias("customer_zip_code_prefix"),
        col("customer_city").cast("string").alias("customer_city"),
        col("customer_state").cast("string").alias("customer_state"),
    )
)

output_path = f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/ingestion_date={INGESTION_DATE}"

(
    df_clean
    .write
    .mode("overwrite")
    .parquet(output_path)
)

spark.stop()
