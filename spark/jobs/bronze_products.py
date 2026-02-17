from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os, sys

S3_BUCKET = os.environ.get("S3_BUCKET", "de-olist-thiago-dev")
RAW_PREFIX = "raw/olist"
BRONZE_PREFIX = "bronze/olist/products"

INGESTION_DATE = os.environ.get("INGESTION_DATE")
if not INGESTION_DATE:
    print("ERROR: set INGESTION_DATE (ex: 2026-02-04)")
    sys.exit(1)

spark = SparkSession.builder.appName("bronze_products").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw_path = f"s3a://{S3_BUCKET}/{RAW_PREFIX}/ingestion_date={INGESTION_DATE}/olist_products_dataset.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path)
)

df_clean = df.select(
    col("product_id").cast("string").alias("product_id"),
    col("product_category_name").cast("string").alias("product_category_name"),
    col("product_name_lenght").cast("int").alias("product_name_length"),
    col("product_description_lenght").cast("int").alias("product_description_length"),
    col("product_photos_qty").cast("int").alias("product_photos_qty"),
    col("product_weight_g").cast("int").alias("product_weight_g"),
    col("product_length_cm").cast("int").alias("product_length_cm"),
    col("product_height_cm").cast("int").alias("product_height_cm"),
    col("product_width_cm").cast("int").alias("product_width_cm"),
)

output_path = f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/ingestion_date={INGESTION_DATE}"

df_clean.write.mode("overwrite").parquet(output_path)
spark.stop()