import os
from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
        .appName("inspect_parquet_order_reviews")
        .getOrCreate()
    )

    bucket = os.getenv("S3_BUCKET", "de-olist-thiago-dev")
    ingestion_date = os.getenv("INGESTION_DATE")
    if not ingestion_date:
        raise ValueError("INGESTION_DATE is required (YYYY-MM-DD)")

    path = f"s3a://{bucket}/bronze/olist/order_reviews/ingestion_date={ingestion_date}/"

    df = spark.read.parquet(path)

    df.printSchema()
    print("cols:", df.columns)
    print("count:", df.count())
    df.show(5, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()