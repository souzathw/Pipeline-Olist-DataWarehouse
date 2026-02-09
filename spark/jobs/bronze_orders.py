from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, lit
import os
import sys


def main():
    S3_BUCKET = os.environ.get("S3_BUCKET", "de-olist-thiago-dev")
    RAW_PREFIX = "raw/olist"
    BRONZE_PREFIX = "bronze/olist/orders"

    INGESTION_DATE = os.environ.get("INGESTION_DATE")
    if not INGESTION_DATE:
        print("ERROR: Defina INGESTION_DATE=YYYY-MM-DD")
        sys.exit(1)

    spark = (
        SparkSession.builder
        .appName("bronze_orders")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_path = (
        f"s3a://{S3_BUCKET}/{RAW_PREFIX}/"
        f"ingestion_date={INGESTION_DATE}/"
        f"olist_orders_dataset.csv"
    )

    output_path = (
        f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/"
        f"ingestion_date={INGESTION_DATE}"
    )

    print("========== BRONZE ORDERS ==========")
    print(f"[INFO] S3_BUCKET={S3_BUCKET}")
    print(f"[INFO] RAW_PREFIX={RAW_PREFIX}")
    print(f"[INFO] BRONZE_PREFIX={BRONZE_PREFIX}")
    print(f"[INFO] INGESTION_DATE={INGESTION_DATE}")
    print(f"[INFO] raw_path={raw_path}")
    print(f"[INFO] output_path={output_path}")
    print("===================================")

    # 1) Leitura
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(raw_path)
    )

    # Força ação para você VER que leu (Spark é lazy)
    rows_raw = df.count()
    print(f"[INFO] Read OK. rows_raw={rows_raw} cols={len(df.columns)}")
    df.show(5, truncate=False)

    # 2) Parsing timestamps (mantém sua lógica)
    df_clean = (
        df
        .withColumn("order_purchase_timestamp",
                    to_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_approved_at",
                    to_timestamp("order_approved_at", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_delivered_carrier_date",
                    to_timestamp("order_delivered_carrier_date", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_delivered_customer_date",
                    to_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_estimated_delivery_date",
                    to_timestamp("order_estimated_delivery_date", "yyyy-MM-dd HH:mm:ss"))
    )

    # 3) Filtro
    df_clean = df_clean.filter(col("order_purchase_timestamp").isNotNull())

    # 4) Partições + metadado ingestão
    df_clean = (
        df_clean
        .withColumn("year", year(col("order_purchase_timestamp")))
        .withColumn("month", month(col("order_purchase_timestamp")))
        .withColumn("ingestion_date", lit(INGESTION_DATE))
    )

    rows_clean = df_clean.count()
    print(f"[INFO] Clean OK. rows_clean={rows_clean} (dropped={rows_raw - rows_clean})")

    # 5) Escrita Bronze
    print("[INFO] Writing parquet (overwrite) partitionBy(year, month)...")
    (
        df_clean
        .repartition(col("year"), col("month"))   
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(output_path)
    )
    print("[INFO] Write done.")

    spark.stop()
    print("[INFO] Spark stopped successfully.")


if __name__ == "__main__":
    main()
