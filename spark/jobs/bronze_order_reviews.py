import os
from pyspark.sql import SparkSession, functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("bronze_order_reviews")
        # garante saída de timestamp como MICROS (INT64) e não INT96
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )

    # IMPORTANTE: session timezone pode afetar parsing; deixamos UTC para ser determinístico
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    bucket = os.getenv("S3_BUCKET", "de-olist-thiago-dev")
    ingestion_date = os.getenv("INGESTION_DATE")
    if not ingestion_date:
        raise ValueError("INGESTION_DATE is required (YYYY-MM-DD)")

    raw_path = (
        f"s3a://{bucket}/raw/olist/"
        f"ingestion_date={ingestion_date}/"
        f"olist_order_reviews_dataset.csv"
    )

    bronze_path = (
        f"s3a://{bucket}/bronze/olist/"
        f"order_reviews/ingestion_date={ingestion_date}/"
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(raw_path)
    )

    def clean_str(col):
        return F.when(F.trim(F.col(col)) == "", F.lit(None)).otherwise(F.trim(F.col(col)))

    # ⚠️ CHAVE DA CORREÇÃO:
    # usar timestamp_ntz para o Parquet sair como TIMESTAMP(MICROS,false)
    # (Spectrum costuma falhar com TIMESTAMP(MICROS,true))
    df = (
        df
        .withColumn("review_id", clean_str("review_id"))
        .withColumn("order_id", clean_str("order_id"))
        .withColumn("review_comment_title", clean_str("review_comment_title"))
        .withColumn("review_comment_message", clean_str("review_comment_message"))
        .withColumn("review_score", F.col("review_score").cast("int"))
        # cria como NTZ (sem timezone)
        .withColumn("review_creation_ts", F.expr("to_timestamp_ntz(review_creation_date)"))
        .withColumn("review_answer_ts", F.expr("to_timestamp_ntz(review_answer_timestamp)"))
        .withColumn("ingestion_date", F.to_date(F.lit(ingestion_date)))
        .select(
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_ts",
            "review_answer_ts",
            "ingestion_date",
        )
    )

    (
        df.write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    spark.stop()
    print(f"OK: wrote {bronze_path}")


if __name__ == "__main__":
    main()