import os
from pyspark.sql import SparkSession

S3_BUCKET = os.environ.get("S3_BUCKET", "de-olist-thiago-dev")
INGESTION_DATE = os.environ.get("INGESTION_DATE", "2026-02-04")

BASE = f"s3a://{S3_BUCKET}/bronze/olist/orders/ingestion_date={INGESTION_DATE}/"
QUAR = f"s3a://{S3_BUCKET}/bronze/olist/_quarantine/orders/ingestion_date={INGESTION_DATE}/"

spark = SparkSession.builder.appName("quarantine_bad_parquet_orders_hdfs").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

jvm = spark._jvm
conf = spark._jsc.hadoopConfiguration()
fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)

Path = jvm.org.apache.hadoop.fs.Path

base_path = Path(BASE)

# Lista recursiva
it = fs.listFiles(base_path, True)

parquets = []
while it.hasNext():
    st = it.next()
    p = st.getPath().toString()
    if p.endswith(".parquet"):
        parquets.append(p)

print(f"[INFO] Found {len(parquets)} parquet files under {BASE}")

good, bad, failed = 0, 0, 0

for p in parquets:
    try:
        df = spark.read.parquet(p)
        ncols = len(df.columns)
        if ncols == 11:
            good += 1
            continue

        # Qualquer coisa != 11 vai pra quarentena (inclui 9)
        bad += 1
        rel = p.split(f"/bronze/olist/orders/ingestion_date={INGESTION_DATE}/", 1)[1]
        dst = QUAR + rel

        src_path = Path(p)
        dst_path = Path(dst)

        # garante "diretório" pai
        parent = dst_path.getParent()
        fs.mkdirs(parent)

        ok = fs.rename(src_path, dst_path)
        print(f"[MOVE] ncols={ncols} {p} -> {dst} (ok={ok})")

    except Exception as e:
        failed += 1
        print(f"[ERROR] reading/moving {p}: {e}")

print(f"[SUMMARY] good={good} bad_moved={bad} failed={failed}")
spark.stop()
