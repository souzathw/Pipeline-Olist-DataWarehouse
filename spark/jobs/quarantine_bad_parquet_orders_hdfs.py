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
Path = jvm.org.apache.hadoop.fs.Path

# IMPORTANT: define base_path BEFORE asking for its filesystem
base_path = Path(BASE)
fs = base_path.getFileSystem(conf)

# recursive iterator over files under BASE
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

        # expected schema for your Redshift table is 11 cols
        if ncols == 11:
            good += 1
            continue

        bad += 1

        # build destination path keeping the same relative structure
        rel = p.split(f"/bronze/olist/orders/ingestion_date={INGESTION_DATE}/", 1)[1]
        dst = QUAR + rel

        src_path = Path(p)
        dst_path = Path(dst)

        # ensure destination parent exists
        fs.mkdirs(dst_path.getParent())

        ok = fs.rename(src_path, dst_path)
        print(f"[MOVE] ncols={ncols} {p} -> {dst} (ok={ok})")

        if not ok:
            failed += 1
            print(f"[ERROR] rename returned ok=False for {p}")

    except Exception as e:
        failed += 1
        print(f"[ERROR] {p}: {e}")

print(f"[SUMMARY] good={good} bad_moved={bad} failed={failed}")
spark.stop()
