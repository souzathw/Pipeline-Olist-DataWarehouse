from __future__ import annotations

import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

import boto3
from airflow import DAG
from airflow.decorators import task

KAGGLE_DATASET = "olistbr/brazilian-ecommerce"

EXPECTED_FILES = {
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
}

def _s3_client():
    return boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))

def _ensure_kaggle_env():
    os.environ.setdefault("KAGGLE_CONFIG_DIR", "/home/airflow/.kaggle")

def _list_s3_filenames(s3, bucket: str, prefix: str) -> set[str]:
    filenames: set[str] = set()
    token = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            filenames.add(key.split("/")[-1])

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return filenames

with DAG(
    dag_id="olist_extract_to_s3_raw",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "extract", "kaggle", "s3", "raw"],
) as dag:

    @task
    def extract_and_upload():
        bucket = os.environ["S3_BUCKET"]
        ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")

        _ensure_kaggle_env()

        s3 = _s3_client()
        prefix = f"raw/olist/ingestion_date={ingestion_date}/"

        existing = _list_s3_filenames(s3, bucket, prefix)
        if EXPECTED_FILES.issubset(existing):
            print(f"Skip: arquivos já existem no S3 para ingestion_date={ingestion_date}")
            return [f"s3://{bucket}/{prefix}{name}" for name in sorted(EXPECTED_FILES)]

        workdir = Path("/tmp/olist_kaggle")
        if workdir.exists():
            shutil.rmtree(workdir)
        workdir.mkdir(parents=True, exist_ok=True)

        cmd = [
            "kaggle", "datasets", "download",
            "-d", KAGGLE_DATASET,
            "-p", str(workdir),
            "--force",
            "--unzip",
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=900,
        )

        print("Kaggle stdout:\n", result.stdout)
        print("Kaggle stderr:\n", result.stderr)

        if result.returncode != 0:
            raise RuntimeError(f"Kaggle download failed with code {result.returncode}")

        csv_files = sorted(workdir.rglob("*.csv"))
        if not csv_files:
            raise RuntimeError(
                "Nenhum CSV encontrado após download/unzip. "
                "Verifique credenciais Kaggle e o conteúdo do dataset."
            )

        found_names = {p.name for p in csv_files}
        missing = EXPECTED_FILES - found_names
        if missing:
            raise RuntimeError(
                "Dataset baixado, mas faltam arquivos esperados: "
                + ", ".join(sorted(missing))
            )

        uploaded = []
        for csv_file in csv_files:
         
            if csv_file.name not in EXPECTED_FILES:
                continue

            key = f"{prefix}{csv_file.name}"
            s3.upload_file(str(csv_file), bucket, key)
            uploaded.append(f"s3://{bucket}/{key}")

        print("Uploaded files:", len(uploaded))
        return uploaded

    extract_and_upload()
