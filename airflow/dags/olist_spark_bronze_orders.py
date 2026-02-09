from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="olist_spark_bronze_orders",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "spark", "bronze"],
) as dag:

    run_bronze_orders = BashOperator(
    task_id="run_bronze_orders",
    bash_command="""
set -euo pipefail

INGESTION_DATE="{{ ds }}"

docker exec dw-spark-client bash -lc '
  export INGESTION_DATE="'${INGESTION_DATE}'" &&
  spark-submit \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
    --conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com \
    /work/spark/jobs/bronze_orders.py
'
""",
)


    run_bronze_orders
