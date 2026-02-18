from __future__ import annotations

import os
from datetime import datetime

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise ValueError(f"Missing env var: {name}")
    return v


def _s3_client():
    return boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))


def _latest_ingestion_date(bucket: str) -> str:
    s3 = _s3_client()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix="raw/olist/", Delimiter="/")
    prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    dates = []
    for pref in prefixes:
        if "ingestion_date=" in pref:
            dates.append(pref.split("ingestion_date=")[-1].strip("/"))
    if not dates:
        raise RuntimeError(f"No ingestion_date prefixes found in s3://{bucket}/raw/olist/")
    dates.sort()
    return dates[-1]


with DAG(
    dag_id="olist_load_redshift_order_payments",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "redshift", "staging", "payments"],
) as dag:

    @task
    def resolve_ingestion_date() -> str:
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
        conf_date = conf.get("ingestion_date")

        bucket = _get_env("S3_BUCKET")
        return conf_date or _latest_ingestion_date(bucket)

    @task
    def load(ingestion_date: str) -> dict:
        bucket = _get_env("S3_BUCKET")
        role_arn = _get_env("REDSHIFT_IAM_ROLE_ARN")
        s3_path = f"s3://{bucket}/bronze/olist/order_payments/ingestion_date={ingestion_date}/"

        sql = f"""
        truncate table stg.order_payments;

        copy stg.order_payments (
          order_id,
          payment_sequential,
          payment_type,
          payment_installments,
          payment_value
        )
        from '{s3_path}'
        iam_role '{role_arn}'
        format as parquet;

        update stg.order_payments
        set ingestion_date = date '{ingestion_date}';
        """

        hook = PostgresHook(postgres_conn_id="redshift_default")
        hook.run(sql)

        rows = hook.get_first("select count(*) from stg.order_payments;")[0]
        return {"ingestion_date": ingestion_date, "s3_path": s3_path, "rows_loaded": int(rows)}

    d = resolve_ingestion_date()
    load(d)