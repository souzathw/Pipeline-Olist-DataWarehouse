from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import boto3


DAG_ID = "olist_load_redshift_orders"


def _get_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise ValueError(f"Missing env var: {name}")
    return v


def _s3_client():
    return boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))


def _latest_ingestion_date_in_s3(bucket: str) -> str:
    s3 = _s3_client()
    resp = s3.list_objects_v2(
        Bucket=bucket,
        Prefix="raw/olist/",
        Delimiter="/",
    )
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
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,  
    catchup=False,
    tags=["olist", "redshift", "staging", "load"],
) as dag:

    @task
    def resolve_ingestion_date() -> str:
        from airflow.operators.python import get_current_context

        ctx = get_current_context()
        conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
        conf_date = conf.get("ingestion_date")

        bucket = _get_env("S3_BUCKET")

        if conf_date:
            return conf_date

        return _latest_ingestion_date_in_s3(bucket)

    @task
    def load_orders_to_redshift(ingestion_date: str) -> dict:
        bucket = _get_env("S3_BUCKET")
        role_arn = _get_env("REDSHIFT_IAM_ROLE_ARN")

        s3_path = f"s3://{bucket}/bronze/olist/orders/ingestion_date={ingestion_date}/"

        sql = f"""
        -- 1) truncate
        truncate table stg.orders;

        -- 2) copy só com as 9 colunas do parquet
        copy stg.orders (
          order_id,
          customer_id,
          order_status,
          order_purchase_timestamp,
          order_approved_at,
          order_delivered_carrier_date,
          order_delivered_customer_date,
          order_estimated_delivery_date,
          ingestion_date
        )
        from '{s3_path}'
        iam_role '{role_arn}'
        format as parquet;

        -- 3) derivar year/month no Redshift
        update stg.orders
        set
          year  = extract(year  from order_purchase_timestamp),
          month = extract(month from order_purchase_timestamp)
        where year is null or month is null;
        """

        hook = PostgresHook(postgres_conn_id="redshift_default")
        hook.run(sql)

        rows = hook.get_first("select count(*) from stg.orders;")[0]
        nulls = hook.get_first("""
            select
              sum(case when year is null then 1 else 0 end) as year_nulls,
              sum(case when month is null then 1 else 0 end) as month_nulls
            from stg.orders;
        """)
        minmax = hook.get_first("""
            select min(order_purchase_timestamp), max(order_purchase_timestamp)
            from stg.orders;
        """)

        return {
            "ingestion_date": ingestion_date,
            "s3_path": s3_path,
            "rows_loaded": int(rows),
            "year_nulls": int(nulls[0]),
            "month_nulls": int(nulls[1]),
            "min_purchase_ts": str(minmax[0]),
            "max_purchase_ts": str(minmax[1]),
        }

    ingestion_date = resolve_ingestion_date()
    load_orders_to_redshift(ingestion_date)
