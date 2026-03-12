import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

TZ = "America/Sao_Paulo"

DBT_CONTAINER = "dw-dbt-runner"
DBT_PROJECT_DIR = "/app"
DBT_SELECT = "fact_orders fact_order_reviews fact_order_payments"

default_args = {
    "owner": "thiago",
    "retries": 0,
}

with DAG(
    dag_id="olist_dbt_build_dw",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 2, 1, tz=TZ),
    schedule=None,
    catchup=False,
    tags=["olist", "dbt", "dw"],
) as dag:

    dbt_build = BashOperator(
        task_id="dbt_build_incremental_facts",
        bash_command=(
            "set -euo pipefail; "
            "ING_DATE='{{ dag_run.conf.get(\"ingestion_date\", ds) }}'; "
            "echo \"Running dbt build on container {c} with ingestion_date=${{ING_DATE}}\"; "
            "docker exec -i {c} bash -lc "
            "\"cd {p} && "
            "dbt build "
            "--select {s} "
            "--vars '{{\\\"ingestion_date\\\": \\\"${{ING_DATE}}\\\"}}'\""
        ).format(c=DBT_CONTAINER, p=DBT_PROJECT_DIR, s=DBT_SELECT),
    )

    dbt_build