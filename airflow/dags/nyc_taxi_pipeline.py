from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

# Resolved at parse time inside the container; both paths are bind-mounted via
# docker-compose.yml.  Defining them once avoids silent typos across tasks.
SCRIPTS_DIR = "/opt/airflow/scripts"
DBT_DIR = "/opt/airflow/dbt"

# dbt reads GCP_PROJECT_ID and GOOGLE_APPLICATION_CREDENTIALS from the
# container environment (set in docker-compose.yml / .env), so no extra
# env injection is needed here.
DBT_TARGET = os.environ.get("DBT_TARGET", "prod")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    description=(
        "Daily ELT pipeline: download NYC Yellow Taxi data → "
        "load to BigQuery raw → dbt run → dbt test"
    ),
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,          # prevent overlapping runs if a day falls behind
    tags=["nyc_taxi", "bigquery", "dbt", "elt"],
) as dag:

    ingest_raw_data = BashOperator(
        task_id="ingest_raw_data",
        bash_command=f"python {SCRIPTS_DIR}/ingest_to_bigquery.py",
        # Pass logical date so the script can be extended to partition by date.
        env={
            **{k: os.environ[k] for k in ("GCP_PROJECT_ID", "GOOGLE_APPLICATION_CREDENTIALS") if k in os.environ},
            "LOGICAL_DATE": "{{ ds }}",
        },
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --profiles-dir . --target {DBT_TARGET} --no-partial-parse"
        ),
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test --profiles-dir . --target {DBT_TARGET} --no-partial-parse"
        ),
    )

    ingest_raw_data >> run_dbt_models >> run_dbt_tests
