from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="elt_pipeline",
    default_args=default_args,
    description="ELT pipeline: ingest to BigQuery raw, transform with dbt",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["elt", "bigquery", "dbt"],
) as dag:

    ingest_raw = BashOperator(
        task_id="ingest_raw_data",
        bash_command="python /opt/airflow/dags/../scripts/ingest.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --target prod",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . --target prod",
    )

    ingest_raw >> dbt_run >> dbt_test
