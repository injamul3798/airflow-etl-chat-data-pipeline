from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.etl import run_etl


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
}


with DAG(
    dag_id="chat_pipeline",
    description="Daily ETL pipeline for chat support analytics",
    default_args=default_args,
    start_date=datetime(2026, 3, 17),
    schedule="@daily",
    catchup=False,
    tags=["etl", "analytics", "chat"],
) as dag:
    etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl,
    )
