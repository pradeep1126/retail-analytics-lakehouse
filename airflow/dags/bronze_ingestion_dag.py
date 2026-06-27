from datetime import datetime

from airflow.operators.bash import BashOperator

from airflow import DAG

with DAG(
    dag_id="bronze_ingestion_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["retail", "bronze"],
) as dag:

    ingest_datasets = BashOperator(
        task_id="ingest_datasets",
        bash_command=(
            "cd /opt/retail-lakehouse && " "python spark/ingestion/ingest_dataset.py all"
        ),
    )

    ingest_datasets
