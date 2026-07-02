from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "numan",
    "retries": 1,
}

with DAG(
    dag_id="logistics_streaming_batch_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["logistics", "spark", "delta"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="cd /opt/airflow && python spark/bronze_to_silver.py",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold_star_schema",
        bash_command="cd /opt/airflow && python spark/silver_to_gold_star_schema.py",
    )

    gold_kpi_marts = BashOperator(
        task_id="gold_kpi_marts",
        bash_command="cd /opt/airflow && python spark/gold_kpi_marts.py",
    )

    bronze_to_silver >> silver_to_gold >> gold_kpi_marts