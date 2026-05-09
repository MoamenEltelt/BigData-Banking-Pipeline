from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "moamen",
    "start_date": datetime(2025, 1, 1)
}

with DAG(
    dag_id="bank_data_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    run_spark_job = BashOperator(
        task_id="run_spark_analysis",
        bash_command="python /spark_jobs/spark_analysis.py"
    )

    run_spark_job