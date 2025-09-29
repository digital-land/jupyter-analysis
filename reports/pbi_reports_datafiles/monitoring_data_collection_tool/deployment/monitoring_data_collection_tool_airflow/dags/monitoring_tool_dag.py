from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'daniel',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['you@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='monitoring_data_collection_tool',
    default_args=default_args,
    description='Run monitoring tool and upload to SharePoint',
    schedule_interval='30 13 * * *',  # Runs every day at 13:30
    start_date=datetime(2025, 6, 25),
    catchup=False,
    tags=['odp', 'monitoring'],
) as dag:

    run_monitoring_tool = BashOperator(
        task_id='run_tool',
        bash_command='python /opt/airflow/monitoring_data_collection_tool/run_tool.py'
    )
