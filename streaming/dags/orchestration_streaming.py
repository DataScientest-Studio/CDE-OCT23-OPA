from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'binance-streaming-orchestration',
    default_args=default_args,
    max_active_runs=3,
    description='A simple streaming pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 8, 30),
    catchup=False,
)

start_streaming_job = BashOperator(
    task_id='start_streaming_job',
    bash_command='bash -c "/opt/airflow/script/start_streaming_job.sh"',
    dag=dag
)

monitor_streaming_job = BashOperator(
    task_id='monitor_streaming_job',
    bash_command='bash -c "/opt/airflow/script/monitor_streaming_job.sh"',
    dag=dag
)

stop_streaming_job = BashOperator(
    task_id='stop_streaming_job',
    bash_command='bash -c "/opt/airflow/script/stop_streaming_job.sh"',
    dag=dag
)

start_streaming_job >> monitor_streaming_job >> stop_streaming_job
