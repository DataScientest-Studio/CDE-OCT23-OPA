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
    'stop-spark-streaming',
    default_args=default_args,
    description='Un DAG pour arrêter le processus Spark streaming',
    schedule_interval=None,  # Vous pouvez le définir à None pour exécuter manuellement
    start_date=datetime(2024, 8, 31),
    catchup=False,
)

stop_spark_streaming = BashOperator(
    task_id='stop_spark_streaming',
    bash_command='bash -c "/opt/airflow/script/stop_spark_streaming.sh"',
    dag=dag
)
