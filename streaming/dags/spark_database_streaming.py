from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='spark_streaming_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

submit_spark_job = SparkSubmitOperator(
    task_id='submit_spark_job',
    conn_id='spark_default',  # Use the correct connection ID
    application='/opt/airflow/script/spark_stream.py',  # Path to your spark application script
    total_executor_cores='2',  # Example value
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0',
    executor_cores='1',
    executor_memory='4g',
    name='spark_streaming_job',
    verbose=True,
    dag=dag
)

submit_spark_job
