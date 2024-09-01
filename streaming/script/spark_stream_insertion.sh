#!/bin/bash

# Ensure the Spark master URL is accessible from Airflow
SPARK_MASTER_URL=spark://spark-master:7077

# Run the Spark job using spark-submit, connecting to the Spark master
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --master $SPARK_MASTER_URL \
  /opt/airflow/script/spark_stream.py