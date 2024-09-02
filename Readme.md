## Requirements

pip install -r requirements.txt

## Docker

docker-compose up -d

## Uvicorn

ec~~ho 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc~~


uvicorn api:api --reload

## Streamlit

streamlit run streamlit_app/app.py


## Streaming

### Step 1: Clone the Repository

To start, clone the repository to your local machine:

```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo
```
### Step 2: Start Services with Docker Compose

To start all necessary services (Kafka, Cassandra, Spark, and Airflow), use Docker Compose:

```bash
docker-compose up -d
```
### Step 3: Airflow Setup

To set up Airflow with the necessary configurations:

- Place the `spark_streaming_dag.py` file in the Airflow DAGs directory.
- Ensure Kafka and Cassandra connections are properly configured in Airflow.
- Add a new Spark connection in Airflow:

1. Go to **Admin** > **Connections**.
2. Click on **Add a new record**.
3. Fill in the details:

   - **Connection ID:** `spark_default`
   - **Connection Type:** `Spark`
   - **Host:** `spark://spark-master:7077`
   - **Extra:**

   ```json
   {
     "master": "spark://spark-master:7077",
     "deploy-mode": "client",
     "spark-home": "/home/airflow/.local"
   }
   ```
### Step 4: Run the Streaming Job

To execute the streaming job:

- Go to the Airflow web interface.
- Locate the `spark_streaming_dag` and `binance-streaming-orchestration`.
- Toggle the DAG to **On** to enable scheduling.
- Alternatively, trigger the DAG manually by clicking on the play button next to the DAG name.

You can also run the Spark job manually using `spark-submit` if needed.

Actually you will need to run manually the script spark_stream inside pycharm or another ide to process the insertion 
into cassandra.

### Common Issues and Solutions

**Connectivity Issues:** Ensure that all services (Kafka, Spark, Cassandra) are properly running and accessible. Use tools like `ping` and `telnet` to check connectivity.

**Schema Mismatches:** Verify that the Kafka topic schema matches the schema expected by Spark.

**Resource Constraints:** Adjust the Spark executor and driver memory settings based on your environment.

**Logging:** Review the logs in Airflow and Spark to identify errors. Use the Airflow UI or access logs directly from the containers.


### Future Enhancements

- **Add Unit Tests:** Incorporate unit testing for Spark jobs and data validation.
- **Improve Scalability:** Optimize Spark configurations for larger datasets.
- **Integrate Monitoring Dashboards:** Use tools like Grafana or Prometheus for monitoring cluster and job performance.

### License

This project is licensed by DataScientest and us.


