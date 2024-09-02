import logging
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement, ConsistencyLevel
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, exists
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType


def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};
        """)
        logging.info("Keyspace created successfully")
    except Exception as e:
        logging.exception("Error creating keyspace")


def create_table(session):
    try:
        session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.BTCUSDT (
            symbol TEXT,
            timestamp TIMESTAMP,
            id bigint,
            price FLOAT,
            quantity FLOAT,
            PRIMARY KEY ((symbol, timestamp), id)
        ) WITH CLUSTERING ORDER BY (id ASC);
        """)
        logging.info("Table created successfully")
    except Exception as e:
        logging.exception("Error creating table")


def insert_data(batch_df, epoch_id):
    logging.info(f"Inserting data for batch {epoch_id}...")
    try:
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect('spark_streams')

        # Prepared statement to insert data into BTCUSDT
        prepared_statement = session.prepare("""
        INSERT INTO BTCUSDT (symbol, timestamp, id, price, quantity)
        VALUES (?, ?, ?, ?, ?)
        """)

        for row in batch_df.collect():
            logging.info(f"Processing row with id: {row.id}...")

            # Ensure 'id' is an integer
            row_id = int(row.id)  # Convert 'id' to integer

            try:
                session.execute(prepared_statement,
                                (row.symbol, row.time, row_id, row.price, row.qty))
                logging.info(f"Inserted row: {row}")
            except Exception as e:
                logging.error(f"Failed to insert row: {row}. Error: {e}")

    except Exception as e:
        logging.exception(f"Could not insert batch {epoch_id}")
    finally:
        if 'session' in locals():
            session.shutdown()
        if 'cluster' in locals():
            cluster.shutdown()


def create_spark_session():
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .config('spark.cassandra.connection.port', '9042') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        logging.exception("Couldn't create the Spark session")
        return None


def kafka_connect(spark_conn):
    try:
        schema = StructType([
            StructField('id', StringType(), False),
            StructField('symbol', StringType(), False),
            StructField('price', FloatType(), False),
            StructField('qty', FloatType(), False),
            StructField('time', TimestampType(), False)
        ])

        df_spark = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'binance_trades') \
            .option('startingOffsets', 'earliest') \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")

        logging.info("Kafka dataframe created successfully")
        return df_spark
    except Exception as e:
        logging.exception("Kafka dataframe could not be created")
        return None


def create_cassandra_connection():
    try:
        contact_point = "localhost"  # Use direct IP address of Cassandra container
        load_balancing_policy = DCAwareRoundRobinPolicy(local_dc='dc1')
        logging.info(f"Attempting to connect to Cassandra at {contact_point} on port 9042")

        cluster = Cluster(
            [contact_point],
            load_balancing_policy=load_balancing_policy,
            protocol_version=4
        )
        cass_session = cluster.connect()
        logging.info("Cassandra connection created successfully!")
        return cluster, cass_session  # Return both cluster and session
    except Exception as e:
        logging.exception("Couldn't create the Cassandra connection")
        return None, None


def main():
    logging.basicConfig(level=logging.INFO)
    spark = create_spark_session()

    if spark:
        spark_df = kafka_connect(spark)
        if spark_df:
            session = create_cassandra_connection()

            if session:
                create_keyspace(session)
                create_table(session)

                query = spark_df.writeStream.foreachBatch(insert_data).start()
                query.awaitTermination()

    if 'spark' in locals():
        spark.stop()


if __name__ == "__main__":
    main()