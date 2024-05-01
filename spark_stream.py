import logging
import uuid

from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
from cassandra.policies import RoundRobinPolicy
from cassandra.query import BatchStatement, ConsistencyLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType


def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
        """)
        logging.info("Keyspace created successfully")
    except Exception as e:
        logging.error(f"Error creating keyspace: {e}")


def create_table(session):
    try:
        session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.BTCUSDT (
            id bigint PRIMARY KEY,
            symbol TEXT,
            price FLOAT,
            quantity FLOAT,
            timestamp TIMESTAMP);
        """)
        logging.info("Table created successfully")
    except Exception as e:
        logging.error(f"Error creating table: {e}")


def insert_data(batch_df, epoch_id):
    logging.info("Inserting data...")
    # Initialisation de la connexion à Cassandra dans le try block
    try:
        # Création de la session Cassandra pour chaque batch
        cluster = Cluster(['192.168.1.35'], port=9042)
        session = cluster.connect('spark_streams')

        # Préparation de la requête d'insertion
        prepared_statement = session.prepare("""
        INSERT INTO BTCUSDT (id, symbol, price, quantity, timestamp)
        VALUES (?, ?, ?, ?, ?)
        """)
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)

        # Ajout des requêtes au batch
        for row in batch_df.collect():

            try:
                int_id = int(row.id)
            except ValueError:
                logging.error(f"Invalid ID format for ID: {row.id}")
                continue  # Passer à l'itération suivante si la conversion échoue

            batch.add(prepared_statement, (int_id, row.symbol, row.price, row.qty, row.time))

        # Exécution du batch
        session.execute(batch)
        print(f"Batch {epoch_id} inserted successfully")
    except Exception as e:
        logging.error(f"Could not insert batch {epoch_id}: {str(e)}")

    finally:
        # Fermeture de la session Cassandra après l'insertion
        if 'session' in locals():
            session.shutdown()


def create_spark_session():
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .config('spark.cassandra.connection.host', '192.168.1.35') \
            .config('spark.cassandra.connection.port', '9042') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception {e}")
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
            .option('subscribe', 'binance_streaming') \
            .option('startingOffsets', 'earliest') \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")

        logging.info("Kafka dataframe created successfully")
        return df_spark
    except Exception as e:
        logging.error(f"Kafka dataframe could not be created because: {e}")
        return None


def create_cassandra_connection():
    try:
        # Adresse IP du cluster Cassandra
        contact_point = "192.168.1.35"
        # Création de la politique de répartition de charge RoundRobinPolicy
        load_balancing_policy = RoundRobinPolicy()
        # Création de la politique de répartition de charge en spécifiant le datacenter local
        load_balancing_policy = DCAwareRoundRobinPolicy(
            local_dc='datacenter1')  # Remplacez 'datacenter1' par votre datacenter local
        # Connexion au cluster Cassandra avec la politique spécifiée
        cluster = Cluster(
            [contact_point],
            load_balancing_policy=load_balancing_policy,
            protocol_version=5
        )

        cass_session = cluster.connect()
        return cass_session
    except Exception as e:
        logging.error(f"Couldn't create the Cassandra connection due to {e}")
        return None


def main():
    logging.basicConfig(level=logging.INFO)
    # Initialisation de la session Spark
    spark = create_spark_session()

    if spark:
        spark_df = kafka_connect(spark)
        if spark_df:
            session = create_cassandra_connection()

            if session:
                create_keyspace(session)
                create_table(session)

                # Gérer le streaming et insérer les données dans Cassandra
                query = spark_df.writeStream.foreachBatch(insert_data).start()
                query.awaitTermination()


if __name__ == "__main__":
    main()