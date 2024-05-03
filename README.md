Streaming de données en temps réel avec Apache Spark, Apache Kafka et Cassandra

Ce projet implémente un processus de streaming de données en temps réel en utilisant Apache Spark pour la manipulation des données, Apache Kafka comme source de données de streaming et Cassandra comme base de données pour le stockage des données.

Fonctionnalités:

Lecture des données à partir d'un flux Kafka en temps réel.
Traitement des données en utilisant Apache Spark.
Stockage des données dans Cassandra pour l'analyse ultérieure.
Configuration requise
Apache Spark
Apache Kafka
Cassandra
Python

Installation et exécution:


1. Assurez-vous d'avoir Apache Spark, Apache Kafka et Cassandra installés et configurés correctement sur votre système.

2. Clonez ce dépôt sur votre machine locale.

3. Assurez-vous d'avoir les dépendances Python installées en exécutant pip install -r requirements.txt.

4. Assurez-vous de démarrer votre serveur Kafka et Cassandra.

5. Modifiez l'adresse IP de Cassandra dans le fichier spark_stream.py pour correspondre à votre configuration.

6. Exécutez le script Python spark_stream.py pour démarrer le processus de streaming.

7. Pour le script kafka_stream.py, assurez-vous d'avoir les dépendances requises installées en exécutant pip install kafka-python websocket-client.

8. Modifiez l'adresse IP du broker Kafka dans le fichier kafka_stream.py pour correspondre à votre configuration.

9. Exécutez le script Python kafka_stream.py pour démarrer le streaming de données via WebSocket et les publier dans le service Kafka.


Contributions
Ce projet a été développé dans le cadre d'une formation chez DataScientest. Les contributions externes ne sont pas actuellement acceptées.

Licence
Ce projet est sous licence privée et réservée à un usage éducatif chez DataScientest.
