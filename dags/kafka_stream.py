import json
import datetime
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import websocket

default_args = {
    'owner': 'opa',
    'start_date': datetime.datetime(2024, 4, 21, 19, 00),
    'retries': 3,  # Nombre de tentatives en cas d'échec
    'retry_delay': datetime.timedelta(seconds=30)  # Délai entre les tentatives en cas d'échec
}


def on_error(wsapp, error):
    print(f"WebSocket error: {error}")


def on_open(wsapp):
    print('WebSocket connection opened')


def binance_trades():
    socket = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade'

    def on_message(wsapp, message):
        try:
            json_message = json.loads(message)
            trade = handle_trades(json_message)
            stream_data(trade)
        except Exception as e:
            print(f"Error processing message: {e}")

    while True:
        try:
            wsapp = websocket.WebSocketApp(socket,
                                           on_open=on_open,
                                           on_message=on_message,
                                           on_error=on_error)
            wsapp.run_forever()
        except Exception as e:
            print(f"WebSocket connection error: {e}")
            time.sleep(10)  # Attendre avant de réessayer en cas d'échec


def handle_trades(json_message):
    date_time = datetime.datetime.fromtimestamp(json_message['E'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    trades = {
        "id": json_message['a'],
        "symbol": json_message['s'],
        "price": float(json_message['p']),
        "qty": float(json_message['q']),
        "time": str(date_time)
    }
    return trades


def stream_data(trade):
    try:
        print("Creating Kafka producer...")
        producer = KafkaProducer(bootstrap_servers=['broker:29092'],
                                 max_block_ms=5000)
        print("Kafka producer created successfully.")

        print("Sending message to Kafka topic...")
        producer.send('binance_streaming', json.dumps(trade).encode('utf-8'))
        print("Message sent successfully.")
    except Exception as e:
        print(f"Error publishing to Kafka: {e}")


with DAG('binance-streaming-automation',
         default_args=default_args,
         schedule_interval='@daily',  # Exécuter le DAG une fois par jour
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=binance_trades,
        retries=0  # Désactiver les tentatives de réessai car la boucle est déjà gérée
    )

streaming_task
