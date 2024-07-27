import json
import datetime
import threading
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import websocket

default_args = {
    'owner': 'opa',
    'start_date': datetime.datetime(2024, 4, 21, 19, 00),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=30)
}

ping_pong_counter = 0


def on_error(wsapp, error):
    print(f"WebSocket error: {error}")


def on_open(wsapp):
    print('WebSocket connection opened')


def on_ping(wsapp, message):
    print("Ping received from server.")
    wsapp.send(json.dumps({"pong": int(time.time() * 1000)}))


def on_pong(wsapp, message):
    global ping_pong_counter
    ping_pong_counter = 0
    print("Pong received, counter reset.")


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
                                           on_error=on_error,
                                           on_ping=on_ping,
                                           on_pong=on_pong)
            wsapp.run_forever(ping_interval=60, ping_timeout=10)
        except Exception as e:
            print(f"WebSocket connection error: {e}")
            time.sleep(10)


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
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
        print("Kafka producer created successfully.")
        print(f"Sending message to Kafka topic: {json.dumps(trade)}")
        producer.send('binance_streaming', json.dumps(trade).encode('utf-8'))
        print("Message sent successfully.")
    except Exception as e:
        print(f"Error publishing to Kafka: {e}")


with DAG('binance-streaming-automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=binance_trades,
        retries=0
    )

streaming_task
