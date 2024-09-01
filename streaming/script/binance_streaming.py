import datetime
import json
import logging
import time
import websocket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
KAFKA_BROKER = "broker:29092"  # Docker service name and port
KAFKA_TOPIC = "binance_trades"
PING_INTERVAL = 30  # Ping interval in seconds

# Function to create Kafka producer
def create_kafka_producer(retries=5, delay=5):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"Kafka producer created successfully.")
            return producer
        except NoBrokersAvailable:
            logger.error(f"Error connecting to Kafka (Attempt {attempt}/{retries}): NoBrokersAvailable")
            time.sleep(delay)
    raise Exception("Failed to connect to Kafka after multiple attempts.")

# Function to handle trade messages
def handle_trades(json_message):
    date_time = datetime.datetime.fromtimestamp(json_message['E'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    trades = {
        "id": json_message['t'],
        "symbol": json_message['s'],
        "price": float(json_message['p']),
        "qty": float(json_message['q']),
        "time": str(date_time)
    }
    return trades

# Function to send messages to Kafka
def send_message(producer, topic, message):
    try:
        producer.send(topic, message)
        producer.flush()
        logger.info(f"Message sent to Kafka topic '{topic}': {message}")
    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {e}")

# Callback function for WebSocket messages
def on_message(ws, message):
    """
    Callback function that is called when a message is received from the WebSocket.
    """
    json_message = json.loads(message)
    logger.info(f"Received message from Binance: {json_message}")

    try:
        # Retrieve Kafka producer from WebSocket instance
        producer = getattr(ws, 'kafka_producer', None)

        if producer:
            # Process the trade message
            trades = handle_trades(json_message)

            # Send processed trades to Kafka
            send_message(producer, KAFKA_TOPIC, trades)
        else:
            logger.error("Kafka producer not found in WebSocket instance.")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

# Callback function for WebSocket errors
def on_error(ws, error):
    logger.error(f"WebSocket error: {error}")

# Callback function for WebSocket close
def on_close(ws, close_status_code, close_msg):
    logger.info(f"WebSocket closed with code {close_status_code}, message: {close_msg}")

# Callback function for WebSocket open
def on_open(ws):
    logger.info("WebSocket connection opened.")

# Function to run the WebSocket connection
def run_websocket():
    while True:
        try:
            # Initialize WebSocket connection
            ws = websocket.WebSocketApp(
                BINANCE_WS_URL,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_ping=lambda ws, msg: logger.info(f"Received pong: {msg}"),
                on_pong=lambda ws, msg: logger.info("Pong received"),
            )

            # Attach Kafka producer to WebSocket instance safely
            ws.kafka_producer = create_kafka_producer()

            ws.on_open = on_open
            ws.run_forever(ping_interval=PING_INTERVAL, ping_timeout=10)
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
            logger.info("Reconnecting in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    run_websocket()
