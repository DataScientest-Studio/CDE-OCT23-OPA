import uuid
from pprint import pprint
from cassandra.cluster import Cluster

cluster = Cluster(['146.59.153.232'], port=9042)
session = cluster.connect()


def select_all():
    rows = session.execute('SELECT * FROM opa.streaming_history')
    for row in rows:
        pprint(f"{row}")


def insert(symbol, price, qty, date, time):
    stream_id = uuid.uuid4()
    try:
        session.execute(
            "INSERT INTO opa.streaming_history (stream_id, symbole, price, qty, date, time) "
            "VALUES (%S, %S, %S, %S, %S, %S)",
            (stream_id, symbol, price, qty, date, time))
        print("Date inserted into streaming_history")
    except Exception as e:
        print(f"Data insertion failed error: {e}")


select_all()
