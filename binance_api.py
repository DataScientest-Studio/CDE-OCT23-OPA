import json

import websocket
from datetime import datetime
from cassandradb import insert


def ws_trades():
    socket = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade'

    def on_message(wsapp, message):
        json_message = json.loads(message)
        trade = handle_trades(json_message)
        insert(trade['symbole'], trade['price'], trade['qty'], trade['date'], trade['time'])

    def on_error(wsapp, error):
        print(error)

    wsapp = websocket.WebSocketApp(socket, on_message=on_message, on_error=on_error)
    wsapp.run_forever()


def handle_trades(json_message):
    date_time = datetime.fromtimestamp(json_message['E'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    dt = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')

    date = dt.date()
    t = datetime.strptime(dt.time().strftime('%H:%M:%S'), '%H:%M:%S')
    time = ((t.hour * 3600 + t.minute * 60 + t.second) * 10 ** 9) + t.microsecond * 10 ** 3
    trades = {
        "symbol": json_message['s'],
        "price": float(json_message['p']),
        "qty": float(json_message['q']),
        "date": date,
        "time": time
    }

    return trades


ws_trades()
