import os
import logging
from datetime import datetime
import json

import websocket
from google.cloud import pubsub_v1

from gdax import subscription_message


project_id = os.environ['GOOGLE_PROJECT_ID']

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, 'gdax-websocket-raw')

def on_message(ws, message):
    dt = datetime.utcnow()
    dct = json.loads(message)
    data = {'collected_at': dt.isoformat(), **dct}
    publisher.publish(topic_path, data=json.dumps(data).encode('utf-8'))

def on_error(ws, error):
    logging.warning(error)

def on_close(ws):
    logging.warning("### closed ###")

def on_open(ws):
    msg = json.dumps(subscription_message)
    ws.send(msg)
    logging.debug('sent websocket message: {}'.format(msg))

if __name__ == "__main__":
    ws = websocket.WebSocketApp("wss://ws-feed.gdax.com",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)

    ws.on_open = on_open
    ws.run_forever()
