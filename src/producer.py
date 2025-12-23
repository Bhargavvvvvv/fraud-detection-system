import time
import json
import random
import pandas as pd
import os
import sys


sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import get_kafka_producer

producer = get_kafka_producer()
topic = "transactions"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "cc_fraud.csv")

if not os.path.exists(DATA_PATH):
    print("check",DATA_PATH)
    sys.exit(1)

df = pd.read_csv(DATA_PATH)
data_stream = df.to_dict(orient="records")

def delivery_callback(err, msg):
    if err:
        print(f"ERROR: {err}")
    else:
        print(f"Produced event to {msg.topic()}: key = {msg.key().decode('utf-8')}")

def produce_transactions():
    try:
        while True:
            transaction = random.choice(data_stream)
            transaction['timestamp'] = time.time()
            
            key = str(transaction['transaction_id'])
            value = json.dumps(transaction)

            producer.produce(
                topic, 
                key=key, 
                value=value, 
                callback=delivery_callback
            )
            
            producer.poll(1)
            time.sleep(2) 
            
    except KeyboardInterrupt:
        print("\nStream stopped by user.")
    finally:
        producer.flush()

if __name__ == "__main__":
    produce_transactions()