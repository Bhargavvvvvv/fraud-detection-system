import time
import json
import random
import pandas as pd
import os
import sys

# Add current directory to path so we can import utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import get_kafka_producer

# Initialize Producer using our secure utility
producer = get_kafka_producer()
topic = "transactions"

# Load Data (Adjust path if needed to find the CSV)
# We assume the CSV is in the root folder, so we go up one level
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "fraud_data.csv")

if not os.path.exists(DATA_PATH):
    print(f"❌ Error: fraud_data.csv not found at {DATA_PATH}")
    sys.exit(1)

df = pd.read_csv(DATA_PATH)
data_stream = df.to_dict(orient="records")

def delivery_callback(err, msg):
    if err:
        print(f"❌ ERROR: Message failed delivery: {err}")
    else:
        print(f"✅ Produced event to {msg.topic()}: key = {msg.key().decode('utf-8')}")

def produce_transactions():
    print(f"🚀 Starting Transaction Stream from {DATA_PATH}...")
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
        print("\n🛑 Stream stopped by user.")
    finally:
        producer.flush()

if __name__ == "__main__":
    produce_transactions()