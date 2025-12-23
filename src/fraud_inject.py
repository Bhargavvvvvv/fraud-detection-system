import json
import time
import os
import sys

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import get_kafka_producer

# Get secure producer
producer = get_kafka_producer()
topic = "transactions"

# The "Bad" Transaction
fraud_transaction = {
    "transaction_id": "FRAUD_TEST_999",
    "amount": 9999.99, 
    "transaction_hour": 3,
    "merchant_category": "Electronics",
    "foreign_transaction": 1,
    "location_mismatch": 1,
    "device_trust_score": 0,
    "cardholder_age": 25,
    "timestamp": time.time()
}

def delivery_callback(err, msg):
    if err:
        print(f"❌ Failed: {err}")
    else:
        print(f"😈 FRAUD INJECTED: {msg.key().decode('utf-8')}")

# Send it
key = str(fraud_transaction['transaction_id'])
value = json.dumps(fraud_transaction)

producer.produce(topic, key=key, value=value, callback=delivery_callback)
producer.flush()