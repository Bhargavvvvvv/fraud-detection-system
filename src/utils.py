import os
from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer
from upstash_redis import Redis

# Load environment variables from .env file
load_dotenv()

# --- GLOBAL CONFIGURATION ---
# Now we read from the environment, not hardcoded strings
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")
REDIS_URL = os.getenv("REDIS_URL")
REDIS_TOKEN = os.getenv("REDIS_TOKEN")

# Raise error if keys are missing (Good for debugging)
if not KAFKA_API_KEY or not REDIS_TOKEN:
    raise ValueError("❌ API Keys not found! Did you set up your .env file?")

# --- HELPER FUNCTIONS ---
def get_kafka_producer():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': KAFKA_API_KEY,
        'sasl.password': KAFKA_API_SECRET,
    }
    return Producer(conf)

def get_kafka_consumer(group_id):
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': KAFKA_API_KEY,
        'sasl.password': KAFKA_API_SECRET,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe(['transactions'])
    return consumer

def get_redis_client():
    return Redis(url=REDIS_URL, token=REDIS_TOKEN)