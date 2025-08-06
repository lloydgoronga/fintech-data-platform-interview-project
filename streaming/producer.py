# File: streaming/producer.py
import os
import redis
import json
import time
import random
import logging
from dotenv import load_dotenv

# --- 1. SETUP AND CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

def get_redis_client():
    """Creates and returns a Redis client, ensuring connection."""
    try:
        client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=0,
            decode_responses=True # Decode responses to strings
        )
        # Ping the server to check the connection
        client.ping()
        logging.info("Successfully connected to Redis.")
        return client
    except Exception as e:
        logging.error(f"Could not connect to Redis: {e}")
        raise

# --- 2. MAIN PRODUCER LOGIC ---
if __name__ == "__main__":
    try:
        redis_client = get_redis_client()
        # The channel is like a topic in Kafka
        channel_name = 'financial_transactions'
        logging.info(f"Starting real-time transaction producer on channel '{channel_name}'...")

        # Infinite loop to continuously produce messages
        while True:
            # Generate a realistic-looking fake transaction
            transaction = {
                'transaction_id': f"txn_{int(time.time())}_{random.randint(1000, 9999)}",
                'customer_id': random.randint(1, 3),
                'amount': round(random.uniform(5.0, 5000.0), 2),
                'merchant_name': random.choice(['GreenLeaf Grocers', 'The Daily Grind Coffee', 'TechSphere Electronics', 'GlobalMart']),
                'timestamp': time.time()
            }

            # Serialize the Python dictionary to a JSON string.
            # This is a standard format for data transmission.
            message = json.dumps(transaction)

            # Publish the message to the specified Redis channel.
            redis_client.publish(channel_name, message)
            logging.info(f"Produced: {message}")

            # Wait for a random interval to simulate a real-world stream.
            time.sleep(random.uniform(0.5, 3.0))

    except KeyboardInterrupt:
        # This allows you to stop the producer gracefully with Ctrl+C
        logging.info("Producer stopped by user.")
    except Exception as e:
        logging.critical(f"A critical error occurred in the producer: {e}")