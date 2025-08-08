# File: streaming/consumer.py
import os
import redis
import json
import logging
from dotenv import load_dotenv

# --- 1. SETUP AND CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

def get_redis_client():
    """Creates and returns a Redis client."""
    try:
        client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=0
        )
        client.ping()
        logging.info("Successfully connected to Redis.")
        return client
    except Exception as e:
        logging.error(f"Could not connect to Redis: {e}")
        raise

# --- 2. MAIN CONSUMER LOGIC ---
def listen_for_transactions():
    """Subscribes to the channel and processes messages."""
    redis_client = get_redis_client()
    channel_name = 'financial_transactions'

    # pubsub object is used to subscribe to channels and listen for messages.
    pubsub = redis_client.pubsub()
    pubsub.subscribe(channel_name)

    logging.info(f"Subscribed to '{channel_name}'. Listening for transactions...")

    # This is a generator that yields messages as they arrive.
    for message in pubsub.listen():
        # Filter out non-data messages (like subscribe confirmations)
        if message['type'] == 'message':
            try:
                # Deserialize the JSON string back into a Python dictionary.
                transaction = json.loads(message['data'])
                amount = transaction.get('amount', 0)

                logging.info(f"Consumed: {transaction}")

                # The real-time analytics rule for fraud detection.
                # This is a simple rule, but in a real system, it could be a complex model.
                if amount > 3000.00:
                    # Use a higher logging level for alerts to make them stand out.
                    logging.warning(f"[!!! FRAUD ALERT !!!] High-value transaction detected: {transaction}")
                    # In a production system, this would trigger another action:
                    # - Write to a high-priority database table
                    # - Send an alert to Slack/email
                    # - Call another API to temporarily block the customer's account

            except json.JSONDecodeError:
                logging.error(f"Could not decode message: {message['data']}")
            except Exception as e:
                logging.error(f"An error occurred while processing message: {e}")

if __name__ == "__main__":
    try:
        listen_for_transactions()
    except KeyboardInterrupt:
        logging.info("Consumer stopped by user.")
    except Exception as e:
        logging.critical(f"A critical error occurred in the consumer: {e}")