import os
import redis
import json
import logging
from threading import Thread
from flask import Flask, render_template
from flask_socketio import SocketIO
from dotenv import load_dotenv

# --- SETUP ---
load_dotenv()
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
# The secret key is needed for session management by SocketIO
app.config['SECRET_KEY'] = 'your-secret-key!' 
socketio = SocketIO(app, cors_allowed_origins="*")

# --- REDIS BACKGROUND THREAD ---
# This part of the code will run in the background, listening to Redis
# and pushing data to the web clients via WebSockets.

def redis_listener():
    """Listens to Redis and broadcasts messages to clients."""
    r = redis.Redis(host=os.getenv('REDIS_HOST'), port=int(os.getenv('REDIS_PORT')), db=0)
    pubsub = r.pubsub()
    pubsub.subscribe('financial_transactions')
    logging.info("Subscribed to Redis channel and listening for messages...")

    for message in pubsub.listen():
        if message['type'] == 'message':
            data = message['data'].decode('utf-8')
            # The 'emit' function sends an event to all connected web clients.
            # We name our event 'new_transaction'.
            socketio.emit('new_transaction', data)
            logging.info(f"Broadcasted to dashboard: {data}")

# --- FLASK ROUTES ---
@app.route('/')
def index():
    """Serves the main dashboard page."""
    return render_template('index.html')

# --- MAIN EXECUTION ---
if __name__ == '__main__':
    # Start the Redis listener in a separate thread so it doesn't block the web server.
    listener_thread = Thread(target=redis_listener, daemon=True)
    listener_thread.start()
    
    # Start the Flask-SocketIO web server.
    logging.info("Starting Flask-SocketIO server...")
    # Use allow_unsafe_werkzeug=True for newer versions of Werkzeug
    socketio.run(app, host='0.0.0.0', port=5001, debug=True, allow_unsafe_werkzeug=True)
