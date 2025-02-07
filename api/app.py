from flask import Flask, jsonify, Response
from prometheus_client import Counter, Histogram, generate_latest
from confluent_kafka import Consumer, KafkaError
import joblib
import json
import pandas as pd
import threading
import logging
import signal

app = Flask(__name__)

# Load the trained model and scaler
model = joblib.load('anomaly_model.joblib')
scaler = joblib.load('scaler.joblib')

# Kafka consumer setup
consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'real_time_group',
    'auto.offset.reset': 'earliest',
    'broker.address.family': 'v4'
})
consumer.subscribe(['sensor_data'])

# Prometheus metrics
ANOMALY_COUNT = Counter('anomaly_count', 'Total number of anomalies detected')
MESSAGE_COUNT = Counter('message_count', 'Total number of messages processed')
CONSUMER_ERRORS = Counter('consumer_errors', 'Total number of consumer errors')
ANOMALY_SCORE_HIST = Histogram('anomaly_score_histogram', 'Anomaly Score Histogram')

# Global flag to track if the consumer thread has started
consumer_started = False

# Setup logging
logging.basicConfig(level=logging.INFO)

def consume_data():
    """
    Consume data from Kafka topic and make predictions.
    """
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                    CONSUMER_ERRORS.inc()
                    break

            data = json.loads(msg.value().decode('utf-8'))
            logging.info(f"Received data: {data}")

            # Extract features with names
            feature_names = ['temperature', 'humidity', 'sound']
            features = pd.DataFrame([data], columns=feature_names)

            # Standardize features
            features_scaled = scaler.transform(features)

            # Predict anomalies and scores
            anomaly_score = model.decision_function(features_scaled)[0]  # Anomaly score
            is_anomaly = model.predict(features_scaled)[0]  # 1 if normal, -1 if anomaly
            is_anomaly = 1 if is_anomaly == -1 else 0  # Convert -1 (anomalies) to 1 and 1 (normal) to 0

            # Log the results
            result = {
                'data': data,
                'anomaly_score': anomaly_score,
                'is_anomaly': is_anomaly
            }
            logging.info(result)

            # Update Prometheus metrics
            MESSAGE_COUNT.inc()
            ANOMALY_SCORE_HIST.observe(anomaly_score)
            if is_anomaly:
                ANOMALY_COUNT.inc()

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        CONSUMER_ERRORS.inc()
    finally:
        consumer.close()

def start_consumer_thread():
    global consumer_started
    if not consumer_started:
        consumer_thread = threading.Thread(target=consume_data)
        consumer_thread.daemon = True
        consumer_thread.start()
        consumer_started = True

@app.route('/start', methods=['GET'])
def start():
    """
    Endpoint to start consuming data from Kafka.
    """
    start_consumer_thread()
    return jsonify({"message": "Started consuming data from Kafka"}), 200

@app.route('/metrics', methods=['GET'])
def metrics():
    """
    Endpoint to expose Prometheus metrics.
    """
    return Response(generate_latest(), mimetype='text/plain')

def shutdown(signum, frame):
    logging.info("Shutting down Kafka consumer...")
    consumer.close()
    exit(0)

# Register signals for graceful shutdown
signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

if __name__ == '__main__':
    start_consumer_thread()
    app.run(host='0.0.0.0', port=5000)
