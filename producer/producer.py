import time
import random
import json
from confluent_kafka import Producer
import numpy as np
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Kafka producer setup
producer = Producer({
    'bootstrap.servers': 'kafka:9092',
    'broker.address.family': 'v4'
})

# Factory realistic ranges and anomaly probabilities
SENSOR_CONFIG = {
    "temperature": {"mean": 22, "std_dev": 2, "minor_anomaly_prob": 0.05, "major_anomaly_prob": 0.01},  # Factory temperature
    "humidity": {"mean": 45, "std_dev": 5, "minor_anomaly_prob": 0.05, "major_anomaly_prob": 0.01},     # Factory humidity
    "sound": {"mean": 75, "std_dev": 7, "minor_anomaly_prob": 0.05, "major_anomaly_prob": 0.01}         # Factory sound level
}

def generate_sensor_data():
    """Generates sensor data with occasional minor and major anomalies."""
    data = {}
    for sensor, config in SENSOR_CONFIG.items():
        # Normal data
        value = np.random.normal(config["mean"], config["std_dev"])

        # Introduce minor anomalies more frequently
        if random.random() < config["minor_anomaly_prob"]:
            value += np.random.normal(0, config["std_dev"] * 1.5)  # Minor deviation

        # Introduce major anomalies less frequently
        if random.random() < config["major_anomaly_prob"]:
            value += np.random.choice([-1, 1]) * np.random.uniform(config["std_dev"] * 3, config["std_dev"] * 5)  # Major deviation

        # Round the value for realism
        data[sensor] = round(value, 2)

    # Add a timestamp
    data["timestamp"] = datetime.utcnow().isoformat()
    return data

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery success or failure."""
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

def produce_sensor_data():
    """Continuously produce sensor data to the Kafka topic."""
    try:
        while True:
            sensor_data = generate_sensor_data()
            producer.produce('sensor_data', value=json.dumps(sensor_data), callback=delivery_report)
            logging.info(f"Produced: {sensor_data}")
            producer.poll(0)  # Poll to handle delivery report callbacks
            time.sleep(1)  # Simulate one second between data points
    except KeyboardInterrupt:
        logging.info("Stopping producer.")
    finally:
        producer.flush()

if __name__ == "__main__":
    produce_sensor_data()
