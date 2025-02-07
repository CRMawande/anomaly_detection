import json
from confluent_kafka import Consumer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Kafka consumer setup
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'sensor_data_group',
    'auto.offset.reset': 'latest',
    'broker.address.family': 'v4'
})

# Subscribe to the topic
consumer.subscribe(['sensor_data'])

def collect_sensor_data(output_file, num_records=10000):
    """Collect sensor data from Kafka and save to a JSON file."""
    data = []
    try:
        while len(data) < num_records:
            msg = consumer.poll(1.0)  # Poll for messages
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                continue

            # Decode and append message
            record = json.loads(msg.value().decode('utf-8'))
            data.append(record)
            logging.info(f"Collected record {len(data)}: {record}")

        # Save to file
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=4)
        logging.info(f"Saved {num_records} records to {output_file}")
    except KeyboardInterrupt:
        logging.info("Stopped by user.")
    finally:
        consumer.close()

if __name__ == "__main__":
    collect_sensor_data(output_file="sensor_data.json")
