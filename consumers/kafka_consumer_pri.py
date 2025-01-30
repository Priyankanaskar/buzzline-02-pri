import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WeatherConsumer")

# Kafka settings
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_topic")

# Function to create Kafka consumer
def create_kafka_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

# Main consumer function
def run_consumer():
    logger.info("Starting Kafka consumer...")
    consumer = create_kafka_consumer()

    for message in consumer:
        weather_data = message.value
        logger.info(f"Received weather update: {weather_data}")

if __name__ == "__main__":
    run_consumer()
