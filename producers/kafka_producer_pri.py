import os
from kafka import KafkaProducer
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BuzzProducer")

# Get Kafka settings from environment variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "buzz_topic")

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: str(v).encode('utf-8')
)

# Send a simple message to the topic
message = "Hello, this is a test buzz message!"
producer.send(KAFKA_TOPIC, value=message)

# Log and close producer
logger.info(f"Sent message: {message}")
producer.close()
