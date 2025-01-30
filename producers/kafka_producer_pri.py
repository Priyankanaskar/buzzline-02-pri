import os
import time
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from dotenv import load_dotenv
import json
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WeatherProducer")

# Kafka settings
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_topic")

# Weather URL from .env
WEATHER_URL = os.getenv("WEATHER_URL")

if not WEATHER_URL:
    logger.error("No weather URL found! Please set WEATHER_URL in .env.")
    exit(1)

# Function to scrape weather data
def fetch_weather_data():
    try:
        headers = {"User-Agent": "Mozilla/5.0"}  # Avoid getting blocked
        response = requests.get(WEATHER_URL, headers=headers)
        response.raise_for_status()

        # Parse the HTML content
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract weather details (modify these selectors as needed)
        temperature = soup.find(class_="CurrentConditions--tempValue--3KcTQ").text.strip()
        condition = soup.find(class_="CurrentConditions--phraseValue--2xXSr").text.strip()

        return {
            "temperature": temperature,
            "condition": condition,
            "timestamp": time.time(),
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching weather data: {e}")
        return None
    except AttributeError:
        logger.error("Could not find weather data on the page. The HTML structure may have changed.")
        return None

# Function to create Kafka producer
def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

# Main producer function
def run_producer():
    logger.info("Starting Kafka producer...")
    producer = create_kafka_producer()

    while True:
        weather_data = fetch_weather_data()
        if weather_data:
            logger.info(f"Sending message: {weather_data}")
            producer.send(KAFKA_TOPIC, value=weather_data)
        else:
            logger.error("Failed to fetch weather data, skipping message.")

        time.sleep(60)  # Send message every 60 seconds

if __name__ == "__main__":
    run_producer()
