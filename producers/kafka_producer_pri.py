#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time
import requests 

# Import external packages
from dotenv import load_dotenv
load_dotenv()

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

# Fetch the OpenWeather API key from the environment
API_KEY = os.getenv("OPENWEATHER_API_KEY")
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "buzz_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 60))
    logger.info(f"Message interval: {interval} seconds")
    return interval

def fetch_weather_data(city):
    """Fetch weather data for the given city."""
    url = f"{BASE_URL}?q={city}&appid={API_KEY}&units=metric"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return {
            "description": data["weather"][0]["description"],
            "temp": data["main"]["temp"]
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching weather data: {e}")
        return None

#####################################
# Message Generator
#####################################


def generate_messages(producer, topic, interval_secs, city):
        try:
            while True:
                # Request weather data from OpenWeather API
                weather_data = fetch_weather_data(city)
                
                if weather_data:
                    message = f"Weather in {city}: {weather_data['description']}, Temp: {weather_data['temp']}°C"
                else:
                    message = f"Could not retrieve weather data for {city}."

                logger.info(f"Generated buzz: {message}")
                producer.send(topic, value=message)
                logger.info(f"Sent message to topic '{topic}': {message}")
                
                time.sleep(interval_secs)
        except KeyboardInterrupt:
            logger.warning("Producer interrupted by user.")
        except Exception as e:
            logger.error(f"Error in message generation: {e}")
        finally:
            producer.close()
            logger.info("Kafka producer closed.")

#####################################
# Main Function
#####################################


def main():
    logger.info("START producer.")
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()
    city = os.getenv("CITY")  

    # Create the Kafka producer
    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    generate_messages(producer, topic, interval_secs, city)

    logger.info("END producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()