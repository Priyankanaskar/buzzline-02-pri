#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "buzzline")  # Ensure correct topic name
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID", "default_group")  # Changed variable name for clarity
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

#####################################
# Define a function to process a single message
#####################################

def process_message(message: str) -> None:
    logger.info(f"Processing message: {message}")
    if "Weather" in message:
        logger.info(f"Weather update: {message}")
    else:
        logger.warning(f"Non-weather message received: {message}")

#####################################
# Define main function for this module
#####################################

def main() -> None:
    logger.info("START consumer.")

    # Fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            # Check if the message is in bytes, and decode it if necessary
            message_str = message.value.decode('utf-8') if isinstance(message.value, bytes) else message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()