import aiokafka
from config.config import Config
from utils.logger import setup_logger
import asyncio
import logging
import json


setup_logger()
logger = logging.getLogger(__name__)


# Function to create and return an aiokafka producer instance
async def create_kafka_producer():
    config = Config()
    # bootstrap_servers='127.0.0.1:9092'
    bootstrap_servers='localhost:9093'
    producer = aiokafka.AIOKafkaProducer(
        # bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        bootstrap_servers=bootstrap_servers
        # security_protocol=config.KAFKA_SECURITY_PROTOCOL,
        # sasl_mechanism=config.KAFKA_SASL_MECHANISM,
        # sasl_plain_username=config.KAFKA_SASL_USERNAME,
        # sasl_plain_password=config.KAFKA_SASL_PASSWORD,
    )
    logger.info(f"producer starting at bootstrap_servers = {bootstrap_servers}")
    await producer.start()
    logger.info("Kafka producer created and started successfully.")
    return producer


# Function to send a message to a specified Kafka topic
# async def send_message(producer, topic, key, value):
#     try:
#         # Send the message to the specified topic with the given key and value
#         await producer.send_and_wait(topic, key=key.encode('utf-8'), value=value.encode('utf-8'))
#         logger.info(f"Message sent to topic '{topic}': key={key}, value={value}")
#     except Exception as e:
#         logger.error(f"Failed to send message to Kafka: {e}")

async def send_message(producer, topic: str, key=None, value=None):
    try:
        if isinstance(value, dict):
            value = json.dumps(value).encode('utf-8')
        elif isinstance(value, str):
            value = value.encode('utf-8')
        # else: assume it's already bytes

        if isinstance(key, str):
            key = key.encode('utf-8')

        await producer.send_and_wait(
            topic=topic,
            key=key,
            value=value
        )
        logger.debug(f"Successfully sent message to topic {topic}")
        
    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {e}")
        raise


# Function to gracefully shut down the Kafka producer
async def shutdown_kafka_producer(producer):
    try:
        await producer.stop()
        logger.info("Kafka producer stopped successfully.")
    except Exception as e:
        logger.error(f"Failed to stop Kafka producer: {e}")


# Function to create a topic if it doesn't exist (optional, depending on your Kafka setup)
async def create_topic(producer, topic_name):
    try:
        # Create the topic with the specified name and number of partitions
        await producer.create_topics([topic_name], num_partitions=1, replication_factor=1)
        logger.info(f"Topic '{topic_name}' created successfully.")
    except aiokafka.errors.TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.error(f"Failed to create topic '{topic_name}': {e}")