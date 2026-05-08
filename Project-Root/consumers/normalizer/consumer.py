import asyncio
import logging
import signal

from utils.logger import setup_logger
from config.config import Config
from messaging.kafka_service.base_consumer import KafkaConsumerService
from .handler import handle_message

setup_logger()
logger = logging.getLogger(__name__)


async def main():
    config = Config()

    consumer = KafkaConsumerService(
        topic=config.FINNHUB_KAFKA_TOPIC,         # 'market-data-raw'
        group_id="normalizer-group",
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,   # 'localhost:9092'
        message_handler=handle_message,
    )

    # Graceful shutdown on Ctrl+C or SIGTERM (for Kubernetes pods)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(consumer.stop()))

    await consumer.start()


if __name__ == "__main__":
    asyncio.run(main())