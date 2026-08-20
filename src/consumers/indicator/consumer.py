import asyncio
import logging
import signal

from utils.logger import setup_logger
from config.config import Config
from messaging.kafka_service.base_consumer import KafkaConsumerService
from messaging.kafka_service.service import create_kafka_producer, shutdown_kafka_producer
from storage.timescaledb_deployment.db_wrapper import init_pool, close_pool
from .handler import make_handler

setup_logger()
logger = logging.getLogger(__name__)


async def main():

    config = Config()

    # Initialize shared resources once at startup
    # These are long-lived objects that are reused for every message in this
    # process. Creating them per-message would mean opening and closing TCP
    # connections on every single trade — catastrophic for throughput.
    db_pool  = await init_pool()
    producer = await create_kafka_producer()
    dlt_producer = await create_kafka_producer()

    handler = make_handler(pool=db_pool, producer=producer)

    consumer = KafkaConsumerService(
        topic=config.KAFKA_TOPIC_CANDLES,
        group_id="indicator-group",
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        message_handler=handler,
        dlt_topic=config.DEAD_LETTER_TOPIC_INDICATOR,
        dlt_producer=dlt_producer,
    )

    # Register graceful shutdown for Ctrl+C (SIGINT) and Kubernetes SIGTERM.
    # Both signals call consumer.stop(), which sets the stop event so the
    # consumer loop exits cleanly after processing the current message.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(consumer.stop()))

    try:
        await consumer.start()
    finally:
        # Shutdown order matters:
        # 1. Consumer is already stopped (stop event triggered the exit from start()).
        # 2. Flush and stop Kafka producers — ensures in-flight messages are delivered
        #    before the process exits, not silently dropped.
        # 3. Close DB pool — releases all connections cleanly back to the server.
        logger.info("Shutting down indicator resources...")
        await shutdown_kafka_producer(producer)
        await shutdown_kafka_producer(dlt_producer)
        await close_pool()

        logger.info("Indicator pipeline shutdown complete.")
    



if __name__ == "__main__":
    asyncio.run(main())