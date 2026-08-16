import asyncio
import json
import logging
import signal

from utils.logger import setup_logger
from config.config import Config
from messaging.kafka_service.base_consumer import KafkaConsumerService
from messaging.kafka_service.service import create_kafka_producer, shutdown_kafka_producer
from storage.timescaledb_deployment.db_wrapper import init_pool, close_pool
from order_executor.models import Order
from consumers.signal_generator.models import Signal
from .order_manager.order_manager import OrderManager

setup_logger()
logger = logging.getLogger(__name__)


def make_handler(pool, producer):

    async def handle_message(msg):
        """
        Handle incoming Kafka messages for order execution.

        This function decodes the incoming message, parses it into a Signal object,
        and passes it to the OrderManager for validation and execution. If the order
        is valid, it returns True; otherwise, it returns False.

        Args:
            msg: The incoming Kafka message.
        """
        # the function receives an order from kafka consumer, decodes it and passes it to the risk manager
        # for validation. If the order is valid, it returns True, otherwise it returns False.
        # Decoding the message
        raw = msg.value.decode("utf-8")
        try:
            logger.debug(f"Received raw message: {raw}")
            payload = json.loads(raw)
            signal = Signal(**payload)
            logger.debug(f"Parsed signal: {signal}")
        except Exception as e:
            logger.exception(f"Failed to parse message: {e} raw={raw}")
            raise e

        order_manager = OrderManager(risk_config=None)
        return await order_manager.handle_order(signal)  # Here 'signal' is actually a Signal instance

    return handle_message

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
        topic=config.KAFKA_TOPIC_SIGNAL,
        group_id="order-executor-group",
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        message_handler=handler,
        dlt_topic=config.DEAD_LETTER_TOPIC_ORDER_EXECUTOR,
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
        logger.info("Shutting down order executor resources...")
        await shutdown_kafka_producer(producer)
        await shutdown_kafka_producer(dlt_producer)
        await close_pool(db_pool)

        logger.info("Order executor pipeline shutdown complete.")
    



if __name__ == "__main__":
    asyncio.run(main())