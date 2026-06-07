import asyncio
import logging
from aiokafka import AIOKafkaConsumer

logger = logging.getLogger(__name__)


class KafkaConsumerService:
    """
    Reusable Kafka consumer loop. Inject a message_handler for each consumer type.
    Handles lifecycle (start/stop), graceful shutdown, and per-message error isolation.
    """

    def __init__(
        self,
        topic: str,
        group_id: str,
        bootstrap_servers: str,
        message_handler,            # async callable: async def handler(msg) -> None
        auto_offset_reset: str = "earliest",
    ):
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.message_handler = message_handler
        self.auto_offset_reset = auto_offset_reset
        self._consumer: AIOKafkaConsumer | None = None
        self._stop_event = asyncio.Event()

    async def start(self):
        self._consumer = AIOKafkaConsumer(
            self.topic,                                  # positional
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=False,                    # manual commit — see note below
        )
        await self._consumer.start()
        logger.info(f"Consumer started: topic={self.topic}, group={self.group_id}")
        try:
            await self._run()
        except Exception as e:
            logger.debug(f"Exception occured in consumer: {e}")
            raise e
        finally:
            await self._consumer.stop()
            logger.info(f"Consumer stopped: topic={self.topic}")

    async def _run(self):

        msg = None
        while not self._stop_event.is_set():
            try:
                msg = await asyncio.wait_for(self._consumer.getone(), timeout=1.0)
                # await self._consumer.getone()
                await self.message_handler(msg)
                await self._consumer.commit()

            except asyncio.TimeoutError:
                continue
            
            except Exception as e:
                logger.exception(
                    f"Failed to process message: topic={msg.topic} "
                    f"partition={msg.partition} offset={msg.offset}"
                    f"message={msg}"
                )
                
    async def stop(self):
        logger.info("Shutdown signal received.")
        self._stop_event.set()