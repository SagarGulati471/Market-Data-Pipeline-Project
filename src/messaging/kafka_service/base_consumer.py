import asyncio
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

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
        dlt_topic: str | None = None,
        dlt_producer: AIOKafkaProducer | None = None,
    ):
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.message_handler = message_handler
        self.auto_offset_reset = auto_offset_reset
        self.dlt_topic = dlt_topic
        self.dlt_producer = dlt_producer
        self._consumer: AIOKafkaConsumer | None = None
        self._stop_event = asyncio.Event()

    async def start(self):
        self._consumer = AIOKafkaConsumer(
            self.topic,                                  # positional
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=False,                  
        )
        await self._consumer.start()
        logger.info(f"Consumer started: topic={self.topic}, group={self.group_id}")
        try:
            await self._run()
        except Exception as e:
            logger.exception(f"Exception occured in consumer: {e}")
            raise e
        finally:
            # Handles the CTRL+C scenario / SIGTERM shutdown signal. Ensures the consumer is stopped cleanly.
            await self._consumer.stop()
            logger.info(f"Consumer stopped: topic={self.topic}")

    async def _run(self):
        msg = None
        while not self._stop_event.is_set():
            try:
                # Timeout prevents blocking indefinitely — allows the stop event
                # to be checked every second even when no messages are arriving.
                msg = await asyncio.wait_for(self._consumer.getone(), timeout=1.0)
                await self.message_handler(msg)
                await self._consumer.commit()

            except asyncio.TimeoutError:
                continue

            except Exception as e:
                if msg is not None:
                    await self._handle_failed_message(msg, e)
                else:
                    logger.exception("Exception occurred before a message was received.")

    async def _handle_failed_message(self, msg, exc: Exception) -> None:
        """
        Handles a message that failed during processing.

        Pipeline contract:
        - If a DLT is configured: write the raw message to the Dead Letter Topic,
          preserving the original topic/partition/offset and the error as headers.
          This allows failed messages to be inspected and replayed without data loss.
        - Always commit the offset afterward, even if the DLT write itself fails.
          An uncommitted poison pill stalls every subsequent message on the same
          partition — the partition will not advance until the offset is committed.
        - If no DLT is configured, the message is logged at CRITICAL and skipped.
        """
        logger.error(
            "Message processing failed. "
            f"topic={msg.topic} partition={msg.partition} offset={msg.offset}",
            exc_info=exc,
        )

        if self.dlt_topic and self.dlt_producer:
            try:
                await self.dlt_producer.send_and_wait(
                    topic=self.dlt_topic,
                    key=msg.key,
                    value=msg.value,
                    headers=[
                        ("source_topic",     msg.topic.encode()),
                        ("source_partition", str(msg.partition).encode()),
                        ("source_offset",    str(msg.offset).encode()),
                        ("error",            str(exc).encode()),
                    ],
                )
                logger.warning(
                    f"Poison pill written to DLT '{self.dlt_topic}': "
                    f"partition={msg.partition} offset={msg.offset}"
                )
            except Exception:
                logger.critical(
                    f"Failed to write to DLT '{self.dlt_topic}'. "
                    "Message will be skipped without DLT backup.",
                    exc_info=True,
                )
        else:
            logger.critical(
                "No DLT configured. Message skipped permanently. "
                f"value={msg.value[:200] if msg.value else None}"
            )

        # Commit regardless never leave a partition stalled on a bad message.
        # We are committing even if the DLT write fails to avoid a poison pill scenario where the same bad message is retried indefinitely, blocking the partition. This means that if the DLT write fails,
        # the message will be lost without backup, but it prevents the entire consumer from stalling. The
        # Losing one message is often better than stopping an entire stream. [Availability > One bad record]
        # Have added more information in the Readme about the Poison Pill
        try:
            await self._consumer.commit()
        except Exception:
            logger.critical(
                "Failed to commit offset after failed message handling. "
                "Message may be reprocessed on restart.",
                exc_info=True,
            )

    async def stop(self):
        logger.info("Shutdown signal received.")
        self._stop_event.set()