import json
import logging
import asyncpg
import asyncio

from config.config import Config
from ..candle_builder.models import Candle, NormalizedTrade

config = Config()
logger = logging.getLogger(__name__)


def make_handler(pool, producer):
    """
    Factory function that creates the message handler with bound resources.

    This pattern allows us to inject shared dependencies (DB pool, Kafka producer)
    into the handler without making them global variables or passing them around
    explicitly in the consumer loop. The returned handle_message function has
    access to these resources via closure scope.
    """

    async def handle_message(msg):
        # Decoding the message
        raw = msg.value.decode("utf-8")
        try:
            logger.debug(f"Received raw message: {raw}")
            payload = json.loads(raw)
            candle = Candle(**payload)
            logger.debug(f"Parsed candle: {candle}")
        except Exception as e:
            logger.exception(f"Failed to parse message: {e} raw={raw}")
            return

        

        
        


    return handle_message


    
