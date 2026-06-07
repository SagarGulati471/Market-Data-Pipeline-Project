import json
import logging
from .models import Trade

logger = logging.getLogger(__name__)


async def handle_message(msg):
    """
    Parse raw Finnhub message, validate, and produce normalized trades
    to the next topic. One raw message can contain multiple trades.
    """
    raw = msg.value.decode("utf-8")

    try:
        payload = json.loads(raw)
        logger.debug(f"Kafka message received: {payload}")
    except json.JSONDecodeError:
        logger.exception(f"Invalid JSON at offset {msg.offset}: {raw[:200]}")
        return  # skip malformed messages, don't crash the loop

    # Finnhub sends {"type": "ping"} — ignore non-trade messages
    if payload.get("type") != "trade":
        logger.debug(f"Skipping non-trade message: type={payload.get('type')}")
        return

    for item in payload.get("data", []):
        try:
            trade = Trade(**item)
            logger.info(f"Trade: {trade}")
            # TODO: produce to 'trades-normalized' topic
        except Exception:
            logger.exception(f"Validation failed for item: {item}")
            # individual bad trade → skip, don't kill the whole batch