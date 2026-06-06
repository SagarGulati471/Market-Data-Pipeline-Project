import json
import logging
from .models import FinnhubMessage

logger = logging.getLogger(__name__)


async def handle_message(msg):
    """
    Parse raw Finnhub message, validate, and produce normalized trades
    to the next topic. One raw message can contain multiple trades.
    """
    raw = msg.value.decode("utf-8")

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON at offset {msg.offset}: {raw[:200]}")
        return  # skip malformed messages, don't crash the loop

    # Finnhub sends {"type": "ping"} — ignore non-trade messages
    if payload.get("type") != "trade":
        logger.debug(f"Skipping non-trade message: type={payload.get('type')}")
        return

    for item in payload.get("data", []):
        try:
            trade = FinnhubMessage(**item)
            logger.info(f"Trade: {trade}")
            # TODO: produce to 'trades-normalized' topic
        except Exception:
            logger.exception(f"Validation failed for item: {item}")
            # individual bad trade → skip, don't kill the whole batch