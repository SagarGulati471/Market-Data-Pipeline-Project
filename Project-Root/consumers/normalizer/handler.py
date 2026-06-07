import json
import logging
from .models import Trade

logger = logging.getLogger(__name__)

# Finnhub condition codes that mark a trade as invalid for OHLCV and analytics.
# "8" = Out of Sequence — a late-reported or corrected trade carrying a stale price.
# Including these corrupts high/low/close calculations in the candle builder.
_EXCLUDED_CONDITIONS = frozenset({"8"})


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
        # Exclude trades flagged with invalid condition codes before validation.
        # Checked on the raw dict to avoid building a Trade object unnecessarily.
        conditions = item.get("c") or []
        if _EXCLUDED_CONDITIONS.intersection(conditions):
            logger.debug(
                f"Skipping excluded trade: symbol={item.get('s')}, conditions={conditions}"
            )
            continue

        try:
            trade = Trade.model_validate(item)
            logger.debug(f"Trade validated: {trade}")
            # TODO: produce to 'trades-normalized' topic
        except Exception:
            logger.exception(f"Validation failed for item: {item}")
            # individual bad trade → skip, don't kill the whole batch