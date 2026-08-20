import json
import logging
import asyncio
from collections import OrderedDict

import asyncpg
from aiokafka import AIOKafkaProducer

from .models import Trade
from config.config import Config
from messaging.kafka_service.service import send_message

logger = logging.getLogger(__name__)
config = Config()

# Finnhub condition codes that mark a trade as invalid for OHLCV and analytics.
# "8" = Out of Sequence — a late-reported or corrected trade carrying a stale price.
# Including these in candle calculations corrupts high/low/close values.
_EXCLUDED_CONDITIONS = frozenset({"8"})

# Maximum number of trade hashes retained in the in-memory dedup set.
# Once this limit is reached, the oldest entry is evicted (FIFO) to make room.
# 10,000 hashes × ~64 bytes each ≈ 640 KB — negligible memory footprint.
_DEDUP_MAX_SIZE = 10_000


def make_handler(pool: asyncpg.Pool, producer: AIOKafkaProducer):
    """
    Factory function that returns a configured handle_message coroutine.

    This is a CLOSURE — the async function returned by this factory "closes over"
    the `pool` and `producer` variables from the outer scope. That means the
    returned handle_message carries permanent references to those objects without
    needing global variables or re-creation on every call.

    Example of what a closure means in practice:

        handler = make_handler(pool=db_pool, producer=kafka_producer)
        # handler is now an async function that already "knows" about db_pool
        # and kafka_producer internally. You never pass them again.
        await handler(msg)   # ← uses db_pool and kafka_producer transparently

    The caller (consumer.py) creates the pool and producer once at startup,
    calls make_handler() once to bind them, and passes the resulting handler
    to KafkaConsumerService. Every message invocation reuses those same
    long-lived TCP connections.

    Args:
        pool:     asyncpg connection pool (initialized once at startup).
        producer: AIOKafkaProducer (initialized once at startup).
    """

    # Dedup state is scoped to this closure — it lives for the lifetime of the
    # service process and is not shared between different handler instances.
    # OrderedDict preserves insertion order, enabling FIFO eviction of old hashes.
    _seen_hashes: OrderedDict[str, None] = OrderedDict()

    def _is_duplicate(trade_hash: str) -> bool:
        """
        Returns True if this trade hash was seen recently (duplicate).
        Adds the hash to the seen set if new. Evicts the oldest entry
        when the cap is reached to keep memory bounded.
        """
        if trade_hash in _seen_hashes:
            return True
        if len(_seen_hashes) >= _DEDUP_MAX_SIZE:
            _seen_hashes.popitem(last=False)  # evict oldest (FIFO)
        _seen_hashes[trade_hash] = None
        return False

    async def handle_message(msg) -> None:
        """
        Entry point for each raw Kafka message from the market-data-raw topic.

        Responsibilities:
        1. Decode and parse the raw Finnhub JSON payload.
        2. Skip non-trade messages (e.g. pings).
        3. For each trade in the batch:
           a. Filter out invalid condition codes (out-of-sequence trades).
           b. Validate and deserialize into a Trade model.
           c. Deduplicate using an in-memory hash set.
           d. Concurrently produce to trades-normalized topic AND persist to DB.
        """
        raw = msg.value.decode("utf-8")

        try:
            payload = json.loads(raw)
            logger.debug(f"Message received: offset={msg.offset} partition={msg.partition}")
        except json.JSONDecodeError as e:
            logger.exception(f"Invalid JSON at offset {msg.offset}: {raw[:200]}")
            raise e # malformed message — base_consumer will route to DLT

        # Finnhub sends periodic {"type": "ping"} heartbeats — skip silently.
        if payload.get("type") != "trade":
            logger.debug(f"Skipping non-trade message: type={payload.get('type')}")
            return

        for item in payload.get("data", []):

            # 1.) Filter invalid condition codes
            # Checked on raw dict before model construction to avoid wasted allocation.
            conditions = item.get("c") or []
            if _EXCLUDED_CONDITIONS.intersection(conditions):
                logger.debug(
                    f"Skipping out-of-sequence trade: "
                    f"symbol={item.get('s')} conditions={conditions}"
                )
                continue

            # 2.) Validate and deserialize
            # model_validate() handles Finnhub's aliased keys (s, p, v, t, c).
            try:
                trade = Trade.model_validate(item)
            except Exception:
                logger.exception(f"Validation failed for item: {item}")
                continue  # skip bad item, process the rest of the batch

            # 3.) In-memory deduplication
            # First line of defense against Finnhub retransmissions.
            # DB-level ON CONFLICT handles cross-restart duplicates.
            if _is_duplicate(trade.trade_hash):
                logger.debug(
                    f"Duplicate skipped: symbol={trade.symbol} "
                    f"hash={trade.trade_hash[:12]}…"
                )
                continue

            logger.debug(
                f"Trade accepted: symbol={trade.symbol} "
                f"price={trade.price} qty={trade.quantity} "
                f"trade_time={trade.trade_time.isoformat()}"
            )

            # 4.) Produce to Kafka and persist to DB in parallel
            # Neither operation depends on the result of the other.
            # asyncio.gather() runs both concurrently, halving the I/O wait time.
            # Each helper catches its own exceptions so one failure does not
            # cancel the other (independent side effects).
            await asyncio.gather(
                _produce_normalized_trade(producer, trade),
                _ingest_into_db(pool, trade),
            )

    return handle_message


async def _produce_normalized_trade(producer: AIOKafkaProducer, trade: Trade) -> None:
    """
    Serializes the normalized Trade and produces it to the downstream Kafka topic.

    Key detail: model_dump(mode='json') is required here — not model_dump().
    The Trade model has a @computed_field `trade_time` which is a Python datetime
    object. Plain model_dump() returns the raw datetime, which json.dumps() inside
    send_message() cannot serialize (raises TypeError). mode='json' converts it to
    an ISO 8601 string, which is valid JSON and can be parsed by any downstream consumer.

    The symbol is used as the Kafka message key so that all trades for the same
    symbol are routed to the same partition, preserving per-symbol message ordering
    for the downstream candle builder.
    """
    topic = config.KAFKA_TOPIC_NORMALIZED_TRADES
    try:
        await send_message(
            producer,
            topic=topic,
            key=trade.symbol,               # partition key — guarantees ordering per symbol
            value=trade.model_dump(mode='json'),  # datetime → ISO string
        )
        logger.debug(f"Trade produced to '{topic}': symbol={trade.symbol}")
    except Exception:
        logger.exception(
            f"Failed to produce trade to '{topic}': "
            f"symbol={trade.symbol} trade_time={trade.trade_time.isoformat()}"
        )
        # Do not re-raise — DB insert must still proceed independently.


async def _ingest_into_db(pool: asyncpg.Pool, trade: Trade) -> None:
    """
    Inserts the normalized trade into the raw_trades hypertable.

    Two-layer deduplication:
    - Layer 1 (in-memory, above): catches within-session duplicates with zero I/O.
    - Layer 2 (ON CONFLICT here): catches cross-restart duplicates atomically.
      If the service restarts mid-batch, Kafka redelivers uncommitted messages.
      ON CONFLICT DO NOTHING ensures those redelivered trades are silently skipped
      rather than causing UniqueViolationError exceptions.

    Note on trade_time: trade.trade_time is already a timezone-aware datetime
    (UTC). asyncpg maps Python datetime → PostgreSQL TIMESTAMPTZ natively.
    No to_timestamp() or manual conversion is needed.

    Note on conditions: trade.conditions is list[str] | None.
    asyncpg maps Python list[str] → PostgreSQL TEXT[] natively.
    """
    INSERT_QUERY = """
        INSERT INTO raw_trades (symbol, price, volume, trade_time, conditions, source)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (symbol, trade_time, price, volume) DO NOTHING
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                INSERT_QUERY,
                trade.symbol,
                trade.price,
                trade.quantity,
                trade.trade_time,    # timezone-aware datetime → TIMESTAMPTZ
                trade.conditions,    # list[str] | None → TEXT[]
                "finnhub",
            )
        logger.debug(
            f"Trade persisted: symbol={trade.symbol} "
            f"trade_time={trade.trade_time.isoformat()}"
        )
    except Exception:
        logger.exception(
            f"Failed to persist trade to DB: "
            f"symbol={trade.symbol} trade_time={trade.trade_time.isoformat()}"
        )
        # Do not re-raise — Kafka produce must still proceed independently.
