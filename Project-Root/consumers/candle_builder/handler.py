import json
import logging
import asyncpg
import asyncio
from datetime import timedelta, datetime, timezone
from aiokafka import AIOKafkaProducer
# from ..normalizer.models import Trade
from .models import Candle, NormalizedTrade
from config.config import Config
from messaging.kafka_service.service import send_message

config = Config()
logger = logging.getLogger(__name__)

BUCKET_EXPIRY_SECONDS = 60
GRACE_PERIOD_SECONDS = 30
BUCKET_TS = dict()

def make_handler(pool, producer):
    """
    Factory function that creates the message handler with bound resources.

    This pattern allows us to inject shared dependencies (DB pool, Kafka producer)
    into the handler without making them global variables or passing them around
    explicitly in the consumer loop. The returned handle_message function has
    access to these resources via closure scope.
    """

    async def handle_message(msg):
       
        raw = msg.value.decode("utf-8")
        try:
           logger.debug(f"Received raw message: {raw}")
           payload = json.loads(raw)
           trade = NormalizedTrade(**payload)
        except Exception as e:
           logger.exception(f"Failed to parse message: {e} raw={raw}")
           return
       
        # Extract the time bucket for this trade (e.g. 12:00:00, 12:01:00, etc.)
        # We are rounding down the trade time to the nearest minute to create 1-minute candles.
        bucket_time = datetime.fromtimestamp(trade.timestamp / 1000, tz=timezone.utc).replace(second=0, microsecond=0)
        
        # Extract the trade symbol and price for easier access
        symbol = trade.symbol
        price = trade.price

        # Emit the previous bucket if it is still in memory and has expired
        previous_bucket_time = bucket_time - timedelta(minutes=1)
        prev_bucket = BUCKET_TS.get(symbol, {}).get(previous_bucket_time, None)

        if prev_bucket is not None:

            # Mark the previous bucket as complete before emitting it
            BUCKET_TS[symbol][previous_bucket_time]['is_partial'] = False

            candle = emit_candle(BUCKET_TS[symbol][previous_bucket_time])
            await produce_candle(producer, candle)
            await _ingest_into_db(pool, candle)
            del BUCKET_TS[symbol][previous_bucket_time]
        
        if symbol not in BUCKET_TS:
            BUCKET_TS[symbol] = {}
        if bucket_time not in BUCKET_TS[symbol]:
            BUCKET_TS[symbol][bucket_time] = {
                'symbol': symbol,
                'resolution': '1m',
                'open':   price,
                'high':   price,
                'low':    price,
                'close':  price,
                'volume': trade.quantity,
                'trade_count': 1,
                'vwap':   price * trade.quantity,  # Volume Weighted Average Price numerator
                'is_partial': True,  # Indicates if the candle is still being built
                'open_time': bucket_time,
                'close_time': bucket_time + timedelta(minutes=1),
            }
        else:
            candle['symbol'] = symbol
            candle['resolution'] = '1m'
            candle = BUCKET_TS[symbol][bucket_time]
            candle['high'] = max(candle['high'], price)
            candle['low'] = min(candle['low'], price)
            candle['close'] = price # Update the close price to the latest trade price we have received
            candle['volume'] += trade.quantity
            candle['trade_count'] += 1
            candle['vwap'] += price * trade.quantity  # Update VWAP numerator
            candle['is_partial'] = True  # Candle is still being built
            candle['open_time'] = bucket_time
            candle['close_time'] = bucket_time + timedelta(minutes=1)  # Update close time to the end of the bucket
            # Note: Here, we don't touch the open price here as it should have been set when the bucket was first created.

    return handle_message



def emit_candle(trade: dict) -> Candle:
    """
    Converts a Trade object into a Candle object for downstream processing.

    Args:
        trade: The Trade instance to convert.
    """
    # Calculate VWAP (Volume Weighted Average Price)
    vwap = trade['vwap'] / trade['volume'] if trade['volume'] > 0 else 0
    trade['vwap'] = vwap  # Store the calculated VWAP in the trade dictionary
    candle = Candle(**trade)
    return candle

async def sweep_old_candle(producer: AIOKafkaProducer, pool: asyncpg.Pool) -> None:
    """
    Sweeps old candles from the in-memory bucket and emits them if they are complete.

    Args:
        producer: The Kafka producer instance.
        pool: The asyncpg pool instance for database operations.
    """
    while True:
        logger.info(f"Sweeping old candles from in-memory bucket... \n\n {BUCKET_TS}")
        for symbol, buckets in list(BUCKET_TS.items()):
            for bucket_time, candle_data in list(buckets.items()):
                if not candle_data['is_partial']:
                    continue # Skip partial candles; we only want to emit complete ones
                candle_age = (datetime.now(timezone.utc) - bucket_time).total_seconds() 
                if candle_age > (BUCKET_EXPIRY_SECONDS + GRACE_PERIOD_SECONDS):
                    candle['is_partial'] = False  # Mark the candle as complete before emitting it
                    candle = emit_candle(candle_data)
                    
                    # Pushing the candle's info to kafka and ingesting in DB
                    # Note - Added some explanation of goroutines and gather() function in Readme file. Please refer to that for more details.
                    produce_candle_task = produce_candle(producer, candle)
                    ingest_db_candle_task = _ingest_into_db(pool, candle)
                    await asyncio.gather(produce_candle_task, ingest_db_candle_task)
                    del BUCKET_TS[symbol][bucket_time]
        await asyncio.sleep(5)  # Sleep briefly to avoid tight loop and allow other tasks to run    

async def produce_candle(producer: AIOKafkaProducer, trade: Candle) -> None:
    """
    Serializes the normalized Candle and produces it to the downstream Kafka topic.

    Key detail: model_dump(mode='json') is required here — not model_dump().
    The Candle model has a @computed_field `trade_time` which is a Python datetime
    object. Plain model_dump() returns the raw datetime, which json.dumps() inside
    send_message() cannot serialize (raises TypeError). mode='json' converts it to
    an ISO 8601 string, which is valid JSON and can be parsed by any downstream consumer.

    The symbol is used as the Kafka message key so that all trades for the same
    symbol are routed to the same partition, preserving per-symbol message ordering
    for the downstream candle builder.
    """
    topic = config.KAFKA_TOPIC_CANDLES
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
            f"symbol={trade.symbol} trade_time={trade.open_time.isoformat()}"
        )

async def _ingest_into_db(pool: asyncpg.Pool, candle: Candle) -> None:
    """
    Inserts the candle into the candles hypertable.

    Two-layer deduplication:
    - Layer 1 (in-memory, above): catches within-session duplicates with zero I/O.
    - Layer 2 (ON CONFLICT here): catches cross-restart duplicates atomically.
      If the service restarts mid-batch, Kafka redelivers uncommitted messages.
      ON CONFLICT DO NOTHING ensures those redelivered candles are silently skipped
      rather than causing UniqueViolationError exceptions.

    Note on open_time: candle.open_time is already a timezone-aware datetime
    (UTC). asyncpg maps Python datetime → PostgreSQL TIMESTAMPTZ natively.
    No to_timestamp() or manual conversion is needed.

    Note on conditions: candle.conditions is list[str] | None.
    asyncpg maps Python list[str] → PostgreSQL TEXT[] natively.
    """
    INSERT_QUERY = """
        INSERT INTO candles (symbol, resolution, open, high, low, close, volume, trade_count, vwap, is_partial, open_time, close_time)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (symbol, resolution, open_time) DO NOTHING
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                    INSERT_QUERY,
                    candle.symbol,
                    candle.resolution,
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    candle.volume,
                    candle.trade_count,
                    candle.vwap,
                    candle.is_partial,
                    candle.open_time,
                    candle.close_time,
            )
        logger.debug(
            f"Candle persisted: symbol={candle.symbol} "
            f"open_time={candle.open_time.isoformat()}"
        )
    except Exception:
        logger.exception(
            f"Failed to persist candle to DB: "
            f"symbol={candle.symbol} open_time={candle.open_time.isoformat()}"
        )
        raise
