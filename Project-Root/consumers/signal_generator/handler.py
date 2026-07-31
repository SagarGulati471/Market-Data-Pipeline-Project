import json
import logging
import asyncpg
import asyncio
from aiokafka import AIOKafkaProducer

from config.config import Config
from .models import Signal
from ..indicator.models import Indicator
from messaging.kafka_service.service import send_message

config = Config()
logger = logging.getLogger(__name__)


def make_handler(pool, producer):
    
    async def handle_message(msg):
        # Decoding the message
        raw = msg.value.decode("utf-8")
        try:
            logger.debug(f"Received raw message: {raw}")
            payload = json.loads(raw)
            indicator = Indicator(**payload)
            logger.debug(f"Parsed indicator: {indicator}")
        except Exception as e:
            logger.exception(f"Failed to parse message: {e} raw={raw}")
            raise e # Raising the exception to let the consumer know that this message failed processing and should be sent to the dead letter topic.

        # Fetch the latest indicators from the database for the given symbol, resolution, and timestamp
        prev_indicator = await fetch_indicators_from_db(pool, indicator.symbol, indicator.resolution,
                                                           indicator.open_time)

        # Process the indicator data (e.g., generate signals)
        # Signal will always be either of BUY, SELL, HOLD. 
        signal = await generate_signal(indicator, prev_indicator)

        await asyncio.gather(
            ingest_into_db(pool, signal),
            produce_signal(producer, signal)
        )    
    return handle_message



# Important Note:
# We are not making all the strategies computation as async because they are pure CPU arithmetic and do not involve any I/O operations.
# Making them async would add unnecessary add overhead without any benefit.

# We mainly use async when we have I/O bound operations like database queries, network requests, etc where we have some waiting time for the request to complete.
# In this case, the strategies are purely computational and do not involve any I/O, so making them async would not provide any performance benefit and would only add complexity.

# Additionally, was thinking to run them parallely, but to parallelize we will need to use multiprocessing and
# since loading a task from the process pool is expensive and it will add on to the computation time, it would be better to run them sequentially in the same process.
# Rest explanation is added in the architecure_discussions_with_ai.md file.

# Strategies are pure CPU arithmetic — no I/O, no awaiting. 
# Hence, making the function async adds coroutine overhead for no reason.
def generate_signal(indicator, prev_indicator):
    """
    In this function we will be calculating the signals based on the current and previous indicator values.
    We will calling different strategies to generate signals and then combine them to generate a final signal.
    """
    pass
     
    



# Fetch indicators from the database for a given symbol, resolution, and timestamp
async def fetch_indicators_from_db(pool, symbol, resolution, timestamp, max_candles_to_fetch = 1):
    """
    Fetch an indicator from the database based on the provided symbol, resolution, and timestamp.
    Returns an Indicator object if found, otherwise returns None.
    """
    async with pool.acquire() as connection:

        try:
            DB_QUERY = """
                SELECT *
                FROM indicators
                WHERE symbol = $1
                AND resolution = $2
                AND open_time < $3
                ORDER BY open_time DESC
                LIMIT $4
                """

            result = await connection.fetchrow(
                DB_QUERY,
                symbol,
                resolution,
                timestamp,
                max_candles_to_fetch
            )
            if result:
                logger.debug(f"Fetched indicators: {result} \n\n")
                return Indicator(**dict(result))
            return None
        except Exception as e:
            logger.exception(f"Database query failed:{e}, Query={DB_QUERY}, parameters=({symbol}, {resolution}, {timestamp}, {max_candles_to_fetch})")
            raise




async def ingest_into_db(pool: asyncpg.Pool, signal: Signal) -> None:
        """
        Inserts the Signal into the signals hypertable.
        """

        INSERT_QUERY="""      
        INSERT INTO signals (symbol, resolution, open_time, signal_type, strategy_EMA_crossover, strategy_RSI_reversal, strategy_MACD_crossover, strategy_VWAP_confluence, weighted_score, threshold)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT (symbol,resolution,open_time) DO NOTHING
        """
        try:
            async with pool.acquire() as conn:
                  await conn.execute(
                      INSERT_QUERY,
                      signal.symbol,
                      signal.resolution,
                      signal.open_time,
                      signal.signal_type,
                      signal.strategy_EMA_crossover,
                      signal.strategy_RSI_reversal,
                      signal.strategy_MACD_crossover,
                      signal.strategy_VWAP_confluence,
                      signal.weighted_score,
                      signal.threshold,
                  )
            logger.debug(
                f"Signal persisted: symbol={signal.symbol} "
                f"open_time={signal.open_time}"
          )
        except Exception as w:
            logger.exception(
                f"Failed to persist signal to DB: "
                f"symbol={signal.symbol} open_time={signal.open_time.isoformat()}"
            )
            raise



async def produce_signal(producer: AIOKafkaProducer, signal: Signal) -> None:
    """
    Serializes the Signal and produces it to the downstream Kafka topic.

    Key detail: model_dump(mode='json') is required here — not model_dump().
    The Signal model has a @computed_field `open_time` which is a Python datetime
    object. Plain model_dump() returns the raw datetime, which json.dumps() inside
    send_message() cannot serialize (raises TypeError). mode='json' converts it to
    an ISO 8601 string, which is valid JSON and can be parsed by any downstream consumer.

    The symbol is used as the Kafka message key so that all indicators for the same
    symbol are routed to the same partition, preserving per-symbol message ordering
    for the downstream consumers.
    """
    topic = config.KAFKA_TOPIC_SIGNAL
    try:
        await send_message(
            producer,
            topic=topic,
            key=signal.symbol,               # partition key — guarantees ordering per symbol
            value=signal.model_dump(mode='json'),  # datetime → ISO string
        )
        logger.debug(f"Signal produced to '{topic}': symbol={signal.symbol}")
    except Exception:
        logger.exception(
            f"Failed to produce signal to '{topic}': "
            f"symbol={signal.symbol} open_time={signal.open_time.isoformat()}"
        )
