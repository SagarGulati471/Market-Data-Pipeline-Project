import json
import logging
import asyncpg
import asyncio
from aiokafka import AIOKafkaProducer

from config.config import Config
from .models import Indicator
from ..candle_builder.models import Candle
from messaging.kafka_service.service import send_message

config = Config()
logger = logging.getLogger(__name__)


def make_handler(pool, producer):
    """
    Factory function that creates the message handler with bound resources.

    This pattern allows us to inject shared dependencies (DB pool, Kafka producer)
    into the handler without making them global variables or passing them we
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

        # Compute indicators for the candle
        indicator = await compute_indicators(pool, candle)
        await ingest_into_db(pool, indicator)
        await produce_indicator(producer, indicator)
    
    return handle_message



async def compute_indicators(pool, candle: Candle) -> Indicator:
    """
    Computes various indicators for the given candle.
    This is a placeholder function. Actual implementation will depend on the specific indicators
    you want to compute (e.g., SMA, EMA, RSI, MACD, Bollinger Bands, etc.).
    """

    # Fetching historical candles from the database for the same symbol and resolution
    # Fetching the last 200 candles for the same symbol and resolution to compute indicators
    # Selecting 200 because 200 is the max number of candles needed to compute the 200-period SMA/EMA, 
    # hence in single query we can fetch all the required candles for all indicators.
    # Returns a list of Candle objects sorted by open_time in descending order (most recent first).
    
    historical_candles = await fetch_candle_from_db(
        pool,
        candle.symbol,
        candle.resolution,
        candle.open_time,
        200
    )
    candle_closes = [candle.close] + [c.close for c in historical_candles]

    # Fetching historical indicators from the database for the same symbol, resolution, and timestamp
    # Fetching the last indicator for the same symbol and resolution to compute EMA
    historical_indicators = await fetch_indicators_from_db(
        pool,
        candle.symbol,
        candle.resolution,
        candle.open_time,
        1
    )

    closes_oldest_first=None

    # 1.)  Small Moving Averages (SMA) Calculations
    sma_9 = sum(candle_closes[:9]) / 9 if len(candle_closes) >= 9 else None
    sma_21 = sum(candle_closes[:21]) / 21 if len(candle_closes) >= 21 else None
    sma_50 = sum(candle_closes[:50]) / 50 if len(candle_closes) >= 50 else None
    sma_200 = sum(candle_closes[:200]) / 200 if len(candle_closes) >= 200 else None


    # 2.) Exponential Moving Averages (EMA) Calculations

    # Formula for EMA = α * (Current Price) + (1 - α) * (Previous EMA)
    # where α = 2 / (N + 1), and N is the number of periods (e.g., 9, 21, 50, 200).
    # Or 
    # (Current Price * (2 / (N + 1))) + (Previous EMA * (1 - (2 / (N + 1))))

    # Calculating EMA_9
    alpha = 2 / (9 + 1)
    if historical_indicators and historical_indicators.ema_9 is not None:
        ema_9 = (candle.close * alpha) + (historical_indicators.ema_9 * (1 - alpha))
    else:
        if not closes_oldest_first:  # If we need it then only we reverse the list, otherwise we can avoid this extra operation.
                                     # Reverse once, reuse for all four
            closes_oldest_first = list(reversed(candle_closes))
        ema_9 = compute_ema(closes_oldest_first, 9)


    # Calculating EMA_21
    alpha = 2 / (21 + 1)
    if historical_indicators and historical_indicators.ema_21 is not None:
        ema_21 = (candle.close * alpha) + (historical_indicators.ema_21 * (1 - alpha))
    else:
        if not closes_oldest_first:
            closes_oldest_first = list(reversed(candle_closes))
        ema_21 = compute_ema(closes_oldest_first, 21)


    # Calculating EMA_50
    alpha = 2 / (50 + 1)
    if historical_indicators and historical_indicators.ema_50 is not None:
        ema_50 = (candle.close * alpha) + (historical_indicators.ema_50 * (1 - alpha))
    else:
        if not closes_oldest_first:
            closes_oldest_first = list(reversed(candle_closes))
        ema_50 = compute_ema(closes_oldest_first, 50)


    # Calculating EMA_200
    alpha = 2 / (200 + 1)
    if historical_indicators and historical_indicators.ema_200 is not None:
        ema_200 = (candle.close * alpha) + (historical_indicators.ema_200 * (1 - alpha))
    else:
        if not closes_oldest_first:
            closes_oldest_first = list(reversed(candle_closes))
        ema_200 = compute_ema(closes_oldest_first, 200)


    indicator = Indicator(
        symbol=candle.symbol,
        resolution=candle.resolution,
        open_time=candle.open_time,
        vwap_session=0.0,  # Placeholder
        sma_9 =             sma_9,
        sma_21 =            sma_21,
        sma_50 =            sma_50,
        sma_200 =           sma_200,
        ema_9 =             ema_9,
        ema_21 =            ema_21,
        ema_50  =           ema_50,
        ema_200 =           ema_200,
        rsi_14=0.0,        # Placeholder
        macd_line=0.0,     # Placeholder
        macd_signal=0.0,   # Placeholder
        macd_histogram=0.0,# Placeholder
        bb_upper=0.0,      # Placeholder (To be implemented later)
        bb_middle=0.0,     # Placeholder
        bb_lower=0.0,      # Placeholder
        bb_bandwidth=0.0   # Placeholder
    )
    return indicator
     

def compute_ema(closes_oldest_first: list[float], period: int) -> float | None:
    """
    Computes the Exponential Moving Average (EMA) for a given list of closing prices and a specified period.

    """

    if len(closes_oldest_first) < period:
        return None
    
    alpha = 2 / (period + 1)
    
    # Seed with SMA of first N values
    ema = sum(closes_oldest_first[:period]) / period
    
    # Apply EMA recursively on remaining values
    for price in closes_oldest_first[period:]:
        ema = alpha * price + (1 - alpha) * ema
    
    return ema


# Fetch indicators from the database for a given symbol, resolution, and timestamp
async def fetch_indicators_from_db(pool, symbol, resolution, timestamp, max_candles_to_fetch = 1):
    """
    Fetch a candle from the database based on the provided symbol, resolution, and timestamp.
    Returns a Candle object if found, otherwise returns None.
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
                logger.debug(f"Fetched {len(result)} Candle's indicator records from DB for symbol={symbol}, resolution={resolution}, timestamp<{timestamp}")
                logger.debug(f"Fetched indicators: {result}")
                return Indicator(**dict(result))
            return None
        except Exception as e:
            logger.exception(f"Database query failed:{e}, Query={DB_QUERY}, parameters=({symbol}, {resolution}, {timestamp}, {max_candles_to_fetch})")
            return None
            


# Async function to fetch a candle from the database
async def fetch_candle_from_db(pool, symbol, resolution, timestamp, max_candles_to_fetch = 200):
    """
    Fetch a candle from the database based on the provided symbol, resolution, and timestamp.
    Returns a Candle object if found, otherwise returns None.
    """
    async with pool.acquire() as connection:

        try:

            # *********************** Note on SQL Injection Prevention ***********************
            # Initially I thought of constructing the query string directly, but using parameterized queries
            # is safer and avoids SQL injection risks.
            # Eg: The SQL injection can happen like -
            # symbol = "'; DROP TABLE candles; --"

            # Using parameterized queries (with $1, $2, etc.) is a best practice to prevent SQL injection attacks.
            # It ensures that user input is treated as data, not executable code.
            # This is especially important in applications that interact with databases, as it helps maintain the integrity and security


            # DB_QUERY = f"""SELECT * FROM candles
            # WHERE symbol = '{symbol}' AND resolution = '{resolution}' AND timestamp < '{timestamp}'
            # ORDER BY timestamp DESC
            # LIMIT {max_candles_to_fetch}"""
            # logger.debug(f"Executing DB query: {DB_QUERY}")

            DB_QUERY = """
                SELECT *
                FROM candles
                WHERE symbol = $1
                AND resolution = $2
                AND open_time < $3
                ORDER BY open_time DESC
                LIMIT $4
                """

            result = await connection.fetch(
                DB_QUERY,
                symbol,
                resolution,
                timestamp,
                max_candles_to_fetch
            )
            logger.debug(f"Fetched {len(result)} candles from DB for symbol={symbol}, resolution={resolution}, timestamp<{timestamp}")
            logger.debug(f"Fetched candles: {result}")
            if result:
               return [Candle(**dict(row)) for row in result]
            return []
        except Exception as e:
            logger.exception(f"Database query failed:{e}, Query={DB_QUERY}, parameters=({symbol}, {resolution}, {timestamp}, {max_candles_to_fetch})")
            return []


async def ingest_into_db(pool: asyncpg.Pool, indicator: Indicator) -> None:
        """
        Inserts the Indicator into the Indicator hypertable.
        """

        INSERT_QUERY="""      
        INSERT INTO indicator (symbol, resolution, open_time, vwap_session, sma_9, sma_21, sma_50, sma_200, ema_9, ema_21, ema_50, ema_200, rsi_14, macd_line, macd_signal, macd_histogram, bb_upper, bb_middle, bb_lower, bb_bandwidth)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
        ON CONFLICT (symbol,resolution,open_time) DO NOTHING
        """
        try:
            async with pool.acquire() as conn:
                  await conn.execute(
                      INSERT_QUERY,
                      indicator.symbol,
                      indicator.resolution,
                      indicator.open_time,
                      indicator.vwap_session,
                      indicator.sma_9,
                      indicator.sma_21,
                      indicator.sma_50,
                      indicator.sma_200,
                      indicator.ema_9,
                      indicator.ema_21,
                      indicator.ema_50,
                      indicator.ema_200,
                      indicator.rsi_14,
                      indicator.macd_line,
                      indicator.macd_signal,
                      indicator.macd_histogram,
                      indicator.bb_upper,
                      indicator.bb_middle,
                      indicator.bb_lower,
                      indicator.bb_bandwidth,
                  )
            logger.debug(
                f"Indicator persisted: symbol={indicator.symbol} "
                f"open_time={indicator.open_time.isoformat()}"
          )
        except Exception as w:
            logger.exception(
                f"Failed to persist indicator to DB: "
                f"symbol={indicator.symbol} open_time={indicator.open_time.isoformat()}"
            )
            raise



async def produce_indicator(producer: AIOKafkaProducer, indicator: Indicator) -> None:
    """
    Serializes the normalized Indicator and produces it to the downstream Kafka topic.

    Key detail: model_dump(mode='json') is required here — not model_dump().
    The Indicator model has a @computed_field `open_time` which is a Python datetime
    object. Plain model_dump() returns the raw datetime, which json.dumps() inside
    send_message() cannot serialize (raises TypeError). mode='json' converts it to
    an ISO 8601 string, which is valid JSON and can be parsed by any downstream consumer.

    The symbol is used as the Kafka message key so that all indicators for the same
    symbol are routed to the same partition, preserving per-symbol message ordering
    for the downstream consumers.
    """
    topic = config.KAFKA_TOPIC_INDICATOR
    try:
        await send_message(
            producer,
            topic=topic,
            key=indicator.symbol,               # partition key — guarantees ordering per symbol
            value=indicator.model_dump(mode='json'),  # datetime → ISO string
        )
        logger.debug(f"Indicator produced to '{topic}': symbol={indicator.symbol}")
    except Exception:
        logger.exception(
            f"Failed to produce indicator to '{topic}': "
            f"symbol={indicator.symbol} open_time={indicator.open_time.isoformat()}"
        )
