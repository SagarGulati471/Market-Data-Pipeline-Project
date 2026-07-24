import json
import logging
import asyncpg
import asyncio

from config.config import Config
from .models import Indicator
from ..candle_builder.models import Candle

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




    
    return handle_message


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
            # if result:
            #     return Candle(**result)
            return None
        except Exception as e:
            logger.exception(f"Database query failed:{e}, Query={DB_QUERY}, parameters=({symbol}, {resolution}, {timestamp}, {max_candles_to_fetch})")
            return None


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
