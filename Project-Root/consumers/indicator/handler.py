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
            raise e

        # Compute indicators for the candle
        indicator = await compute_indicators(pool, candle)
        await asyncio.gather(
            ingest_into_db(pool, indicator),
            produce_indicator(producer, indicator)
        )
    
    return handle_message



async def compute_indicators(pool, candle: Candle) -> Indicator:
    """
    Computes various indicators for the given candle.
    This is a placeholder function. Actual implementation will depend on the specific indicators
    you want to compute (e.g., SMA, EMA, RSI, MACD, Bollinger Bands, etc.).
    """

    # fetch_candle_from_db:
    # Fetching historical candles from the database for the same symbol and resolution
    # Fetching the last 200 candles for the same symbol and resolution to compute indicators
    # Selecting 200 because 200 is the max number of candles needed to compute the 200-period SMA/EMA, 
    # hence in single query we can fetch all the required candles for all indicators.
    # Returns a list of Candle objects sorted by open_time in descending order (most recent first).

    # fetch_indicators_from_db:
    # Fetching the last computed indicators from the database for the same symbol and resolution
    
    historical_candles, historical_indicators = await asyncio.gather(
            fetch_candle_from_db(pool, candle.symbol, candle.resolution, candle.open_time, 200),
            fetch_indicators_from_db(pool, candle.symbol, candle.resolution, candle.open_time, 1),
    )
    
    candle_closes = [candle.close] + [c.close for c in historical_candles]    
    closes_oldest_first=None

    # 1.) ************************** Small Moving Averages (SMA) Calculations **************************
    sma_9 = sum(candle_closes[:9]) / 9 if len(candle_closes) >= 9 else None
    sma_21 = sum(candle_closes[:21]) / 21 if len(candle_closes) >= 21 else None
    sma_50 = sum(candle_closes[:50]) / 50 if len(candle_closes) >= 50 else None
    sma_200 = sum(candle_closes[:200]) / 200 if len(candle_closes) >= 200 else None



    # 2.) ************************** Exponential Moving Averages (EMA) Calculations **************************

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


    alpha = 2 / (12 + 1)
    if historical_indicators and historical_indicators.ema_12 is not None:
        ema_12 = (candle.close * alpha) + (historical_indicators.ema_12 * (1 - alpha))
    else:
        if not closes_oldest_first:
            closes_oldest_first = list(reversed(candle_closes))
        ema_12 = compute_ema(closes_oldest_first, 12)

    alpha = 2 / (26 + 1)
    if historical_indicators and historical_indicators.ema_26 is not None:
        ema_26 = (candle.close * alpha) + (historical_indicators.ema_26 * (1 - alpha))
    else:
        if not closes_oldest_first:
            closes_oldest_first = list(reversed(candle_closes))
        ema_26 = compute_ema(closes_oldest_first, 26)


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



    # 3.) ************************** Relative Strength Index (RSI) Calculations **************************
    if closes_oldest_first is None:
        closes_oldest_first = list(reversed(candle_closes))

    # passing the last 15 closes to compute RSI for period 14, because we need the change between the current close and the previous close, 
    # hence we need one more candle than the period.
    if historical_indicators and historical_indicators.rsi_avg_gain_14 is not None and historical_indicators.rsi_avg_loss_14 is not None:
        rsi_avg_gain_14 = (historical_indicators.rsi_avg_gain_14 * (14 - 1) + (candle.close - closes_oldest_first[-2] if candle.close > closes_oldest_first[-2] else 0)) / 14
        rsi_avg_loss_14 = (historical_indicators.rsi_avg_loss_14 * (14 - 1) + (closes_oldest_first[-2] - candle.close if candle.close < closes_oldest_first[-2] else 0)) / 14
        rs = rsi_avg_gain_14 / rsi_avg_loss_14 if rsi_avg_loss_14 != 0 else float('inf')
        rsi_14 = 100 - (100 / (1 + rs)) if rsi_avg_loss_14 != 0 else 100.0
    else:
        rsi_avg_gain_14, rsi_avg_loss_14, rsi_14 = compute_rsi(closes_oldest_first, 14) if len(candle_closes) >= 16 else (None, None, None)


    
    # 4.) ************************** Moving Average Convergence Divergence (MACD) Calculations **************************
    # MACD is calculated by subtracting the 26-period EMA from the 12-period EMA. The signal line is a 9-period EMA of the MACD line.
    # MACD = EMA_12 - EMA_26
    # Signal Line = EMA_9 of MACD
    # MACD Histogram = MACD - Signal Line

    macd_line = None
    macd_signal = None
    macd_histogram = None

    if ema_12 is not None and ema_26 is not None:
        macd_line = ema_12 - ema_26

    if historical_indicators and historical_indicators.macd_signal is not None and macd_line is not None:
        alpha = 2/(9 + 1)
        macd_signal = alpha * macd_line + (1 - alpha) * historical_indicators.macd_signal
    else:
        # Cold start, we need to compute the MACD signal line from the historical MACD values.
        if closes_oldest_first is None:
            closes_oldest_first = list(reversed(candle_closes))

        macd_series = compute_macd(closes_oldest_first)
        macd_signal = compute_ema(macd_series, 9) if len(macd_series) >= 9 else None
    
    macd_histogram = macd_line - macd_signal if macd_line is not None and macd_signal is not None else None



    # 5.) ************************** Bollinger Bands (BB) Calculations **************************
    if closes_oldest_first is None:
        closes_oldest_first = list(reversed(candle_closes))
    mean = sum(closes_oldest_first[-20:]) / 20 if len(closes_oldest_first) >= 20 else None
    std_dev = (sum((x - mean) ** 2 for x in closes_oldest_first[-20:]) / 20) ** 0.5 if mean is not None else None
    bb_upper = mean + 2 * std_dev if mean is not None and std_dev is not None else None
    bb_middle = mean
    bb_lower = mean - 2 * std_dev if mean is not None and std_dev is not None else None
    bb_bandwidth = (bb_upper - bb_lower) / bb_middle if bb_upper is not None and bb_lower is not None and bb_middle else None



    # 6.) ************************** VWAP Session (VWAP) Calculations **************************
    # Formula for VWAP = (Cumulative (Price * Volume)) / (Cumulative Volume)
    # VWAP = Σ(price × volume) / Σ(volume)
    is_new_session = (
        historical_indicators is None or
        historical_indicators.open_time.date() != candle.open_time.date() or
        historical_indicators.vwap_numerator is None   # handles rows stored before VWAP was implemented
        # We are checking only numerator because if numerator is None, then denominator will also be None, hence we don't need to check for denominator.
    )
    if is_new_session:
        # First candle of today, it will start fresh
        new_numerator   = candle.vwap * candle.volume
        new_denominator = candle.volume
    else:
        # its a candle of the same day, just accumulate
        # The candle.vwap is (price * volume) / volume)
        # however for Session VWAP numerator we need cummulative (Price * Volume) since there we have divided by volume now here we are multiplying by volume,
        # we're undoing the division that happened in the candle builder to get back to the raw dollar-volume. 
        # Hence we get back the numerator of (Price * Volume) for this candle.
        new_numerator   = historical_indicators.vwap_numerator   + (candle.vwap * candle.volume)
        new_denominator = historical_indicators.vwap_denominator + candle.volume

    vwap_session = new_numerator / new_denominator if new_denominator > 0 else None


    indicator = Indicator(
        symbol=             candle.symbol,
        resolution=         candle.resolution,
        open_time=          candle.open_time,
        close_price=        candle.close,
        vwap_session=       vwap_session,
        vwap_numerator=     new_numerator,
        vwap_denominator=   new_denominator,
        sma_9=              sma_9,
        sma_21=             sma_21,
        sma_50=             sma_50,
        sma_200=            sma_200,
        ema_9=              ema_9,
        ema_12=             ema_12,
        ema_21=             ema_21,
        ema_26=             ema_26,
        ema_50=             ema_50,
        ema_200=            ema_200,
        rsi_14=             rsi_14,
        rsi_avg_gain_14=    rsi_avg_gain_14,
        rsi_avg_loss_14=    rsi_avg_loss_14,
        macd_line=          macd_line,
        macd_signal=        macd_signal,
        macd_histogram=     macd_histogram,
        bb_upper=           bb_upper,
        bb_middle=          bb_middle,
        bb_lower=           bb_lower,
        bb_bandwidth=       bb_bandwidth
    )

    logger.debug(f"\n\n Computed indicators for symbol={candle.symbol} open_time={candle.open_time.isoformat()}: {indicator}")
    return indicator
     


def compute_macd(closes_oldest_first: list[float]) -> list[float]:
    # Computing the EMA for 12 and 26 periods to calculate the MACD line

    
    if len(closes_oldest_first) < 26:
        return []

    macd_series = []
    alpha_12 = 2/(12 + 1)
    alpha_26 = 2/(26 + 1)

    # Seed EMA(12) from the first 12 closes
    ema_12 = sum(closes_oldest_first[:12]) / 12

    # Rolling this ema_12 forward for the next 14 closes to get the EMA(12) for the 26th close, 
    # so we can compute the MACD line for the 26th close. 
    for price in closes_oldest_first[12:26]:
        ema_12 = alpha_12 * price + (1 - alpha_12) * ema_12

    # Seed EMA(26) from the first 26 closes
    ema_26 = sum(closes_oldest_first[:26]) / 26

    # Now we have both EMA(12) and EMA(26) for the 26th close, we can compute the MACD line for the 26th close.
    # Even if we don't add this to the macd series, we will still be computing the correct MACD line from the 27th close onwards,
    # just adding it for more warm up (smoothing).
    macd_series.append(ema_12 - ema_26)

    # Both EMAs are now at the same candle (index 25). 
    # we wil start computing the MACD line from the index 26 onwards, that will be 27th close
    for price in closes_oldest_first[26:]:
        ema_12 = alpha_12 * price + (1 - alpha_12) * ema_12
        ema_26 = alpha_26 * price + (1 - alpha_26) * ema_26
        macd_line = ema_12 - ema_26

        # Store or process the macd_line as needed
        macd_series.append(macd_line)
    return macd_series
    
    



# Wilder's RSI calculation method is used here, which is a smoothed version of the original RSI calculation.
def compute_rsi(closes_oldest_first: list[float], period: int) -> list[float | None]:
    """
    Computes the Relative Strength Index (RSI) for a given list of closing prices and a specified period.

    Returns the RSI value as a float if computable, otherwise returns None.
    Document from where I understood RSI: https://blog.quantinsti.com/rsi-indicator/
    """
    # Why period + 1? Because we need to calculate the change between the current close and the previous close, hence we need one more candle than the period.
    if len(closes_oldest_first) < (period + 1):
        logger.debug(f"There are not enough {period} candles, hence skipping calculating RSI")
        return (None, None, None)

    # We will store the avg gain in the DB and check if exists, if exists then we will use that to calculate the next RSI, else we will calculate the first RSI from the closes.
    # Placeholder

    gains = []
    losses = []
    
    # Note: The len of gains and losses will be period i.e., 14 for standard RSI period because it is a change from the current candle to its previous candle
    for idx in range(1, len(closes_oldest_first)):
        change = closes_oldest_first[idx] - closes_oldest_first[idx - 1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))
    

    # ********************************************************************************************************************
    # We use Wilder's RSI which uses a smoothed moving average of gains and losses. The first average gain and loss are simple averages,
    # and subsequent averages are smoothed.
    
    # Note:
    # There are two common methods to calculate RSI: the Cutler's method and Wilder's method.
    # The Cutler method uses a simple moving average of gains and losses, while Wilder's method uses a smoothed moving average.
    # Wilder's RSI is used by most trading platforms like TradingView, Bloomberg, etc. and is considered more reliable than the simple RSI calculation.
    # Hence, we are using Wilder's RSI calculation method here.

    # Wilder's RSI calculation formula is:
    # average_gain = (previous_average_gain * (period - 1) + current_gain) / period
    # average_loss = (previous_average_loss * (period - 1) + current_loss) / period
    # RSI = 100 - (100 / (1 + RS)), where RS = average_gain / average_loss
    # ********************************************************************************************************************
    
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    # Wilder smoothing on everything beyond the seed window
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period


    # If average_loss is 0, RSI is defined to be 100 (no losses).
    # Because RSI = 100 - (100 / (1 + RS)), and if average_loss is 0, RS becomes infinite, making RSI = 100.
    if avg_loss == 0:
        return avg_gain, 0.0, 100.0  # RSI is 100 if there are no losses

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return avg_gain, avg_loss, rsi


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
                logger.debug(f"Fetched {len(result)} Candle's indicator records from DB for symbol={symbol}, resolution={resolution}, timestamp<{timestamp}")
                logger.debug(f"Fetched indicators: {result} \n\n")
                return Indicator(**dict(result))
            return None
        except Exception as e:
            logger.exception(f"Database query failed:{e}, Query={DB_QUERY}, parameters=({symbol}, {resolution}, {timestamp}, {max_candles_to_fetch})")
            raise



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
            logger.debug(f"Fetched candles: {result} \n\n")
            if result:
               return [Candle(**dict(row)) for row in result]
            return []
        except Exception as e:
            # → DB fails
            # → exception logged
            # → raise propagates to compute_indicators
            # → propagates to handle_message (no catch there)
            # → base consumer catches it → routes original Kafka message to DLT
            # → when DB recovers, DLT replays the message correctly

            logger.exception(f"Database query failed:{e}, Query={DB_QUERY}, parameters=({symbol}, {resolution}, {timestamp}, {max_candles_to_fetch})")
            raise


async def ingest_into_db(pool: asyncpg.Pool, indicator: Indicator) -> None:
        """
        Inserts the Indicator into the Indicator hypertable.
        """

        INSERT_QUERY="""      
        INSERT INTO indicators (symbol, resolution, open_time, close_price, vwap_session, vwap_numerator, vwap_denominator, sma_9, sma_21, sma_50, sma_200, ema_9, ema_12, ema_26, ema_21, ema_50, ema_200, rsi_14, rsi_avg_gain_14, rsi_avg_loss_14, macd_line, macd_signal, macd_histogram, bb_upper, bb_middle, bb_lower, bb_bandwidth)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)
        ON CONFLICT (symbol,resolution,open_time) DO NOTHING
        """
        try:
            async with pool.acquire() as conn:
                  await conn.execute(
                      INSERT_QUERY,
                      indicator.symbol,
                      indicator.resolution,
                      indicator.open_time,
                      indicator.close_price,
                      indicator.vwap_session,
                      indicator.vwap_numerator,
                      indicator.vwap_denominator,
                      indicator.sma_9,
                      indicator.sma_21,
                      indicator.sma_50,
                      indicator.sma_200,
                      indicator.ema_9,
                      indicator.ema_12,
                      indicator.ema_26,
                      indicator.ema_21,
                      indicator.ema_50,
                      indicator.ema_200,
                      indicator.rsi_14,
                      indicator.rsi_avg_gain_14,
                      indicator.rsi_avg_loss_14,
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
