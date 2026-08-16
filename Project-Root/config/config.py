from dotenv import load_dotenv
from dotenv import set_key
import os
import logging
from utils.logger import setup_logger

load_dotenv()  # Must load .env before setup_logger() so LOG_LEVEL is available
setup_logger()
logger = logging.getLogger(__name__)

class Config:
    def __init__(self):
        load_dotenv()
        self.load_config()
        self.envFile = '.env'

    def load_config(self):
        self.CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
        self.SECRET_KEY = os.getenv("FYERS_SECRET_KEY")
        self.AUTH_CODE = os.getenv("FYERS_AUTH_CODE")
        self.ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
        self.REFRESH_TOKEN = os.getenv("FYERS_REFRESH_TOKEN")
        self.REDIRECT_URI = os.getenv("FYERS_REDIRECT_URI")
        self.WEBSOCKET_URL = os.getenv('WEBSOCKET_URL', 'wss://rtsocket-api.fyers.in/versova').strip("'")
        self.SYMBOL_TICKERS = os.getenv('SYMBOL_TICKERS', 'NSE:SBIN- EQ').strip("'").split(',')
        self.WEBSOCKET_MAX_RETRIES = int(os.getenv('WEBSOCKET_MAX_RETRIES', 5))
        self.WEBSOCKET_RETRY_INTERVAL = int(os.getenv('WEBSOCKET_RETRY_INTERVAL', 5))
        self.FINNHUB_API_TOKEN = os.getenv("FINNHUB_API_TOKEN", "")
        self.FINNHUB_WEBSOCKET_URI= os.getenv("FINNHUB_WEBSOCKET_URI", f"wss://ws.finnhub.io?token={self.FINNHUB_API_TOKEN}").strip("'")
        self.FINNHUB_STOCK_SYMBOLS = os.getenv('FINNHUB_STOCK_SYMBOLS', 'BINANCE:BTCUSDT,OANDA:EUR_USD').strip("'").split(',')
        self.KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

        # TOPICS
        self.FINNHUB_KAFKA_TOPIC = os.getenv('FINNHUB_KAFKA_TOPIC', 'market_data')

        # NORMALIZER PIPELINE
        self.DEAD_LETTER_TOPIC_NORMALIZER = os.getenv('DEAD_LETTER_TOPIC_NORMALIZER', 'normalizer-dlt')
        self.KAFKA_TOPIC_NORMALIZED_TRADES = os.getenv('KAFKA_TOPIC_NORMALIZED_TRADES', 'trades-normalized')

        # CANDLE BUILDER PIPELINE
        self.KAFKA_TOPIC_CANDLES = os.getenv('KAFKA_TOPIC_CANDLES', 'candles')
        self.DEAD_LETTER_TOPIC_CANDLE_BUILDER = os.getenv('DEAD_LETTER_TOPIC_CANDLE_BUILDER', 'candle-builder-dlt')

        # INDICATOR PIPELINE
        self.KAFKA_TOPIC_INDICATOR = os.getenv('KAFKA_TOPIC_INDICATOR', 'indicators')
        self.DEAD_LETTER_TOPIC_INDICATOR = os.getenv('DEAD_LETTER_TOPIC_INDICATOR', 'indicator-dlt')

        # SIGNAL GENERATOR PIPELINE
        self.KAFKA_TOPIC_SIGNAL = os.getenv('KAFKA_TOPIC_SIGNAL', 'signals')
        self.DEAD_LETTER_TOPIC_SIGNAL = os.getenv('DEAD_LETTER_TOPIC_SIGNAL', 'signal-dlt')

        # ORDER EXECUTOR PIPELINE
        self.KAFKA_TOPIC_ORDER_EXECUTOR = os.getenv('KAFKA_TOPIC_ORDER_EXECUTOR', 'orders-executor')
        self.DEAD_LETTER_TOPIC_ORDER_EXECUTOR = os.getenv('DEAD_LETTER_TOPIC_ORDER_EXECUTOR', 'orders-executor-dlt')
        self.IS_PAPER_TRADING = os.getenv('IS_PAPER_TRADING', 'True').lower() in ('true', '1', 't')

        # RISK MANAGER SETTINGS
        self.MAX_POSITION_SIZE_PER_SYMBOL = int(os.getenv('MAX_POSITION_SIZE_PER_SYMBOL', 100))
        self.MAX_OPEN_POSITIONS = int(os.getenv('MAX_OPEN_POSITIONS', 10))
        self.MAX_CAPITAL_PER_TRADE = float(os.getenv('MAX_CAPITAL_PER_TRADE', 1000.0))
        self.MAX_DAILY_LOSS = float(os.getenv('MAX_DAILY_LOSS', 5000.0))
        self.MAX_ORDERS_PER_MINUTE = int(os.getenv('MAX_ORDERS_PER_MINUTE', 5))
        self.SIGNAL_MAX_AGE_SECONDS = int(os.getenv('SIGNAL_MAX_AGE_SECONDS', 60))  # Maximum age of a signal in seconds before it is considered stale and ignored

       # TimescaleDB / PostgreSQL connection settings
        self.DB_HOST     = os.getenv('DB_HOST',     'timescale')
        self.DB_PORT     = os.getenv('DB_PORT',     '5432')
        self.DB_NAME     = os.getenv('DB_NAME',     'marketdata')
        self.DB_USER     = os.getenv('DB_USER',     'postgres')
        self.DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
        return self

    def setEnvVariable(self, key, value, overwrite=True, add_to_file=False):
        if not overwrite and os.getenv(key) is not None:
            logger.info(f"Environment variable '{key}' already exists and overwrite is set to False. Skipping update.")
            return self
        os.environ[key] = value
        setattr(self, key, value)
        if add_to_file:
            self.AddEnvVariableToFile(key, value)
        return self

    def AddEnvVariableToFile(self, key, value):
        set_key(self.envFile, key, value)
    
