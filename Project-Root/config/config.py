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
        self.KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BROKER_URL', 'localhost:9093')
        self.FINNHUB_KAFKA_TOPIC = os.getenv('FINNHUB_KAFKA_TOPIC', 'market_data')
        self.DEAD_LETTER_TOPIC_NORMALIZER = os.getenv('DEAD_LETTER_TOPIC_NORMALIZER', 'normalizer-dlt')
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
    
