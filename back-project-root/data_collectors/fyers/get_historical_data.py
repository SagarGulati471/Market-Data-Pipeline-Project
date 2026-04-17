from fyers_apiv3 import fyersModel
from config.config import Config
import logging
from utils.logger import setup_logger

setup_logger()
logger = logging.getLogger(__name__)


class GetHistoricalData:
    def __init__(self):
        config = Config()
        self.fyers = fyersModel.FyersModel(client_id=config.CLIENT_ID, is_async=False, token=config.ACCESS_TOKEN, log_path="")

    def fetch_historical_data(self, symbol, resolution, date_format, range_from, range_to, cont_flag):
        logger.info(f"Fetching historical data for symbol: {symbol}, resolution: {resolution}, date_format: {date_format}, range_from: {range_from}, range_to: {range_to}, cont_flag: {cont_flag}")
        data = {
            "symbol": symbol,
            "resolution": resolution,
            "date_format": date_format,
            "range_from": range_from,
            "range_to": range_to,
            "cont_flag": cont_flag
        }
        response = self.fyers.history(data=data)
        logger.debug(f"Received historical data for symbol: {symbol}, response: {response}")
        return response
    

if __name__ == "__main__":
    data_fetcher = GetHistoricalData()
    historical_data = data_fetcher.fetch_historical_data(
        symbol="NSE:RELIANCE-EQ",
        resolution="120",
        date_format="1",
        range_from="2026-03-04",
        range_to="2026-03-06",
        cont_flag="1"
    )
    print(historical_data)