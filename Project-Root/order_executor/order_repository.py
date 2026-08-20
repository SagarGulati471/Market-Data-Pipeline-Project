# A shared repository for database operations related to orders.
# This module is used by the order_executor service to interact with the TimescaleDB database.
# It provides functions to fetch filled orders for the current day, which is useful for risk management

from datetime import datetime
from zoneinfo import ZoneInfo

async def fetch_todays_filled_orders(db_pool):
    async with db_pool.acquire() as connection:
        todays_market_open_time = datetime.now(ZoneInfo("America/New_York")).replace(hour=9, minute=30, second=0, microsecond=0)

        result = await connection.fetch(
            """
            SELECT * from orders where status='FILLED' and timestamp >= $1
            ORDER BY timestamp ASC
            """,
            todays_market_open_time
        )
        return result