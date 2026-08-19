import logging
import random
from decimal import Decimal
from datetime import datetime

from .cache import PaperAdapterCache
from .db_wrapper import DatabaseWrapper
from ..base import BrokerAdapter
from ...models import Order, OrderSide, OrderStatus
from ....utils.logger import setup_logger


setup_logger()
logger = logging.getLogger(__name__)


class PaperAdapter(BrokerAdapter):
    def __init__(self, db_pool):
        super().__init__()
        self.cache = PaperAdapterCache()  # Initialize the in-memory cache for paper trading
        self.db_wrapper = DatabaseWrapper(db_pool)  # Initialize the database wrapper with the provided db_pool

    async def place_order(self, order) -> Order:
        # Simulate placing an order in a paper trading environment
        
        # Log the order details for debugging purposes
        logger.debug(f"Placing paper order: {order}")


        # Check if the order is already in the cache
        order_exists = self.cache.get_order(order.order_id)
        if order_exists:
            logger.warning(f"Order with ID {order.order_id} already exists in the cache. Skipping order placement.")
            return order_exists  # Return the existing order from the cache

        # Order does not exists in the cache, check if the order is already in the database
        order_exists = await self.db_wrapper.fetch_order(order.order_id)
        if order_exists:
            logger.warning(f"Order with ID {order.order_id} already exists in the Database. Skipping order placement.")
            return order_exists  # Return the existing order from the cache

        try:
            # Before the DB insert, simulate fill:
            filled_order = self._simulate_fill(order)
            await self.db_wrapper.insert_order(filled_order)
            self.cache.add_order(filled_order)
            return filled_order
        except Exception as e:
            logger.error(f"Error while placing the order: {e} \n Order Details:{order.model_dump()} ")
            raise

    async def cancel_order(self, order_id):
        # Simulate canceling an order in a paper trading environment
        print(f"Cancelling paper order: {order_id}")
        return {"status": "success", "order_id": order_id}


    def _simulate_fill(self, order: Order) -> Order:
        # Simulate filling the order by setting its status to 'filled'
        slippage = Decimal(str(round(random.uniform(0.0001, 0.0005), 6)))  # 0.01%–0.05% slippage, realistic for liquid US equities
        if order.side == OrderSide.BUY:
            fill_price = order.price * (1 + slippage)
        else:
            fill_price = order.price * (1 - slippage)

        return order.model_copy(
            update={
                "status": OrderStatus.FILLED,
                "filled_price": fill_price,
                "filled_at": datetime.now()
            }
        )


    async def get_order_status(self, order_id):
        # Simulate getting the status of an order in a paper trading environment
        print(f"Getting status for paper order: {order_id}")
        return {"status": "filled", "order_id": order_id}

    async def get_positions(self):
        # Simulate getting positions in a paper trading environment
        print(f"Getting positions for paper account")
        return {"positions": [{"symbol": "AAPL", "quantity": 10, "price": 150.0}]}