from ....utils.logger import setup_logger
import logging
from ..base import BrokerAdapter
from ...models import Order

setup_logger()
logger = logging.getLogger(__name__)


class PaperAdapter(BrokerAdapter):

    async def place_order(self, order):
        # Simulate placing an order in a paper trading environment
        print(f"Placing paper order: {order}, close_price: {order.price}, quantity: {order.quantity}")
        return {"status": "success", "order_id": "paper_order_123"} # Placeholder order ID for paper trading

    async def cancel_order(self, order_id):
        # Simulate canceling an order in a paper trading environment
        print(f"Cancelling paper order: {order_id}")
        return {"status": "success", "order_id": order_id}

    async def get_order_status(self, order_id):
        # Simulate getting the status of an order in a paper trading environment
        print(f"Getting status for paper order: {order_id}")
        return {"status": "filled", "order_id": order_id}

    async def get_positions(self):
        # Simulate getting positions in a paper trading environment
        print(f"Getting positions for paper account")
        return {"positions": [{"symbol": "AAPL", "quantity": 10, "price": 150.0}]}