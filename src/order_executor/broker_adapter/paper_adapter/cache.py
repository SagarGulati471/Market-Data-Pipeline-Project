# This is an in-memory cache for the paper adapter. It keeps track of orders and positions.
# Going forward, we can replace this with redis or a more robust caching solution if needed.
from order_executor.models import Order


class PaperAdapterCache:
    def __init__(self):
        self._orders = dict[str, Order]()  # order_id → Order

    def get_order(self, order_id: str) -> Order | None:
        return self._orders.get(order_id, None)

    def add_order(self, order: Order):
        self._orders[order.order_id] = order

    def delete_order(self, order: Order):
        if order.order_id in self._orders:
            del self._orders[order.order_id]