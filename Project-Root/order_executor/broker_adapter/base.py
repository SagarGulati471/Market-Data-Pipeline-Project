from abc import ABC, abstractmethod

# This is an abstract base class for a broker adapter. It defines the interface that any concrete broker adapter must implement.
# The methods include placing an order, canceling an order, and getting the status of an order.
class BrokerAdapter(ABC):
    @abstractmethod
    async def place_order(self, order):
        pass

    @abstractmethod
    async def cancel_order(self, order_id):
        pass

    @abstractmethod
    async def get_order_status(self, order_id):
        pass
    