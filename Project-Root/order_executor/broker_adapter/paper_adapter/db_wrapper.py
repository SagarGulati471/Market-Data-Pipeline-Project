# DB Wrapper for asyncpg connection pool
# db_pool is initialized in the consumer file once and is passed to this wrapper,
# and it manages the database operations for orders.
from ...models import Order, OrderSide

class DatabaseWrapper:
    def __init__(self, db_pool):
        self.db_pool = db_pool

    async def insert_order(self, order):
        async with self.db_pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO orders (order_id, source_signal_id,  symbol, side, ordertype, quantity, price, filled_price,  status, filled_at, timestamp)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """,
                    order.order_id,
                    order.source_signal_id,
                    order.symbol,
                    order.side.value,
                    order.ordertype.value,
                    order.quantity,
                    order.price,
                    order.filled_price,
                    order.status.value,
                    order.filled_at,
                    order.timestamp
                )


    async def fetch_order(self, order_id):
        async with self.db_pool.acquire() as connection:
            result = await connection.fetchrow(
                """
                SELECT * FROM orders WHERE order_id = $1
                """,
                order_id
            )
            if result is None:
                return None
            order = Order(
                order_id=result['order_id'],
                source_signal_id=result['source_signal_id'],
                symbol=result['symbol'],
                side=OrderSide(result['side']),
                ordertype=result['ordertype'],
                quantity=result['quantity'],
                price=result['price'],
                filled_price=result['filled_price'],
                status=result['status'],
                filled_at=result['filled_at'],
                timestamp=result['timestamp']
            )
            return order


    async def update_order_status(self, order_id, new_status, filled_price=None, filled_at=None):
        async with self.db_pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    UPDATE orders
                    SET status = $1, filled_price = $2, filled_at = $3
                    WHERE order_id = $4
                    """,
                    new_status.value,
                    filled_price,
                    filled_at,
                    order_id
                )


    async def run_query(self, query, *args):
        async with self.db_pool.acquire() as connection:
            async with connection.transaction():
                result = await connection.fetch(query, *args)
                return result