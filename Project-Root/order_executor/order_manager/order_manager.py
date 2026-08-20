# The OrderManager class is responsible for managing and executing orders in the trading system.
# It interacts with the Risk Manager to assess the risk of each order before execution.
# If an order passes the risk checks, it is placed through the PaperAdapter,
# and relevant information is pushed to Kafka and stored in TimescaleDB
# for further analysis and record-keeping.


import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo
import logging
from config.config import Config
from risk_manager.risk_manager import RiskManager
from broker_adapter.paper_adapter.paper_adapter import PaperAdapter
from ..models import Order, OrderSide, OrderType, OrderStatus
from ...messaging.kafka_service.service import send_message
from ...consumers.signal_generator.models import SignalType

logger = logging.getLogger(__name__)
config = Config()

class OrderManager:

    _BUY_SIGNALS  = frozenset({SignalType.BUY,  SignalType.STRONG_BUY})
    _SELL_SIGNALS = frozenset({SignalType.SELL, SignalType.STRONG_SELL})

    def __init__(self, risk_config, position_state, db_pool, kafka_producer):
        self.risk_config = risk_config
        self.db_pool = db_pool
        self.kafka_producer = kafka_producer

        self.risk_manager = RiskManager(risk_config)
        self.positions = position_state
        if config.IS_PAPER_TRADING:
            self.order_executor = PaperAdapter(self.db_pool)
            logger.info("Paper trading mode enabled. Using PaperAdapter for order execution.")
        else:
            # Placeholder for a real broker adapter, e.g., AlpacaAdapter or InteractiveBrokersAdapter
            # self.order_executor = SomeRealBrokerAdapter()  # To be replaced with actual broker adapter
            pass


    async def handle_order(self, signal):

        # This function will perform the following steps:
        # 1.) Calculate Risk by passing the order to the RiskManager
        risk_passed = self.risk_manager.calculate_risk(signal, self.positions)
        logger.debug(f"Risk check for order {signal}: {'Passed' if risk_passed.approved else 'Failed'}, risk_passed: {risk_passed}")

        if not risk_passed.approved:
            logger.warning(f"Risk check failed for order {signal}: {risk_passed}")
            return False

        # 2.) Risk has passed, send the order to broker adapter for execution
        close_price = Decimal(str(signal.close_price))
        if signal.signal_type in self._SELL_SIGNALS:
            quantity = self.positions.get_quantity(signal.symbol)
        else:
            quantity = int(self.risk_config.max_capital_per_trade // close_price)
        order = Order(
            order_id         = str(uuid4()),
            source_signal_id = signal.signal_id,
            symbol           = signal.symbol,
            side             = OrderSide.BUY if signal.signal_type in self._BUY_SIGNALS else OrderSide.SELL,
            ordertype        = OrderType.MARKET,
            quantity         = quantity,
            price            = close_price,
            status           = OrderStatus.PENDING,
            timestamp        = datetime.now(ZoneInfo("America/New_York")),
        )
        self.positions.add_order(order)  # track as pending before calling broker
        execution_result = await self.order_executor.place_order(order)
        self.positions.record_fill(order.order_id, execution_result.filled_price)
        logger.info(f"Order execution result for {signal}: {execution_result}")


        # Push the order execution result to Kafka for further processing or logging
        if execution_result.status == OrderStatus.FILLED:
            await send_message(
                producer=self.kafka_producer,
                topic=config.KAFKA_TOPIC_ORDER_EXECUTOR,
                key=execution_result.order_id,
                value=execution_result.model_dump(mode='json')
            )
            logger.info(f"Order execution result for {signal} sent to Kafka topic 'order_execution_results'.")
        else:
            logger.warning(f"Order execution for {signal} did not result in a filled order. Status: {execution_result.status}")

        logger.info(f"Order handling completed for signal {signal.signal_id}. Execution result: {execution_result}")
        return True

    async def execute_square_off_order(self, order):
        '''
        Dedicated function to place square-off orders at the end of the trading day.
        This function is similar to handle_order but is specifically designed for closing positions.
        '''
        self.positions.add_order(order)
        execution_result = await self.order_executor.place_order(order)
        self.positions.record_fill(order.order_id, execution_result.filled_price)
        logger.info(f"Order execution result for Square Off order:\n {order}: {execution_result}")


        # Push the order execution result to Kafka for further processing or logging
        if execution_result.status == OrderStatus.FILLED:
            await send_message(
                producer=self.kafka_producer,
                topic=config.KAFKA_TOPIC_ORDER_EXECUTOR,
                key=execution_result.order_id,
                value=execution_result.model_dump(mode='json')
            )
            logger.info(f"Order execution result for Square Off order {order} sent to Kafka topic 'order_execution_results'.")
        else:
            logger.warning(f"Order execution for Square Off order {order} did not result in a filled order. Status: {execution_result.status}")

        logger.info(f"Order handling completed for order Square Off {order.order_id}. Execution result: {execution_result}")
        return True

    


async def intraday_auto_square_off(current_position_state, order_manager):
    """
    This function is responsible for automatically squaring off all open positions at the end of the trading day.
    It retrieves all open positions, creates market orders to close them, and sends these orders to the broker adapter for execution.
    After execution, it updates the position state and logs the results.
    """

    while True:
        ET = ZoneInfo("America/New_York")
        curr_time = datetime.now(ET)
        square_off_time = curr_time.replace(hour=15, minute=30, second=0, microsecond=0)


        market_open  = curr_time.replace(hour=9,  minute=30, second=0, microsecond=0)
        market_close = curr_time.replace(hour=16, minute=0,  second=0, microsecond=0)
        if curr_time.weekday() >= 5 or not (market_open <= curr_time < market_close): 
            # Sleep until next market open
            next_run = (curr_time + timedelta(days=1)).replace(hour=9, minute=30, second=0, microsecond=0)
            await asyncio.sleep((next_run - curr_time).total_seconds())
            continue
        
        if curr_time < square_off_time:
            # Sleep until exactly 3:30 PM
            await asyncio.sleep((square_off_time - curr_time).total_seconds())
            continue

        if curr_time >= curr_time.replace(hour=16, minute=0, second=0, microsecond=0):
            # Already past market close — sleep until next day's 3:30 PM
            next_run = (curr_time + timedelta(days=1)).replace(hour=15, minute=30, second=0, microsecond=0)
            await asyncio.sleep((next_run - curr_time).total_seconds())
            continue

        
        open_positions = current_position_state.get_all_open_positions()
        for symbol, quantity in open_positions.items():
            if quantity == 0:
                continue
            order_side = OrderSide.SELL if quantity > 0 else OrderSide.BUY
            order_quantity = abs(quantity)

            order = Order(
                order_id = str(uuid4()),
                source_signal_id = "SQUARE_OFF",
                symbol = symbol,
                side = order_side,
                ordertype = OrderType.MARKET,
                quantity = order_quantity,
                price = Decimal(0),  # Market order, To create an API to fetch the current market price for the symbol. (Later)
                status = OrderStatus.PENDING,
                timestamp = datetime.now(ZoneInfo("America/New_York"))  
            )
            await order_manager.execute_square_off_order(order)

        await asyncio.sleep(24 * 60 * 60)  # all positions closed — sleep until next trading day


