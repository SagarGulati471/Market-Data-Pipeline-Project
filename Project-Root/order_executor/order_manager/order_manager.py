# The OrderManager class is responsible for managing and executing orders in the trading system.
# It interacts with the Risk Manager to assess the risk of each order before execution.
# If an order passes the risk checks, it is placed through the PaperAdapter,
# and relevant information is pushed to Kafka and stored in TimescaleDB
# for further analysis and record-keeping.


from datetime import datetime
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
        if Config.IS_PAPER_TRADING:
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