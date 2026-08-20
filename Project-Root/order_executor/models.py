from __future__ import annotations
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Optional
from pydantic import BaseModel, Field
from enum import Enum


class OrderSide(str, Enum):
    BUY  =  "BUY"
    SELL =  "SELL"


class OrderType(str, Enum):
    MARKET =  "MARKET"
    LIMIT  =  "LIMIT"
    STOP   =  "STOP"


class OrderStatus(str, Enum):
    PENDING   =  "PENDING"      # order has been created but not yet sent to the broker.
    SUBMITTED =  "SUBMITTED"    # order has been sent to the broker but not yet executed.
    FILLED    =  "FILLED"       # fully executed by the broker.
    REJECTED  =  "REJECTED"     # broker refused it (insufficient funds, invalid symbol).
    CANCELLED =  "CANCELLED"    # broker cancelled it (user requested cancellation).
    PARTIAL   =  "PARTIAL"      # partially executed by the broker (some quantity filled, some still pending).
    

class Order(BaseModel):
    order_id:          str               = Field(..., description="Unique identifier for the order")
    source_signal_id:  Optional[str]               = Field(..., description="signal_id of the Signal that triggered this order")
    symbol:            str               = Field(..., description="Trading symbol for the order")
    side:              OrderSide         = Field(..., description="Side of the order (buy/sell)")
    ordertype:         OrderType         = Field(..., description="Type of the order (market/limit/stop)")
    quantity:          int               = Field(..., description="Quantity of the asset to be traded")
    price:             Decimal           = Field(..., description="Price at which the order is placed")
    filled_price:      Optional[Decimal] = Field(None, description="Price at which the order was filled (if applicable)")
    status:            OrderStatus       = Field(..., description="Current status of the order (pending/filled/cancelled)")
    filled_at:         Optional[datetime] = Field(None, description="Timestamp when the order was filled (if applicable)")
    timestamp:         datetime          = Field(..., description="Timestamp when the order was created")


class RiskDecisionReason(str, Enum):
    DAILY_LOSS_LIMIT_HIT    =  "DAILY_LOSS_LIMIT_HIT"
    DUPLICATE_SIGNAL        =  "DUPLICATE_SIGNAL"
    CONFLICTING_POSITION    =  "CONFLICTING_POSITION"
    MARKET_HOURS            =  "MARKET_HOURS"
    CAPITAL_LIMIT_EXCEEDED  =  "CAPITAL_LIMIT_EXCEEDED"
    RATE_LIMIT_EXCEEDED     =  "RATE_LIMIT_EXCEEDED"
    STALE_SIGNAL            =  "STALE_SIGNAL"
    MAX_POSITION_REACHED    =  "MAX_POSITION_REACHED"
    NO_POSITION_TO_SELL     =  "NO_POSITION_TO_SELL"
    SIGNAL_IS_HOLD          = "SIGNAL_IS_HOLD"
    MAX_POSITION_REACHED_PER_SYMBOL =  "MAX_POSITION_REACHED_PER_SYMBOL"
    MAX_CAPITAL_PER_TRADE_EXCEEDED = "MAX_CAPITAL_PER_TRADE_EXCEEDED"
    OTHER                   =  "OTHER"


class RiskDecision(BaseModel):
    approved: bool
    reason:   Optional[RiskDecisionReason] = None       # "DAILY_LOSS_LIMIT_HIT", "STALE_SIGNAL", "MAX_POSITION_REACHED"
    details:  dict


class RiskConfig(BaseModel):
    max_position_size_per_symbol:   int      # max shares of any single symbol
    max_open_positions:             int      # max number of different symbols held simultaneously
    max_capital_per_trade:          Decimal  # max $ value of a single order
    max_daily_loss:                 Decimal  # kill switch or a circuit breaker: stop trading if loss exceeds this
    max_orders_per_minute:          int      # rate limit
    signal_max_age_seconds:         int      # Maximum age of a signal in seconds before it is considered stale and ignored (reject signals older than this)
    cooldown_seconds:               int      # Cooldown period in seconds to prevent duplicate signals (reject signals for the same symbol within this time frame)


# Acts as a In-memory cache to keep:
# current position sizes, daily PnL, and pending orders for the trading session.
# This is used by the RiskManager to make risk decisions.
class CurrentPositionSize:  
    def __init__(self):   
        self._holdings            : dict[str,int]      = {}  # symbol -> quantity
        self._daily_pnl           : Decimal            = Decimal(0)  # daily profit and loss
        self._pending_orders      : dict[str,Order]    = {}  # # order_id → Order
        self._order_times         : list[datetime]     = []  # timestamps of recent orders for rate limiting
        self._avg_cost_per_symbol : dict[str, Decimal] = {}  # symbol -> average cost per share for PnL calculations
        self._recent_signals      : dict[str, datetime] = {}  # symbol -> timestamp of the last signal received for that symbol, used to prevent duplicate signals


    def get_quantity(self, symbol: str) -> int:
        return self._holdings.get(symbol, 0)


    def get_daily_pnl(self) -> Decimal:
        return self._daily_pnl


    def get_position_count(self) -> int:
        # Position count is the number of unique symbols currently held in the portfolio
        return sum(1 for qty in self._holdings.values() if qty > 0)  # Count only symbols with positive holdings

    def get_all_open_positions(self) -> dict[str, int]:
        return dict(self._holdings)  # snapshot — caller iterating while fills modify _holdings
         
    def recent_order_count(self, window_seconds: int) -> int:
        # Count how many orders were placed within the last 'window_seconds' seconds
        now = datetime.now()
        cutoff_time = now - timedelta(seconds=window_seconds)
        return sum(1 for order_time in self._order_times if order_time >= cutoff_time)


    # Picks the order from the pending orders and updates its status to FILLED,
    # also updates the position and daily PnL accordingly.
    # If the order is not found in pending orders, it raises a ValueError.
    def record_fill(self, order_id: str, filled_price: Decimal):
        # Update the order status and filled price for a given order
        if order_id in self._pending_orders:
            order = self._pending_orders[order_id]
            order.status = OrderStatus.FILLED
            order.filled_price = filled_price
            order.filled_at = datetime.now()
            self.update_position(order.symbol, order.quantity if order.side == OrderSide.BUY else -order.quantity, filled_price)
            del self._pending_orders[order_id]  # Remove from pending orders after fill
        else:
            raise ValueError(f"Order ID {order_id} not found in pending orders.")


    # Updates the position of the stock symbol based on the filled order. If the order is a buy, it increases the quantity; if it's a sell, it decreases the quantity.
    # It also updates the daily PnL based on the filled price and quantity change.
    def update_position(self, symbol: str, quantity_change: int, filled_price: Decimal):
        # Update the holdings for the given symbol
        current_quantity = self._holdings.get(symbol, 0)
        new_quantity = current_quantity + quantity_change
        if new_quantity < 0:
            raise ValueError(f"Cannot have negative holdings for symbol {symbol}. Current: {current_quantity}, Change: {quantity_change}")

        # If the quantity change is positive (buy), update the average cost per share;
        # if negative (sell), calculate realized PnL and update daily PnL.
        
        if quantity_change > 0: # i.e., a buy order
            current_average = self._avg_cost_per_symbol.get(symbol, Decimal("0"))
            total_cost = (current_average * current_quantity) + (filled_price * quantity_change)
            new_average_cost = total_cost / new_quantity
            self._avg_cost_per_symbol[symbol] = new_average_cost

        else:   # quantity_change < 0, i.e., a sell order
            current_average = self._avg_cost_per_symbol.get(symbol, Decimal("0"))
            realized_pnl = (filled_price - current_average) * abs(quantity_change)
            self._daily_pnl += realized_pnl
            if new_quantity == 0:
                del self._avg_cost_per_symbol[symbol]

        self._holdings[symbol] = new_quantity

    
    def add_order(self, order: Order):
        self._pending_orders[order.order_id] = order
        now = datetime.now()
        self._order_times.append(now)
        cutoff_time = now - timedelta(seconds=60)
        self._order_times = [t for t in self._order_times if t >= cutoff_time]