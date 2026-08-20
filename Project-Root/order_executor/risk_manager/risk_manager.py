import datetime
import logging
from zoneinfo import ZoneInfo
from decimal import Decimal
from ..models import RiskDecision, RiskConfig, RiskDecisionReason, OrderSide
from ...consumers.signal_generator.models import SignalType

logger = logging.getLogger(__name__)

class RiskManager():
    _BUY_SIGNALS  = frozenset({SignalType.BUY,  SignalType.STRONG_BUY})
    _SELL_SIGNALS = frozenset({SignalType.SELL, SignalType.STRONG_SELL})
    def __init__(self, risk_config: RiskConfig):
        # Initialize the RiskManager with the provided risk configuration
        self.risk_config = risk_config


    def calculate_risk(self, signal, positions) -> RiskDecision:
        result = RiskDecision(approved=True, reason=None, details={})
        curr_time = datetime.datetime.now(ZoneInfo("America/New_York"))


        # 0.) HOLD signals never result in an order — reject immediately
        if signal.signal_type == SignalType.HOLD:
            result.approved = False
            result.reason = RiskDecisionReason.SIGNAL_IS_HOLD
            logger.debug("HOLD signal skipped: symbol=%s", signal.symbol)
            return result


        # 1.) Signal staleness check
        signal_age = (curr_time - signal.open_time.astimezone(ZoneInfo("America/New_York"))).total_seconds()
        if signal_age > self.risk_config.signal_max_age_seconds:
            result.approved = False
            result.reason = RiskDecisionReason.STALE_SIGNAL
            result.details['signal_age'] = signal_age
            logger.warning("Stale signal rejected: symbol=%s age=%.1fs max=%ss", signal.symbol, signal_age, self.risk_config.signal_max_age_seconds)
            return result
        

        # 2.) Market hours check
        # Replace will set the time to 9:30 AM and 4:00 PM respectively, keeping the date the same as curr_time
        # i.e it will create two datetime objects for the current date, one at 9:30 AM and another at 4:00 PM
        market_open  = curr_time.replace(hour=9,  minute=30, second=0, microsecond=0)
        market_close = curr_time.replace(hour=16, minute=0,  second=0, microsecond=0)
        if curr_time.weekday() >= 5 or not (market_open <= curr_time < market_close):  # Assuming market hours are 9 AM to 4 PM
            result.approved = False
            result.reason = RiskDecisionReason.MARKET_HOURS
            result.details['current_time'] = curr_time.isoformat()
            logger.debug("Signal rejected outside market hours: symbol=%s current_time=%s", signal.symbol, curr_time.isoformat())
            return result

        # 2.1.) Intraday cutoff check (e.g., no new positions after 3:30 PM)
        square_off_time = curr_time.replace(hour=15, minute=30, second=0, microsecond=0)
        if curr_time >= square_off_time and signal.signal_type in self._BUY_SIGNALS:
            result.approved = False
            result.reason = RiskDecisionReason.MARKET_HOURS
            result.details['current_time'] = curr_time.isoformat()
            result.details['message'] = "Intraday cutoff: no new positions after 3:30 PM"
            logger.debug("Intraday BUY cutoff reached: symbol=%s current_time=%s", signal.symbol, curr_time.isoformat())
            return result


        # 3.) Daily loss limit check
        # If the current daily PnL is less than or equal to the negative of the maximum daily loss limit, reject the order
        if positions.get_daily_pnl() <= -self.risk_config.max_daily_loss:
            result.approved = False
            result.reason = RiskDecisionReason.DAILY_LOSS_LIMIT_HIT
            result.details['daily_pnl'] = str(positions.get_daily_pnl()) # convert Decimal to string for JSON serialization, while pushing to Kafka
            result.details['max_daily_loss'] = str(self.risk_config.max_daily_loss)
            logger.warning("Daily loss limit hit, trading halted: daily_pnl=%s limit=%s", positions.get_daily_pnl(), self.risk_config.max_daily_loss)
            return result


        # 4.) Duplicate signal guard check
        # Creating signal fingerprint based on symbol, side, and quantity to identify duplicates
        signal_fingerprint = f"{signal.symbol}_{signal.signal_type}_{signal.resolution}"
        if signal_fingerprint in positions._recent_signals:
            recent_signal_time = positions._recent_signals[signal_fingerprint]
            time_since_last_signal = (curr_time - recent_signal_time).total_seconds()
            if time_since_last_signal < self.risk_config.cooldown_seconds:  # Assuming a duplicate signal is defined as
                result.approved = False
                result.reason = RiskDecisionReason.DUPLICATE_SIGNAL
                result.details['last_signal_time'] = positions._recent_signals[signal_fingerprint].isoformat()
                logger.debug("Duplicate signal within cooldown: symbol=%s fingerprint=%s cooldown=%ss", signal.symbol, signal_fingerprint, self.risk_config.cooldown_seconds)
                return result


        # 5.) Conflicting position 
        pending_orders = positions._pending_orders
        for order in pending_orders.values():
            if order.symbol == signal.symbol and (((order.side == OrderSide.BUY) and signal.signal_type in self._SELL_SIGNALS) or (order.side == OrderSide.SELL and signal.signal_type in self._BUY_SIGNALS)):
                result.approved = False
                result.reason = RiskDecisionReason.CONFLICTING_POSITION
                result.details['signal_details'] = {
                    'order_side': order.side.value,
                    'signal_side': signal.signal_type.value,
                    'order_symbol': order.symbol,
                }
                logger.warning("Conflicting pending order: symbol=%s pending_side=%s signal_side=%s", order.symbol, order.side.value, signal.signal_type.value)
                return result

        # 6.) Check if the signal is a sell signal and the current position for that symbol is zero, which would mean there's nothing to sell
        # We are not supporting short selling as of now, so if the position is zero, we cannot sell.
        # This check is only relevant for sell signals.
        if signal.signal_type in self._SELL_SIGNALS:
            if positions.get_quantity(signal.symbol) == 0:
                result.approved = False
                result.reason = RiskDecisionReason.NO_POSITION_TO_SELL
                result.details['symbol'] = signal.symbol
                logger.warning("No position to sell: symbol=%s", signal.symbol)
                return result


        # 7.) Max position per symbol check (Only for buy orders, because buy orders can increase the number of open positions, while sell orders reduce it)
        total_current_holdings = positions._holdings.get(signal.symbol, 0)
        if signal.signal_type in self._BUY_SIGNALS:
            if total_current_holdings >= self.risk_config.max_position_size_per_symbol:
                result.approved = False
                result.reason = RiskDecisionReason.MAX_POSITION_REACHED_PER_SYMBOL
                result.details['reason'] = {
                    'total_current_holdings': total_current_holdings,
                    'max_positions_allowed': self.risk_config.max_position_size_per_symbol
                }
                logger.debug("Max position per symbol: symbol=%s held=%s limit=%s", signal.symbol, total_current_holdings, self.risk_config.max_position_size_per_symbol)
                return result


        # 8.) Max number of open positions (Only for buy orders, because buy orders can increase the number of open positions, while sell orders reduce it)
        current_holdings = positions._holdings
        total_current_holdings = sum(1 for qty in current_holdings.values() if qty > 0)
        if signal.signal_type in self._BUY_SIGNALS:
            if total_current_holdings >= self.risk_config.max_open_positions:
                result.approved = False
                result.reason = RiskDecisionReason.MAX_POSITION_REACHED
                result.details['reason'] = {
                    'total_current_open_positions': total_current_holdings,
                    'max_positions_allowed': self.risk_config.max_open_positions
                }
                logger.debug("Max open positions reached: open=%s limit=%s", total_current_holdings, self.risk_config.max_open_positions)
                return result


        # 9.) Capital per trade limit check
        if signal.close_price is None:
            result.approved = False
            result.reason = RiskDecisionReason.OTHER
            result.details['other'] = {
                'close_price': str(signal.close_price),
                'message': "Close price is None, cannot calculate capital required for the trade."
            }
            logger.warning("Close price is None, cannot size order: symbol=%s signal_id=%s", signal.symbol, signal.signal_id)
            return result

        if signal.signal_type in self._BUY_SIGNALS:
            close_price = Decimal(str(signal.close_price))
            quantity = int(self.risk_config.max_capital_per_trade // close_price)
            if quantity < 1:
                result.approved = False
                result.reason = RiskDecisionReason.MAX_CAPITAL_PER_TRADE_EXCEEDED
                result.details['reason'] = {
                    'close_price': str(close_price),
                    'quantity': quantity,
                    'capital_required': str(close_price * Decimal(quantity)),
                    'max_capital_per_trade': str(self.risk_config.max_capital_per_trade) 
                }
                logger.warning("Stock too expensive for capital limit: symbol=%s price=%s limit=%s", signal.symbol, close_price, self.risk_config.max_capital_per_trade)
                return result


        # 10.) Rate limit check (Max orders per minute)
        recent_order_count = positions.recent_order_count(window_seconds=60)
        if recent_order_count >= self.risk_config.max_orders_per_minute:
            result.approved = False
            result.reason = RiskDecisionReason.RATE_LIMIT_EXCEEDED
            result.details['reason'] = {
                'recent_order_count': recent_order_count,
                'max_orders_per_minute': self.risk_config.max_orders_per_minute
            }
            logger.warning("Rate limit exceeded: orders_last_minute=%s limit=%s", recent_order_count, self.risk_config.max_orders_per_minute)
            return result


        # Update the recent signals dictionary with the current signal's fingerprint and timestamp
        positions._recent_signals[signal_fingerprint] = datetime.datetime.now(ZoneInfo("America/New_York"))
        logger.info("Signal approved: symbol=%s signal_type=%s signal_id=%s", signal.symbol, signal.signal_type.value, signal.signal_id)
        # If all checks passed, return the result as approved
        return result
