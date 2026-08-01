import logging
from ..models import StrategySignal
from ...indicator.models import Indicator

logger = logging.getLogger(__name__)

def check(current: Indicator , previous: Indicator | None) -> StrategySignal:
    """
    Computes the EMA crossover signal for a given dataset.
    """

    curr, prev = current, previous

    # Corner Case: If we don't have enough data to compute the EMA crossover, return HOLD
    if prev is None or curr is None:
        logger.warning("Not enough data to compute EMA crossover signal. Returning HOLD.")
        return StrategySignal.HOLD    

    # Guard against missing EMA values in the current or previous indicator
    if curr.ema_9 is None or curr.ema_21 is None or curr.ema_50 is None or prev.ema_9 is None or prev.ema_21 is None:
        logger.warning("Missing EMA values in the current or previous indicator. Returning HOLD.")
        logger.debug(f"Current Indicator: {curr}, Previous Indicator: {prev}")
        return StrategySignal.HOLD

    if prev.ema_9 < prev.ema_21 and curr.ema_9 > curr.ema_21 and curr.ema_21 > curr.ema_50:
        return StrategySignal.BUY
    
    elif prev.ema_9 > prev.ema_21 and curr.ema_9 < curr.ema_21 and curr.ema_21 < curr.ema_50:
        return StrategySignal.SELL

    else:
        return StrategySignal.HOLD