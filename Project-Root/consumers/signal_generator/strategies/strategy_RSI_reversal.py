import logging
from ..models import StrategySignal
from ...indicator.models import Indicator

logger = logging.getLogger(__name__)

def check(current: Indicator , previous: Indicator | None) -> StrategySignal:
    """
    Computes the RSI reversal signal for a given dataset.
    """

    curr, prev = current, previous

    # Corner Case: If we don't have enough data to compute the RSI reversal, return HOLD
    if prev is None or curr is None:
        logger.debug("Not enough data to compute RSI reversal signal. Returning HOLD.")
        return StrategySignal.HOLD    

    # Guard against missing RSI values in the current or previous indicator
    if curr.rsi_14 is None or prev.rsi_14 is None:
        logger.debug("Missing RSI values in the current or previous indicator. Returning HOLD.")
        logger.debug(f"Current Indicator: {curr}, Previous Indicator: {prev}")
        return StrategySignal.HOLD

    if prev.rsi_14 < 30 and curr.rsi_14 > 30:
        return StrategySignal.BUY
    
    elif prev.rsi_14 > 70 and curr.rsi_14 < 70:
        return StrategySignal.SELL

    else:
        return StrategySignal.HOLD