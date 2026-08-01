import logging
from ..models import StrategySignal
from ...indicator.models import Indicator

logger = logging.getLogger(__name__)

def check(current: Indicator , previous: Indicator | None) -> StrategySignal:
    """
    Computes the VWAP confluence signal for a given dataset.
    """

    # Here we are only using the current indicator for VWAP confluence strategy,
    # but we are keeping the previous indicator parameter for consistency with other strategies.
    # Since we are not using previous indicator in this strategy, we are not perform any checks on it.

    curr, prev = current, previous

    # Corner Case: If we don't have enough data to compute the VWAP confluence, return HOLD
    if curr is None:
        logger.debug("Not enough data to compute VWAP confluence signal. Returning HOLD.")
        return StrategySignal.HOLD

    # Guard against missing VWAP values in the current or previous indicator
    if curr.vwap_session is None:
        logger.debug("Missing VWAP values in the current indicator. Returning HOLD.")
        logger.debug(f"Current Indicator: {curr}")
        return StrategySignal.HOLD

    # Guard against missing RSI values in the current indicator
    if curr.rsi_14 is None:
        logger.debug("Missing RSI values in the current indicator. Returning HOLD.")
        logger.debug(f"Current Indicator: {curr}")
        return StrategySignal.HOLD

    # Guard against missing close price values in the current indicator
    if curr.close_price is None:
        logger.debug("Missing close price values in the current indicator. Returning HOLD.")
        logger.debug(f"Current Indicator: {curr}")
        return StrategySignal.HOLD
    
    # Check for BUY condition: Price > VWAP AND RSI between 50–65
    if curr.close_price > curr.vwap_session and 50 <= curr.rsi_14 <= 65:
        logger.debug(f"VWAP confluence BUY condition met: Current Price: {curr.close_price}, Current VWAP: {curr.vwap_session}, Current RSI: {curr.rsi_14}. Returning BUY.")
        return StrategySignal.BUY
    
    # Check for SELL condition: Price < VWAP AND RSI between 35–50
    elif curr.close_price < curr.vwap_session and 35 <= curr.rsi_14 <= 50:
        logger.debug(f"VWAP confluence SELL condition met: Current Price: {curr.close_price}, Current VWAP: {curr.vwap_session}, Current RSI: {curr.rsi_14}. Returning SELL.")
        return StrategySignal.SELL
    
    else:
        return StrategySignal.HOLD




# ******************* About VVWAP Confluence Strategy: *******************
"""
What VWAP Confluence Actually Means
VWAP = Volume Weighted Average Price, reset at market open each day. 
VWAP tells : "what is the average price at which ALL trades happened today, weighted by how much was traded at each price?"

Institutional traders (funds, banks) use VWAP as a benchmark - they aim to execute near it. 
So:
Price above VWAP = buyers have been more aggressive than sellers all day. Institutions are net buyers. Bullish bias.
Price below VWAP = sellers dominating. Institutions net sellers. Bearish bias.


The "confluence" part means you combine VWAP with RSI so both must agree:

Price > VWAP  AND  RSI between 50–65  →  bullish bias + healthy momentum = BUY
Price < VWAP  AND  RSI between 35–50  →  bearish bias + weakening momentum = SELL

"""