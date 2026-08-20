import logging
from ..models import StrategySignal
from ...indicator.models import Indicator

logger = logging.getLogger(__name__)

def check(current: Indicator , previous: Indicator | None) -> StrategySignal:
    """
    Computes the MACD crossover signal for a given dataset.
    """

    curr, prev = current, previous

    # Corner Case: If we don't have enough data to compute the MACD crossover, return HOLD
    if prev is None or curr is None:
        logger.debug("Not enough data to compute MACD crossover strategy. Returning HOLD.")
        return StrategySignal.HOLD

    # Guard against missing MACD values in the current or previous indicator
    if curr.macd_line is None or prev.macd_line is None or curr.macd_signal is None or prev.macd_signal is None:
        logger.debug("Missing MACD information in the current or previous indicator. Returning HOLD.")
        logger.debug(f"Current Indicator: {curr}, Previous Indicator: {prev}")
        return StrategySignal.HOLD

    # this is a condition of "STRONG BUY", where cross over happens above 0, confirming bullish momentum. This is a more reliable signal than just a crossover.
    if prev.macd_line < prev.macd_signal and curr.macd_line > curr.macd_signal and curr.macd_line > 0 and curr.macd_signal > 0:
        logger.debug(f"MACD crossover detected: Previous MACD Line: {prev.macd_line}, Previous MACD Signal: {prev.macd_signal}, Current MACD Line: {curr.macd_line}, Current MACD Signal: {curr.macd_signal}. Returning STRONG BUY.")
        return StrategySignal.BUY  # STRONG BUY

    # Note:
    # A crossover happening while both values are below zero means EMA(12) is still below EMA(26)
    # the broader trend is still bearish. "Confirming bullish momentum" is factually wrong for this case.
    # It's an early momentum shift in bearish territory, not a confirmation. Something like
    # "early bullish crossover in bearish territory"

    # But still it is a "buy" signal, even though it is a weaker buy signal. Later once we will be collecting all the signals and calculating a weighted score,
    # this will be reflected in the final signal strength.
    elif prev.macd_line < prev.macd_signal and curr.macd_line > curr.macd_signal and curr.macd_line < 0 and curr.macd_signal < 0:
        logger.debug(f"MACD crossover detected: Previous MACD Line: {prev.macd_line}, Previous MACD Signal: {prev.macd_signal}, Current MACD Line: {curr.macd_line}, Current MACD Signal: {curr.macd_signal}. Returning BUY.")
        return StrategySignal.BUY
    
    # this is a condition of "STRONG SELL", where cross over happens below 0, confirming bearish momentum. This is a more reliable signal than just a crossover.
    elif prev.macd_line > prev.macd_signal and curr.macd_line < curr.macd_signal and curr.macd_line < 0 and curr.macd_signal < 0:  
        logger.debug(f"MACD crossover detected: Previous MACD Line: {prev.macd_line}, Previous MACD Signal: {prev.macd_signal}, Current MACD Line: {curr.macd_line}, Current MACD Signal: {curr.macd_signal}. Returning STRONG SELL.")
        return StrategySignal.SELL

    # this is a condition of "SELL", where cross over happens above 0, confirming bearish momentum. This is a more reliable signal than just a crossover.
    elif prev.macd_line > prev.macd_signal and curr.macd_line < curr.macd_signal and curr.macd_line > 0 and curr.macd_signal > 0:  
        logger.debug(f"MACD crossover detected: Previous MACD Line: {prev.macd_line}, Previous MACD Signal: {prev.macd_signal}, Current MACD Line: {curr.macd_line}, Current MACD Signal: {curr.macd_signal}. Returning SELL.")
        return StrategySignal.SELL
    
    else:
        return StrategySignal.HOLD