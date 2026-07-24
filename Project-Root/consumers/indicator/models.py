from pydantic import BaseModel, Field, computed_field


class Indicator(BaseModel):

    # Identifier fields
    symbol: str
    resolution: str
    timestamp: int

    # Indicator fields
    vwap_session: float = Field(alias='vwap_session')

    # Simple Moving Averages
    sma_9: float = Field(alias='sma_9')
    sma_21: float = Field(alias='sma_21')
    sma_50: float = Field(alias='sma_50')
    sma_200: float = Field(alias='sma_200')

    # Exponential Moving Averages
    ema_9: float = Field(alias='ema_9')
    ema_21: float = Field(alias='ema_21')
    ema_50: float = Field(alias='ema_50')
    ema_200: float = Field(alias='ema_200')

    # Relative Strength Index
    rsi_14: float = Field(alias='rsi_14')

    # Moving Average Convergence Divergence
    macd_line: float = Field(alias='macd_line')
    macd_signal: float = Field(alias='macd_signal')
    macd_histogram: float = Field(alias='macd_histogram')

    # Bollinger Bands
    bb_upper: float = Field(alias='bb_upper')
    bb_middle: float = Field(alias='bb_middle')
    bb_lower: float = Field(alias='bb_lower')
    bb_bandwidth: float = Field(alias='bb_bandwidth')


    