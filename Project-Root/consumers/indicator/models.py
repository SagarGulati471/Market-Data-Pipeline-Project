from pydantic import BaseModel, Field
import datetime



# *************** Explanation of the Indicator model fields ***************
# If there are not enough candles, then we can't compute that indicator. 
# So, we will return None for that indicator.

# Example: If we have only 5 candles, then we can't compute SMA_9, SMA_21, SMA_50, SMA_200, EMA_9, EMA_21, EMA_50, EMA_200, RSI_14, MACD_Line, MACD_Signal, MACD_Histogram, BB_Upper, BB_Middle, BB_Lower, BB_Bandwidth.
# In this case all the indicators will be None, except for VWAP_Session which can be computed with any number of candles.

# Syntax - sma_9: float | None = Field(default=None, alias="sma_9") 
# Means - the field sma_9 can either be a float or None.
# If no value is provided, it defaults to None. The alias "sma_9" is used for serialization/deserialization.
# alias="sma_9" is basically if JSon has a key "sma_9", it will be mapped to the field sma_9 in the Indicator model (Python's object).
# And we will access it as indicator.sma_9 only in the code.


class Indicator(BaseModel):

    # Identifier fields
    symbol: str
    resolution: str
    open_time:  datetime.datetime
    close_price: float | None = Field(default=None, alias='close_price')

    # Indicator fields
    vwap_session:      float | None = Field(default=None, alias='vwap_session')
    vwap_numerator:    float | None = Field(default=None, alias='vwap_numerator')
    vwap_denominator:  float | None = Field(default=None, alias='vwap_denominator')

    # Simple Moving Averages
    sma_9: float | None = Field(default=None, alias="sma_9")
    sma_21: float | None = Field(default=None, alias="sma_21")
    sma_50: float | None = Field(default=None, alias="sma_50")
    sma_200: float | None = Field(default=None, alias="sma_200")

    # Exponential Moving Averages
    ema_9: float | None = Field(default=None, alias="ema_9")
    ema_12: float | None = Field(default=None, alias="ema_12")
    ema_21: float | None = Field(default=None, alias="ema_21")
    ema_26: float | None = Field(default=None, alias="ema_26")  
    ema_50: float | None = Field(default=None, alias="ema_50")
    ema_200: float | None = Field(default=None, alias="ema_200")

    # Relative Strength Index
    rsi_14: float | None = Field(default=None, alias="rsi_14")
    rsi_avg_gain_14: float | None = Field(default=None, alias="rsi_avg_gain_14")
    rsi_avg_loss_14: float | None = Field(default=None, alias="rsi_avg_loss_14")

    # Moving Average Convergence Divergence
    macd_line: float | None = Field(default=None, alias="macd_line")
    macd_signal: float | None = Field(default=None, alias="macd_signal")
    macd_histogram: float | None = Field(default=None, alias="macd_histogram")

    # Bollinger Bands
    bb_upper: float | None = Field(default=None, alias="bb_upper")
    bb_middle: float | None = Field(default=None, alias="bb_middle")
    bb_lower: float | None = Field(default=None, alias="bb_lower")
    bb_bandwidth: float | None = Field(default=None, alias="bb_bandwidth")


    