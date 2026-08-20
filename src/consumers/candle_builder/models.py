import datetime
from pydantic import BaseModel, Field, computed_field

class Candle(BaseModel):
    symbol: str         
    resolution: str
    open_time: datetime.datetime = Field(alias='open_time')
    open: float       
    high: float         
    low: float        
    close: float          
    volume: int         
    trade_count: int    = Field(alias='trade_count')
    vwap: float         = Field(alias='vwap')
    is_partial: bool    = Field(alias='is_partial')
    close_time: datetime.datetime = Field(alias='close_time')


class NormalizedTrade(BaseModel):
    symbol: str
    price: float
    quantity: int
    timestamp: int
    conditions: list = Field(default=None)
