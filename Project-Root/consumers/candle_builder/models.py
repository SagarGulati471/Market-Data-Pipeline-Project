import datetime
from pydantic import BaseModel, Field, computed_field

class Candle(BaseModel):
    open: float       
    close: float        
    high: float         
    low: float          
    volume: int         
    symbol: str         
    resolution: str     
    bucket_ts: int      
    trade_count: int    = Field(alias='trade_count')
    vwap: float         = Field(alias='vwap')
    is_partial: bool    = Field(alias='is_partial')
    open_time: datetime.datetime = Field(alias='open_time')
