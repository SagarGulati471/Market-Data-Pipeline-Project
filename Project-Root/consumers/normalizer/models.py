from pydantic import BaseModel, Field
from typing import Optional


class Trade(BaseModel):
    symbol: str = Field(alias='s')
    price: float = Field(alias='p')
    quantity: int = Field(alias='v')
    timestamp: int = Field(alias='t')
    conditions: Optional[list] = Field(default=None, alias='c')