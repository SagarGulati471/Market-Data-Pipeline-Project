from decimal import Decimal
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
from enum import Enum

class OrderSide(str, Enum):
    BUY  =  "BUY"
    SELL =  "SELL"

class OrderType(str, Enum):
    MARKET =  "MARKET"
    LIMIT  =  "LIMIT"
    STOP   =  "STOP"

class OrderStatus(str, Enum):
    PENDING   =  "PENDING"      # order has been created but not yet sent to the broker.
    SUBMITTED =  "SUBMITTED"    # order has been sent to the broker but not yet executed.
    FILLED    =  "FILLED"       # fully executed by the broker.
    REJECTED  =  "REJECTED"     # broker refused it (insufficient funds, invalid symbol).
    CANCELLED =  "CANCELLED"    # broker cancelled it (user requested cancellation).
    PARTIAL   =  "PARTIAL"      # partially executed by the broker (some quantity filled, some still pending).
    

class Order(BaseModel):
    order_id:       str         = Field(..., description="Unique identifier for the order")
    symbol:         str         = Field(..., description="Trading symbol for the order")
    side:           OrderSide   = Field(..., description="Side of the order (buy/sell)")
    type:           OrderType   = Field(..., description="Type of the order (market/limit/stop)")
    quantity:       int         = Field(..., description="Quantity of the asset to be traded")
    price:          Decimal       = Field(..., description="Price at which the order is placed")
    filled_price:   Optional[Decimal] = Field(None, description="Price at which the order was filled (if applicable)")
    status:         OrderStatus = Field(..., description="Current status of the order (pending/filled/cancelled)")
    timestamp:      datetime    = Field(..., description="Timestamp when the order was created")
