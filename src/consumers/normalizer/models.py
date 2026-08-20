import hashlib
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, computed_field


class Trade(BaseModel):
    symbol:     str            = Field(alias='s')
    price:      float          = Field(alias='p')
    quantity:   int            = Field(alias='v')
    timestamp:  int            = Field(alias='t')   # raw Unix ms — retained for hashing
    conditions: Optional[list] = Field(default=None, alias='c')


    
    @computed_field
    @property
    def trade_time(self) -> datetime:
        """
        UTC datetime derived from the raw Unix millisecond timestamp.
        Stored as a computed field so it is included in model_dump() output
        and serialized when the trade is produced to the downstream Kafka topic.

        A @computed_field means it's included in model_dump() automatically.
        When this trade is later serialized to JSON and produced to the trades-normalized topic,
        the downstream candle builder will receive a proper UTC datetime string, not a raw integer
        it has to convert itself.

        """
        return datetime.fromtimestamp(self.timestamp / 1000, tz=timezone.utc)

    @property
    def trade_hash(self) -> str:
        """
        Deterministic SHA-256 fingerprint used for in-memory deduplication.

        Keyed on (symbol, timestamp_ms, price, quantity). Condition codes are
        intentionally excluded Finnhub may retransmit the same trade execution
        with different or missing condition codes, and those should still be
        treated as duplicates.
        """
        key = f"{self.symbol}:{self.timestamp}:{self.price}:{self.quantity}"
        return hashlib.sha256(key.encode()).hexdigest()
