

# Indicator Pipeline


## - The DB Model Structure


### Design Decision

#### Initial Approach

My initial design was to introduce a surrogate `candle_id` as the primary key in the **Candles** table and reference it from the **Indicators** table using a foreign key.

```
Candles
---------
candle_id (PK)
...

Indicators
-----------
candle_id (FK)
...
```

#### Revised Design

After researching TimescaleDB best practices, I found that foreign keys between hypertables are generally discouraged. They can negatively impact chunk management, compression, partitioning, and overall write performance.

Additionally, the **Candles** table already has a natural identifier that uniquely represents each candle:

```text
(symbol, resolution, open_time)
```

This composite key uniquely identifies a candle across all symbols and timeframes, making a separate surrogate `candle_id` unnecessary.

Therefore, the **Indicators** table uses the same composite key to identify the corresponding candle:

* `symbol`
* `resolution`
* `open_time`

No foreign key constraint is defined between the tables. Instead, both tables share the same natural key, allowing direct lookups while preserving TimescaleDB performance characteristics and keeping the schema simple.



## Current Model Design

| Field            | Description                                                                                                                                    |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `symbol`         | Stock ticker symbol (e.g., `AAPL`, `MSFT`).                                                                                                    |
| `resolution`     | Candle timeframe (e.g., `1m`, `5m`, `15m`, `1h`, `1d`).                                                                                        |
| `open_time`      | Start timestamp of the candle. Serves as part of the unique identifier for a candle.                                                           |
| `vwap_session`   | **Session VWAP** (Volume Weighted Average Price), calculated cumulatively from market open and reset at the beginning of each trading session. |
| `sma_9`          | 9-period Simple Moving Average (SMA).                                                                                                          |
| `sma_21`         | 21-period Simple Moving Average (SMA).                                                                                                         |
| `sma_50`         | 50-period Simple Moving Average (SMA).                                                                                                         |
| `sma_200`        | 200-period Simple Moving Average (SMA).                                                                                                        |
| `ema_9`          | 9-period Exponential Moving Average (EMA).                                                                                                     |
| `ema_21`         | 21-period Exponential Moving Average (EMA).                                                                                                    |
| `ema_50`         | 50-period Exponential Moving Average (EMA).                                                                                                    |
| `ema_200`        | 200-period Exponential Moving Average (EMA).                                                                                                   |
| `rsi_14`         | 14-period Relative Strength Index (RSI), used to measure momentum and overbought/oversold conditions.                                          |
| `macd_line`      | MACD line = EMA(12) − EMA(26).                                                                                                                 |
| `macd_signal`    | Signal line = 9-period EMA of the MACD line.                                                                                                   |
| `macd_histogram` | Difference between the MACD line and the Signal line (`MACD - Signal`).                                                                        |
| `bb_upper`       | Upper Bollinger Band = SMA(20) + 2 × Standard Deviation.                                                                                       |
| `bb_middle`      | Middle Bollinger Band = 20-period SMA.                                                                                                         |
| `bb_lower`       | Lower Bollinger Band = SMA(20) − 2 × Standard Deviation.                                                                                       |
| `bb_bandwidth`   | Normalized width of the Bollinger Bands, calculated as `(Upper Band - Lower Band) / Middle Band`. Indicates market volatility.                 |





## Future Scope

If in the future we add a UI layer, and provide the user an option to apply a custom indicator, then
we can create it as an API 


```markdown
Browser (UI)
    │  POST /indicators/compute
    │  { symbol, resolution, indicator: "RSI", params: {period: 7}, from, to }
    ▼
API Server
    │  SELECT * FROM candles WHERE symbol=... ORDER BY open_time DESC LIMIT N
    ▼
Compute layer  (same indicator functions, called on-demand)
    │
    ▼
Return [{open_time, value}]  →  render on chart
```




