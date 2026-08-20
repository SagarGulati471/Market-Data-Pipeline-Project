


### Command to check all available installed extentions in the Postgres

SELECT *
FROM pg_available_extensions 
WHERE name = 'timescaledb';


SELECT default_version, installed_version 
FROM pg_available_extensions 
WHERE name = 'timescaledb';


SELECT extname, extversion FROM pg_extension WHERE extname = 'timescaledb';


### Activate a particular extension which is installed

CREATE EXTENSION IF NOT EXISTS timescaledb;


### Get list of all hypertables

SELECT 
    hypertable_schema, 
    hypertable_name, 
    num_chunks, 
    compression_enabled 
FROM timescaledb_information.hypertables;






```
```
---

## Complete Table Structure

### `raw_trades` — every individual tick
| Column | Type | Description |
|---|---|---|
| `symbol` | TEXT | Ticker symbol (AMZN) |
| `price` | NUMERIC(12,4) | Execution price |
| `volume` | INTEGER | Shares in this trade |
| `trade_time` | TIMESTAMPTZ | Exact trade timestamp |
| `conditions` | TEXT[] | Condition codes (["1"] = regular) |
| `source` | TEXT | Data source (finnhub, fyers) |

Primary key: `(symbol, trade_time, price, volume)` — deduplication

### `candles` — OHLCV candles, all resolutions
| Column | Type | Description |
|---|---|---|
| `symbol` | TEXT | Ticker |
| `resolution` | TEXT | '1m', '5m', '15m', '1h' |
| `open_time` | TIMESTAMPTZ | Candle start time (bucket floor) |
| `open` | NUMERIC(12,4) | First trade price in bucket |
| `high` | NUMERIC(12,4) | Highest trade price |
| `low` | NUMERIC(12,4) | Lowest trade price |
| `close` | NUMERIC(12,4) | Last trade price |
| `volume` | BIGINT | Total shares traded |
| `trade_count` | INTEGER | Number of ticks in candle |
| `vwap` | NUMERIC(12,4) | Volume-weighted avg price |

Primary key: `(symbol, resolution, open_time)`

### `indicators` — computed metrics per candle
| Column | Type | Description |
|---|---|---|
| `symbol` | TEXT | Ticker |
| `resolution` | TEXT | Candle resolution |
| `timestamp` | TIMESTAMPTZ | Candle close time |
| `ema_9` | NUMERIC(12,4) | 9-period exponential moving avg |
| `ema_21` | NUMERIC(12,4) | 21-period EMA |
| `ema_50` | NUMERIC(12,4) | 50-period EMA |
| `rsi_14` | NUMERIC(6,2) | 14-period RSI (0–100) |
| `macd` | NUMERIC(12,4) | MACD line (EMA12 − EMA26) |
| `macd_signal` | NUMERIC(12,4) | Signal line (EMA9 of MACD) |
| `macd_hist` | NUMERIC(12,4) | MACD − Signal |
| `bb_upper` | NUMERIC(12,4) | Bollinger upper band |
| `bb_middle` | NUMERIC(12,4) | Bollinger middle (SMA20) |
| `bb_lower` | NUMERIC(12,4) | Bollinger lower band |
| `vwap` | NUMERIC(12,4) | Intraday VWAP at this candle |
| `atr_14` | NUMERIC(12,4) | 14-period Average True Range |

Primary key: `(symbol, resolution, timestamp)`

### `signals` — trading signals
| Column | Type | Description |
|---|---|---|
| `symbol` | TEXT | Ticker |
| `timestamp` | TIMESTAMPTZ | When signal fired |
| `signal_type` | TEXT | 'BUY' or 'SELL' |
| `strategy` | TEXT | Which rule triggered it |
| `price` | NUMERIC(12,4) | Price at signal time |
| `strength` | NUMERIC(4,2) | Confidence 0.0–1.0 |
| `metadata` | JSONB | Extra context (RSI value, etc.) |
