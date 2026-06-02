-- CREATE DATABASE MARKETDATA;

-- USE MARKETDATA;

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS raw_trades (
    symbol      TEXT            NOT NULL,
    price       NUMERIC(12,4)   NOT NULL,
    volume      INTEGER         NOT NULL,
    trade_time  TIMESTAMPTZ     NOT NULL,
    conditions  TEXT[],
    source      TEXT DEFAULT 'finnhub',
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

SELECT create_hypertable(
    'raw_trades',
    'trade_time',
    if_not_exists => TRUE
);

-- CREATE TABLE candles (
--     symbol       TEXT            NOT NULL,
--     resolution   TEXT            NOT NULL,   -- '1m','5m','15m','1h'
--     open_time    TIMESTAMPTZ     NOT NULL,
--     open         NUMERIC(12, 4),
--     high         NUMERIC(12, 4),
--     low          NUMERIC(12, 4),
--     close        NUMERIC(12, 4),
--     volume       BIGINT,
--     trade_count  INTEGER,
--     vwap         NUMERIC(12, 4),
--     PRIMARY KEY (symbol, resolution, open_time)
-- );
-- SELECT create_hypertable('candles', 'open_time');


-- CREATE TABLE indicators (
--     symbol      TEXT            NOT NULL,
--     resolution  TEXT            NOT NULL,
--     timestamp   TIMESTAMPTZ     NOT NULL,
--     ema_9       NUMERIC(12, 4),
--     ema_21      NUMERIC(12, 4),
--     ema_50      NUMERIC(12, 4),
--     rsi_14      NUMERIC(6, 2),
--     macd        NUMERIC(12, 4),
--     macd_signal NUMERIC(12, 4),
--     macd_hist   NUMERIC(12, 4),
--     bb_upper    NUMERIC(12, 4),
--     bb_middle   NUMERIC(12, 4),
--     bb_lower    NUMERIC(12, 4),
--     vwap        NUMERIC(12, 4),
--     atr_14      NUMERIC(12, 4),
--     PRIMARY KEY (symbol, resolution, timestamp)
-- );
-- SELECT create_hypertable('indicators', 'timestamp');


-- CREATE TABLE signals (
--     symbol      TEXT            NOT NULL,
--     timestamp   TIMESTAMPTZ     NOT NULL,
--     signal_type TEXT            NOT NULL,   -- 'BUY','SELL'
--     strategy    TEXT            NOT NULL,   -- 'rsi_oversold','ema_cross', etc.
--     price       NUMERIC(12, 4),
--     strength    NUMERIC(4, 2),              -- 0.0 to 1.0
--     metadata    JSONB                       -- extra context
-- );
-- SELECT create_hypertable('signals', 'timestamp');