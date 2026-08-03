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

-- Deduplication constraint: the application layer uses ON CONFLICT DO NOTHING
-- which requires a unique index on the columns that define a duplicate trade.
-- This is the DB-level guard that survives service restarts — the in-memory
-- set in handler.py only catches duplicates within a single running session.
CREATE UNIQUE INDEX IF NOT EXISTS raw_trades_dedup_idx
    ON raw_trades (symbol, trade_time, price, volume);

CREATE TABLE IF NOT EXISTS candles (
    symbol       TEXT            NOT NULL,
    resolution   TEXT            NOT NULL,   -- '1m','5m','15m','1h'
    open_time    TIMESTAMPTZ NOT NULL,
    open         NUMERIC(12, 4),
    high         NUMERIC(12, 4),
    low          NUMERIC(12, 4),
    close        NUMERIC(12, 4),
    volume       BIGINT,
    trade_count  INTEGER,
    vwap         NUMERIC(12, 4),
    is_partial   BOOLEAN,        -- true if the candle is still being built
    close_time   TIMESTAMPTZ,
    PRIMARY KEY (symbol, resolution, open_time)
);
SELECT create_hypertable('candles', 'open_time');



CREATE TABLE IF NOT EXISTS indicators (
    symbol           TEXT            NOT NULL,
    resolution       TEXT            NOT NULL,
    open_time        TIMESTAMPTZ     NOT NULL,
    close_price      DOUBLE PRECISION,

    -- Session VWAP (cumulative from market open, resets daily)
    vwap_session     DOUBLE PRECISION,
    vwap_numerator   DOUBLE PRECISION,
    vwap_denominator DOUBLE PRECISION,

    -- Simple Moving Averages
    sma_9            DOUBLE PRECISION,
    sma_21           DOUBLE PRECISION,
    sma_50           DOUBLE PRECISION,
    sma_200          DOUBLE PRECISION,

    -- Exponential Moving Averages
    ema_9            DOUBLE PRECISION,
    ema_12           DOUBLE PRECISION,
    ema_26           DOUBLE PRECISION,
    ema_21           DOUBLE PRECISION,
    ema_50           DOUBLE PRECISION,
    ema_200          DOUBLE PRECISION,

    -- RSI
    rsi_14           DOUBLE PRECISION,
    rsi_avg_gain_14    DOUBLE PRECISION,
    rsi_avg_loss_14    DOUBLE PRECISION,

    -- MACD (fast=12, slow=26, signal=9)
    macd_line        DOUBLE PRECISION,   -- EMA(12) - EMA(26)
    macd_signal      DOUBLE PRECISION,   -- EMA(9) of macd_line
    macd_histogram   DOUBLE PRECISION,   -- macd_line - macd_signal

    -- Bollinger Bands (period=20, std_dev=2)
    bb_upper         DOUBLE PRECISION,   -- SMA(20) + 2*stddev
    bb_middle        DOUBLE PRECISION,   -- SMA(20)
    bb_lower         DOUBLE PRECISION,   -- SMA(20) - 2*stddev
    bb_bandwidth     DOUBLE PRECISION,   -- (upper - lower) / middle

    PRIMARY KEY (symbol, resolution, open_time)
);

SELECT create_hypertable('indicators', 'open_time');




CREATE TABLE IF NOT EXISTS signals (
    symbol                     TEXT            NOT NULL,
    resolution                 TEXT            NOT NULL,
    open_time                  TIMESTAMPTZ     NOT NULL,
    signal_type                TEXT            NOT NULL CHECK (signal_type IN ('BUY', 'SELL', 'HOLD', 'STRONG_BUY', 'STRONG_SELL')),
    strategy_EMA_crossover     TEXT            NOT NULL CHECK (strategy_EMA_crossover IN ('BUY', 'SELL', 'HOLD')),
    strategy_RSI_reversal      TEXT            NOT NULL CHECK (strategy_RSI_reversal IN ('BUY', 'SELL', 'HOLD')),
    strategy_MACD_crossover    TEXT            NOT NULL CHECK (strategy_MACD_crossover IN ('BUY', 'SELL', 'HOLD')),
    strategy_VWAP_confluence   TEXT            NOT NULL CHECK (strategy_VWAP_confluence IN ('BUY', 'SELL', 'HOLD')),
    weighted_score             FLOAT           NOT NULL,
    threshold                  FLOAT           NOT NULL,
    PRIMARY KEY (symbol, resolution, open_time)
);
SELECT create_hypertable('signals', 'open_time');