CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw tick data table
CREATE TABLE IF NOT EXISTS ticks (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (time, symbol)
);

-- Convert to hypertable
SELECT create_hypertable('ticks', 'time', if_not_exists => TRUE);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_time ON ticks (symbol, time DESC);

-- Resampled OHLCV data (1s, 1m, 5m)
CREATE TABLE IF NOT EXISTS ohlcv (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    trade_count INTEGER NOT NULL,
    PRIMARY KEY (time, symbol, interval)
);

SELECT create_hypertable('ohlcv', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_interval ON ohlcv (symbol, interval, time DESC);

-- Analytics results table
CREATE TABLE IF NOT EXISTS analytics (
    time TIMESTAMPTZ NOT NULL,
    symbol1 TEXT NOT NULL,
    symbol2 TEXT,
    metric_type TEXT NOT NULL,
    value DOUBLE PRECISION,
    metadata JSONB,
    PRIMARY KEY (time, symbol1, metric_type, COALESCE(symbol2, ''))
);

SELECT create_hypertable('analytics', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_analytics_symbols ON analytics (symbol1, symbol2, metric_type, time DESC);

-- Alerts table
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    alert_type TEXT NOT NULL,
    symbol1 TEXT NOT NULL,
    symbol2 TEXT,
    condition TEXT NOT NULL,
    threshold DOUBLE PRECISION NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    triggered_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts (active, symbol1);

-- Alert history
CREATE TABLE IF NOT EXISTS alert_history (
    time TIMESTAMPTZ NOT NULL,
    alert_id INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    metadata JSONB,
    PRIMARY KEY (time, alert_id)
);

SELECT create_hypertable('alert_history', 'time', if_not_exists => TRUE);

-- Continuous aggregates for 1s
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1s
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 second', time) AS time,
    symbol,
    '1s' as interval,
    FIRST(price, time) as open,
    MAX(price) as high,
    MIN(price) as low,
    LAST(price, time) as close,
    SUM(size) as volume,
    COUNT(*) as trade_count
FROM ticks
GROUP BY time_bucket('1 second', time), symbol;

-- Continuous aggregates for 1m
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS time,
    symbol,
    '1m' as interval,
    FIRST(price, time) as open,
    MAX(price) as high,
    MIN(price) as low,
    LAST(price, time) as close,
    SUM(size) as volume,
    COUNT(*) as trade_count
FROM ticks
GROUP BY time_bucket('1 minute', time), symbol;

-- Continuous aggregates for 5m
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS time,
    symbol,
    '5m' as interval,
    FIRST(price, time) as open,
    MAX(price) as high,
    MIN(price) as low,
    LAST(price, time) as close,
    SUM(size) as volume,
    COUNT(*) as trade_count
FROM ticks
GROUP BY time_bucket('5 minutes', time), symbol;

-- Refresh policies
SELECT add_continuous_aggregate_policy('ohlcv_1s',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 second',
    schedule_interval => INTERVAL '1 second',
    if_not_exists => TRUE);

SELECT add_continuous_aggregate_policy('ohlcv_1m',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);

SELECT add_continuous_aggregate_policy('ohlcv_5m',
    start_offset => INTERVAL '6 hours',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE);