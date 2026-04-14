CREATE TABLE coinbase.stg_candles_snapshot (
    product_id TEXT,
    candle_ts_utc TIMESTAMPTZ,
    
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    range DOUBLE PRECISION,
    avg_price DOUBLE PRECISION
);


CREATE TABLE coinbase.candles_snapshot (
    product_id TEXT NOT NULL,
    candle_ts_utc TIMESTAMPTZ NOT NULL,
    
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    range DOUBLE PRECISION NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (product_id, candle_ts_utc)
) PARTITION BY RANGE (candle_ts_utc);