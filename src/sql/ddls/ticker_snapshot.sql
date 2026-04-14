CREATE TABLE coinbase.stg_ticker_snapshot (
    product_id TEXT,
    ticker_ts_utc TIMESTAMPTZ,

    price DOUBLE PRECISION,
    best_bid DOUBLE PRECISION,
    best_ask DOUBLE PRECISION,
    mid_price DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    price_pct_chg_24h DOUBLE PRECISION,
    volume_24h DOUBLE PRECISION,
    high_24h DOUBLE PRECISION,
    low_24h DOUBLE PRECISION
);

CREATE TABLE coinbase.ticker_snapshot (
    product_id TEXT NOT NULL,
    ticker_ts_utc TIMESTAMPTZ NOT NULL,

    price DOUBLE PRECISION NOT NULL,
    best_bid DOUBLE PRECISION NOT NULL,
    best_ask DOUBLE PRECISION NOT NULL,
    mid_price DOUBLE PRECISION NOT NULL,
    spread DOUBLE PRECISION NOT NULL,
    price_pct_chg_24h DOUBLE PRECISION NOT NULL,
    volume_24h DOUBLE PRECISION NOT NULL,
    high_24h DOUBLE PRECISION NOT NULL,
    low_24h DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (product_id, ticker_ts_utc)
) PARTITION BY RANGE (ticker_ts_utc);