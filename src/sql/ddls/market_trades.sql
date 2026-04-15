CREATE TABLE coinbase.stg_market_trades (
	trade_id text,
    product_id TEXT,
    trade_time_utc TIMESTAMPTZ,

    side                text NOT NULL,
    size                double precision NOT NULL,
    trade_value         double precision NOT NULL
);

CREATE TABLE coinbase.raw_market_trades (
    trade_id            text PRIMARY KEY,
    product_id          text NOT NULL,
    side                text NOT NULL,
    size                double precision NOT NULL,
    trade_value         double precision NOT NULL,
    trade_time_utc      timestamptz NOT NULL,
    inserted_at_utc     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_raw_market_trades_trade_time
ON coinbase.raw_market_trades (trade_time_utc);

CREATE TABLE coinbase.market_trades_agg (
    product_id TEXT NOT NULL,
    trade_time_utc TIMESTAMPTZ NOT NULL,

    trade_count BIGINT NOT NULL,
    total_volume DOUBLE PRECISION NOT NULL,
    buy_volume DOUBLE PRECISION NOT NULL,
    sell_volume DOUBLE PRECISION NOT NULL,
    notional_value DOUBLE precision not NULL,

    PRIMARY KEY (product_id, trade_time_utc)
) PARTITION BY RANGE (trade_time_utc);