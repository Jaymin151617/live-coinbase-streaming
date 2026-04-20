-- ==============================================================================
-- Schema: coinbase
-- Purpose: Ticker snapshot ingestion (staging → final, partitioned)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Staging Table: coinbase.stg_ticker_snapshot
-- ------------------------------------------------------------------------------
-- Purpose:
-- - Temporary landing table for incoming ticker data (e.g., from Kafka ingestion)
-- - Optimized for high-throughput writes (no constraints)
-- - Data is validated, deduplicated, and upserted into final table

CREATE TABLE coinbase.stg_ticker_snapshot (
    -- Trading pair identifier (e.g., BTC-USD)
    product_id TEXT,

    -- Ticker event timestamp (UTC)
    ticker_ts_utc TIMESTAMPTZ,

    -- Latest traded price
    price DOUBLE PRECISION,

    -- Top-of-book bid/ask
    best_bid DOUBLE PRECISION,
    best_ask DOUBLE PRECISION,

    -- Derived pricing metrics
    mid_price DOUBLE PRECISION,      -- (best_bid + best_ask) / 2
    spread DOUBLE PRECISION,         -- best_ask - best_bid

    -- 24-hour rolling statistics
    price_pct_chg_24h DOUBLE PRECISION,  -- % change over 24h
    volume_24h DOUBLE PRECISION,         -- 24h traded volume
    high_24h DOUBLE PRECISION,           -- 24h high price
    low_24h DOUBLE PRECISION             -- 24h low price
);


-- ------------------------------------------------------------------------------
-- Final Table: coinbase.ticker_snapshot
-- ------------------------------------------------------------------------------
-- Purpose:
-- - Stores validated, deduplicated ticker data
-- - Represents point-in-time market snapshots
-- - Partitioned for scalability and efficient time-based queries

CREATE TABLE coinbase.ticker_snapshot (
    -- Trading pair identifier (required)
    product_id TEXT NOT NULL,

    -- Ticker timestamp (required, used for partitioning)
    ticker_ts_utc TIMESTAMPTZ NOT NULL,

    -- Latest traded price (required for completeness)
    price DOUBLE PRECISION NOT NULL,

    -- Order book best prices
    best_bid DOUBLE PRECISION NOT NULL,
    best_ask DOUBLE PRECISION NOT NULL,

    -- Derived metrics
    mid_price DOUBLE PRECISION NOT NULL,
    spread DOUBLE PRECISION NOT NULL,

    -- 24-hour statistics
    price_pct_chg_24h DOUBLE PRECISION NOT NULL,
    volume_24h DOUBLE PRECISION NOT NULL,
    high_24h DOUBLE PRECISION NOT NULL,
    low_24h DOUBLE PRECISION NOT NULL,

    -- Composite primary key:
    -- Ensures uniqueness per (product_id, timestamp)
    -- Enables efficient UPSERT operations
    PRIMARY KEY (product_id, ticker_ts_utc)

-- ------------------------------------------------------------------------------
-- Partitioning Strategy
-- ------------------------------------------------------------------------------
-- RANGE partitioning on ticker_ts_utc:
-- - Enables partition pruning for time-based queries
-- - Improves performance at scale
-- - Requires periodic partition creation

) PARTITION BY RANGE (ticker_ts_utc);