-- ==============================================================================
-- Schema: coinbase
-- Purpose: Market trades ingestion pipeline (staging → raw → aggregated)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Staging Table: coinbase.stg_market_trades
-- ------------------------------------------------------------------------------
-- Purpose:
-- - Temporary landing table for incoming trade events (e.g., from Kafka ingestion)
-- - Designed for high-throughput inserts (minimal constraints)
-- - Data is later deduplicated and moved to raw + aggregated tables

CREATE TABLE coinbase.stg_market_trades (
	-- Unique trade identifier (may be NULL in staging, validated downstream)
	trade_id text,

    -- Trading pair identifier (e.g., BTC-USD)
    product_id TEXT,

    -- Trade timestamp (UTC)
    trade_time_utc TIMESTAMPTZ,

    -- Trade direction (BUY / SELL)
    side text NOT NULL,

    -- Quantity traded
    size double precision NOT NULL,

    -- Trade notional value (price * size)
    trade_value double precision NOT NULL
);


-- ------------------------------------------------------------------------------
-- Raw Table: coinbase.raw_market_trades
-- ------------------------------------------------------------------------------
-- Purpose:
-- - Stores deduplicated, immutable trade records
-- - Acts as source of truth for all downstream aggregations
-- - Enforces uniqueness via primary key (trade_id)

CREATE TABLE coinbase.raw_market_trades (
    -- Unique trade identifier (primary key for deduplication)
    trade_id text PRIMARY KEY,

    -- Trading pair identifier
    product_id text NOT NULL,

    -- Trade direction
    side text NOT NULL,

    -- Quantity traded
    size double precision NOT NULL,

    -- Notional value
    trade_value double precision NOT NULL,

    -- Trade timestamp (UTC)
    trade_time_utc timestamptz NOT NULL,

    -- Ingestion timestamp (when record entered system)
    inserted_at_utc timestamptz NOT NULL DEFAULT now()
);


-- ------------------------------------------------------------------------------
-- Index: Trade Time Index
-- ------------------------------------------------------------------------------
-- Purpose:
-- - Improves performance for time-based queries (common in analytics)
-- - Supports filtering, aggregation, and retention operations

CREATE INDEX idx_raw_market_trades_trade_time
ON coinbase.raw_market_trades (trade_time_utc);


-- ------------------------------------------------------------------------------
-- Aggregated Table: coinbase.market_trades_agg
-- ------------------------------------------------------------------------------
-- Purpose:
-- - Stores time-bucketed trade metrics (e.g., per minute)
-- - Used for analytics, dashboards, and downstream processing
-- - Partitioned for scalability and efficient time-based querying

CREATE TABLE coinbase.market_trades_agg (
    -- Trading pair identifier
    product_id TEXT NOT NULL,

    -- Aggregation timestamp (minute-level)
    trade_time_utc TIMESTAMPTZ NOT NULL,

    -- Number of trades in interval
    trade_count BIGINT NOT NULL,

    -- Total traded volume
    total_volume DOUBLE PRECISION NOT NULL,

    -- Volume classified as "buy" (based on trade side logic)
    buy_volume DOUBLE PRECISION NOT NULL,

    -- Volume classified as "sell"
    sell_volume DOUBLE PRECISION NOT NULL,

    -- Total notional value traded
    notional_value DOUBLE precision not NULL,

    -- Composite primary key:
    -- Ensures uniqueness per (product_id, time bucket)
    -- Supports efficient UPSERT operations
    PRIMARY KEY (product_id, trade_time_utc)

-- ------------------------------------------------------------------------------
-- Partitioning Strategy
-- ------------------------------------------------------------------------------
-- RANGE partitioning on trade_time_utc:
-- - Enables partition pruning for time-based queries
-- - Improves performance at scale
-- - Requires periodic partition creation

) PARTITION BY RANGE (trade_time_utc);