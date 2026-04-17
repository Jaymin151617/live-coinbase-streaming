-- ==============================================================================
-- Schema: coinbase
-- Purpose: Candle snapshot storage (staging + partitioned final table)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Staging Table: coinbase.stg_candles_snapshot
-- ------------------------------------------------------------------------------
-- Purpose:
-- - Temporary landing table for incoming candle data (e.g., from Spark consumer)
-- - Allows bulk inserts before upsert into final table
-- - No constraints for maximum ingestion speed

CREATE TABLE coinbase.stg_candles_snapshot (
    -- Trading pair identifier (e.g., BTC-USD)
    product_id TEXT,

    -- Candle timestamp (UTC)
    candle_ts_utc TIMESTAMPTZ,
    
    -- OHLC values
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,

    -- Derived metrics
    range DOUBLE PRECISION,      -- high - low
    avg_price DOUBLE PRECISION   -- average price over interval
);


-- ------------------------------------------------------------------------------
-- Final Table: coinbase.candles_snapshot
-- ------------------------------------------------------------------------------
-- Purpose:
-- - Stores validated, deduplicated, and production-ready candle data
-- - Supports time-based partitioning for scalability and query performance

CREATE TABLE coinbase.candles_snapshot (
    -- Trading pair identifier (required)
    product_id TEXT NOT NULL,

    -- Candle timestamp (required, used for partitioning)
    candle_ts_utc TIMESTAMPTZ NOT NULL,
    
    -- OHLC values (all required for data completeness)
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,

    -- Derived metrics (required)
    range DOUBLE PRECISION NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,

    -- Composite primary key ensures:
    -- 1. Uniqueness per product per timestamp
    -- 2. Efficient conflict handling during UPSERT
    PRIMARY KEY (product_id, candle_ts_utc)

-- ------------------------------------------------------------------------------
-- Partitioning Strategy
-- ------------------------------------------------------------------------------
-- RANGE partitioning on candle_ts_utc:
-- - Enables efficient pruning for time-based queries
-- - Improves performance for large datasets
-- - Requires child partitions to be created (e.g., daily/monthly)

) PARTITION BY RANGE (candle_ts_utc);