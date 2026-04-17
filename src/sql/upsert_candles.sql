-- ==============================================================================
-- Purpose: Upsert aggregated candle data from staging table into final table
-- ==============================================================================

-- This query performs the following:
-- 1. Selects the latest unique candle record per (product_id, candle_ts_utc)
-- 2. Inserts into the final table
-- 3. Updates existing records if conflicts occur (UPSERT pattern)
-- 4. Clears the staging table after processing

INSERT INTO {final_table} (
    -- Unique identifier for trading pair (e.g., BTC-USD)
    product_id,

    -- OHLC candle values
    open,
    high,
    low,
    close,

    -- Derived metrics
    range,        -- high - low
    avg_price,    -- Average price for the interval

    -- Candle timestamp (UTC)
    candle_ts_utc
)

-- ------------------------------------------------------------------------------
-- Select Latest Unique Records from Staging
-- ------------------------------------------------------------------------------
-- DISTINCT ON ensures only one row per (product_id, candle_ts_utc)
-- ORDER BY DESC ensures we pick the most recent record for duplicates

SELECT DISTINCT ON (product_id, candle_ts_utc)
    product_id,
    open,
    high,
    low,
    close,
    range,
    avg_price,
    candle_ts_utc
FROM {staging_table}

-- Ignore invalid records with missing timestamps
WHERE candle_ts_utc IS NOT NULL

-- PostgreSQL-specific behavior:
-- DISTINCT ON keeps the FIRST row per group based on ORDER BY
ORDER BY product_id, candle_ts_utc DESC

-- ------------------------------------------------------------------------------
-- Conflict Handling (UPSERT)
-- ------------------------------------------------------------------------------
-- If a record already exists with same (product_id, candle_ts_utc),
-- update it with latest values from staging

ON CONFLICT (product_id, candle_ts_utc)
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    range = EXCLUDED.range,
    avg_price = EXCLUDED.avg_price;

-- ------------------------------------------------------------------------------
-- Cleanup Staging Table
-- ------------------------------------------------------------------------------
-- Removes all processed data to prepare for next ingestion cycle
-- Assumes data has been safely persisted in final_table

TRUNCATE {staging_table};