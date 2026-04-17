-- ==============================================================================
-- Purpose: Upsert latest ticker data into final table from staging
-- ==============================================================================

-- This query performs:
-- 1. Selects the most recent ticker record per (product_id, ticker_ts_utc)
-- 2. Inserts into final table
-- 3. Updates existing records if conflicts occur (UPSERT pattern)
-- 4. Clears staging table after processing

INSERT INTO {final_table} (
    -- Trading pair identifier (e.g., BTC-USD)
    product_id,

    -- Latest traded price
    price,

    -- Order book top levels
    best_bid,     -- Highest bid price
    best_ask,     -- Lowest ask price

    -- Derived pricing metrics
    mid_price,    -- (best_bid + best_ask) / 2
    spread,       -- best_ask - best_bid

    -- 24-hour rolling statistics
    price_pct_chg_24h,  -- Percentage price change over 24h
    volume_24h,         -- Total traded volume over 24h
    high_24h,           -- Highest price in last 24h
    low_24h,            -- Lowest price in last 24h

    -- Event timestamp (UTC)
    ticker_ts_utc
)

-- ------------------------------------------------------------------------------
-- Select Latest Unique Records from Staging
-- ------------------------------------------------------------------------------
-- DISTINCT ON ensures only one row per (product_id, ticker_ts_utc)
-- ORDER BY DESC ensures the most recent record is selected in case of duplicates

SELECT DISTINCT ON (product_id, ticker_ts_utc)
    product_id,
    price,
    best_bid,
    best_ask,
    mid_price,
    spread,
    price_pct_chg_24h,
    volume_24h,
    high_24h,
    low_24h,
    ticker_ts_utc
FROM {staging_table}

-- Exclude invalid or incomplete records
WHERE ticker_ts_utc IS NOT NULL

-- PostgreSQL-specific behavior:
-- DISTINCT ON keeps the FIRST row per group based on ORDER BY
ORDER BY product_id, ticker_ts_utc DESC

-- ------------------------------------------------------------------------------
-- Conflict Handling (UPSERT)
-- ------------------------------------------------------------------------------
-- If a record already exists with same (product_id, ticker_ts_utc),
-- update it with the latest values from staging

ON CONFLICT (product_id, ticker_ts_utc)
DO UPDATE SET
    price = EXCLUDED.price,
    best_bid = EXCLUDED.best_bid,
    best_ask = EXCLUDED.best_ask,
    mid_price = EXCLUDED.mid_price,
    spread = EXCLUDED.spread,
    price_pct_chg_24h = EXCLUDED.price_pct_chg_24h,
    volume_24h = EXCLUDED.volume_24h,
    high_24h = EXCLUDED.high_24h,
    low_24h = EXCLUDED.low_24h;

-- ------------------------------------------------------------------------------
-- Cleanup Staging Table
-- ------------------------------------------------------------------------------
-- Removes processed data to prepare for next ingestion cycle
-- Assumes upstream system guarantees replay/recovery if needed

TRUNCATE {staging_table};