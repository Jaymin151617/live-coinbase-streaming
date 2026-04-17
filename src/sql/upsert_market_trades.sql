-- ==============================================================================
-- Purpose: Incrementally ingest trade data, deduplicate, and aggregate per minute
-- ==============================================================================

-- This pipeline performs:
-- 1. Inserts new (deduplicated) trades into raw table
-- 2. Aggregates newly inserted trades into 1-minute buckets
-- 3. Upserts aggregated metrics into final table (incremental accumulation)
-- 4. Clears staging table after successful processing

-- ------------------------------------------------------------------------------
-- Step 1: Insert New Trades into Raw Table (Deduplication Layer)
-- ------------------------------------------------------------------------------
-- Only new trades (based on trade_id) are inserted
-- ON CONFLICT DO NOTHING ensures idempotency (safe reprocessing)

WITH inserted AS (
    INSERT INTO {raw_table} (
        trade_id,         -- Unique trade identifier (primary key)
        product_id,       -- Trading pair (e.g., BTC-USD)
        side,             -- Trade direction: BUY / SELL
        trade_value,      -- Price * size (notional value)
        size,             -- Quantity traded
        trade_time_utc    -- Trade timestamp (UTC)
    )
    SELECT
        trade_id,
        product_id,
        side,
        trade_value,
        size,
        trade_time_utc
    FROM {staging_table}

    -- Filter out invalid trades
    WHERE trade_id IS NOT NULL

    -- Deduplication based on primary key
    ON CONFLICT (trade_id) DO NOTHING

    -- Return only successfully inserted (new) rows for downstream aggregation
    RETURNING
        product_id,
        side,
        trade_value,
        size,
        trade_time_utc
),

-- ------------------------------------------------------------------------------
-- Step 2: Aggregate Trades into 1-Minute Buckets
-- ------------------------------------------------------------------------------
-- Only newly inserted trades are aggregated (incremental processing)

agg AS (
    SELECT
        product_id,

        -- Truncate timestamp to minute granularity
        date_trunc('minute', trade_time_utc) AS trade_time_utc,

        -- Total traded quantity
        SUM(size) AS total_volume,

        -- Total traded value (price * size)
        SUM(trade_value) AS notional_value,

        -- Number of trades in the interval
        COUNT(*) AS trade_count,

        -- Original offer was a sell, buyer matched it.
        SUM(CASE WHEN side = 'SELL' THEN size ELSE 0 END) AS buy_volume,

        -- Original offer was a buy, seller matched it.
        SUM(CASE WHEN side = 'BUY' THEN size ELSE 0 END) AS sell_volume

    FROM inserted

    -- Group by product and minute bucket
    GROUP BY product_id, date_trunc('minute', trade_time_utc)
)

-- ------------------------------------------------------------------------------
-- Step 3: Upsert Aggregated Metrics into Final Table
-- ------------------------------------------------------------------------------
-- This is an incremental aggregation:
-- Existing rows are UPDATED by adding new values (not overwritten)

INSERT INTO {final_table} (
    product_id,
    trade_time_utc,
    total_volume,
    notional_value,
    trade_count,
    buy_volume,
    sell_volume
)
SELECT
    product_id,
    trade_time_utc,
    total_volume,
    notional_value,
    trade_count,
    buy_volume,
    sell_volume
FROM agg

-- Conflict occurs if (product_id, trade_time_utc) already exists
ON CONFLICT (product_id, trade_time_utc)
DO UPDATE SET
    total_volume = {final_table}.total_volume + EXCLUDED.total_volume,
    notional_value = {final_table}.notional_value + EXCLUDED.notional_value,
    trade_count = {final_table}.trade_count + EXCLUDED.trade_count,
    buy_volume = {final_table}.buy_volume + EXCLUDED.buy_volume,
    sell_volume = {final_table}.sell_volume + EXCLUDED.sell_volume;

-- ------------------------------------------------------------------------------
-- Step 4: Cleanup Staging Table
-- ------------------------------------------------------------------------------
-- Removes processed records to prepare for next ingestion batch
-- Assumes upstream guarantees replay/recovery if needed

TRUNCATE {staging_table};