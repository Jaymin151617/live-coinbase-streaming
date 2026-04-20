-- ==============================================================================
-- Function: coinbase.cleanup_raw_trades()
-- Purpose : Incrementally delete old raw trade records to control table size
-- ==============================================================================

-- Overview:
-- This function removes old records from `coinbase.raw_market_trades`
-- based on a time-based retention policy.

-- Key Design:
-- - Deletes data older than 10 minutes
-- - Uses batch deletion (LIMIT 5000) to avoid long locks
-- - Designed to be called repeatedly (e.g., via cron or scheduler)

CREATE OR REPLACE FUNCTION coinbase.cleanup_raw_trades()
RETURNS void AS $$
BEGIN

    -- --------------------------------------------------------------------------
    -- Step 1: Identify Records Eligible for Deletion
    -- --------------------------------------------------------------------------
    -- Select up to 5,000 trade_ids older than 10 minutes
    -- Ordered by oldest first to ensure deterministic cleanup progression

    WITH to_delete AS (
        SELECT trade_id
        FROM coinbase.raw_market_trades

        -- Retention condition: keep only last 10 minutes of data
        WHERE trade_time_utc < NOW() - INTERVAL '10 minutes'

        ORDER BY trade_time_utc

        -- Batch size limit to avoid large transactions and heavy locks
        LIMIT 5000
    )

    -- --------------------------------------------------------------------------
    -- Step 2: Delete Identified Records
    -- --------------------------------------------------------------------------
    -- Deletes only the selected subset (controlled batch deletion)

    DELETE FROM coinbase.raw_market_trades
    WHERE trade_id IN (SELECT trade_id FROM to_delete);

END;
$$ LANGUAGE plpgsql;