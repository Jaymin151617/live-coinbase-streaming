-- ==============================================================================
-- Function: coinbase.ensure_partition_hourly
-- Purpose : Create an hourly partition for a given parent table if it does not exist
-- ==============================================================================

-- Overview:
-- This function ensures that a partition exists for a specific hour.
-- If the partition is missing, it dynamically creates it.

-- Parameters:
-- p_parent_table     → Name of the partitioned parent table
-- p_partition_start  → Start timestamp (UTC) for the partition

CREATE OR REPLACE FUNCTION coinbase.ensure_partition_hourly(
    p_parent_table text,
    p_partition_start timestamptz
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    -- --------------------------------------------------------------------------
    -- Derived Partition Metadata
    -- --------------------------------------------------------------------------

    -- Partition name format:
    --   <table_name>_YYYYMMDD_HH24
    -- Example:
    --   candles_snapshot_20260416_10
    v_partition_name text;

    -- Fully qualified table name (schema + partition name)
    v_full_name text;

    -- End boundary of partition (1-hour window)
    v_partition_end timestamptz;

BEGIN
    -- --------------------------------------------------------------------------
    -- Build Partition Name and Boundaries
    -- --------------------------------------------------------------------------
    v_partition_name := format(
        '%s_%s',
        p_parent_table,
        to_char(p_partition_start, 'YYYYMMDD_HH24')
    );

    v_full_name := format('coinbase.%I', v_partition_name);

    -- Define partition range: [start, start + 1 hour)
    v_partition_end := p_partition_start + interval '1 hour';

    -- --------------------------------------------------------------------------
    -- Create Partition If It Does Not Exist
    -- --------------------------------------------------------------------------
    -- to_regclass returns NULL if relation does not exist
    IF to_regclass(v_full_name) IS NULL THEN

        -- Dynamic SQL required for partition creation
        EXECUTE format(
            'CREATE TABLE %s PARTITION OF coinbase.%I FOR VALUES FROM (%L) TO (%L)',
            v_full_name,
            p_parent_table,
            p_partition_start,
            v_partition_end
        );

    END IF;

END;
$$;


-- ==============================================================================
-- Function: coinbase.ensure_coinbase_partitions
-- Purpose : Ensure current and next hour partitions exist for all Coinbase tables
-- ==============================================================================

-- Overview:
-- This function proactively creates partitions for:
-- - Current hour
-- - Next hour
--
-- This prevents runtime failures during inserts when a partition is missing.

CREATE OR REPLACE FUNCTION coinbase.ensure_coinbase_partitions()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    -- --------------------------------------------------------------------------
    -- Time References (UTC)
    -- --------------------------------------------------------------------------

    -- Current UTC timestamp
    v_now timestamptz := now() AT TIME ZONE 'UTC';

    -- Start of current hour
    v_current_hour timestamptz;

    -- Start of next hour
    v_next_hour timestamptz;

    -- Iterator for parent tables
    v_table text;

BEGIN
    -- --------------------------------------------------------------------------
    -- Calculate Time Buckets
    -- --------------------------------------------------------------------------
    v_current_hour := date_trunc('hour', v_now);
    v_next_hour := v_current_hour + interval '1 hour';

    -- --------------------------------------------------------------------------
    -- Ensure Partitions for Each Target Table
    -- --------------------------------------------------------------------------
    -- Applies to all partitioned Coinbase tables
    FOREACH v_table IN ARRAY ARRAY[
        'market_trades_agg',
        'ticker_snapshot',
        'candles_snapshot'
    ]
    LOOP

        -- Ensure partition for current hour
        PERFORM coinbase.ensure_partition_hourly(
            v_table,
            v_current_hour
        );

        -- Ensure partition for next hour (pre-warming)
        PERFORM coinbase.ensure_partition_hourly(
            v_table,
            v_next_hour
        );

    END LOOP;

END;
$$;