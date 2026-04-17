-- ==============================================================================
-- Function: coinbase.drop_old_coinbase_partitions()
-- Purpose : Drop outdated partitions for Coinbase data tables
-- ==============================================================================

-- Overview:
-- This function iterates over partitioned tables in the `coinbase` schema and:
-- 1. Identifies child partitions based on naming convention
-- 2. Extracts timestamp suffix from partition name
-- 3. Drops partitions older than a defined cutoff time

-- Retention Policy:
-- - Keeps only the most recent 1 hour of partitions
-- - Drops anything older than (current UTC hour - 1 hour)

CREATE OR REPLACE FUNCTION coinbase.drop_old_coinbase_partitions()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    -- --------------------------------------------------------------------------
    -- Cutoff Time Calculation
    -- --------------------------------------------------------------------------
    -- Current UTC time rounded to hour minus 1 hour
    -- Example:
    --   If now = 10:35 UTC → cutoff = 09:00 UTC
    v_cutoff timestamptz :=
    date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '1 hour';

    -- Parent table name (iterated over list)
    v_parent text;

    -- Record to hold partition metadata
    v_part record;

    -- Extracted suffix from partition name (e.g., 20260416_09)
    v_suffix text;

    -- Parsed timestamp from partition name
    v_partition_start timestamptz;

BEGIN
    -- --------------------------------------------------------------------------
    -- Iterate Over Target Partitioned Tables
    -- --------------------------------------------------------------------------
    -- These are parent tables with time-based partitions
    FOREACH v_parent IN ARRAY ARRAY[
        'market_trades_agg',
        'ticker_snapshot',
        'candles_snapshot'
    ]
    LOOP

        -- ----------------------------------------------------------------------
        -- Fetch All Child Partitions for Current Parent Table
        -- ----------------------------------------------------------------------
        FOR v_part IN
            SELECT c.relname AS partition_name
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_class p ON p.oid = i.inhparent
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'coinbase'
              AND p.relname = v_parent
        LOOP

            -- ------------------------------------------------------------------
            -- Extract Timestamp Suffix from Partition Name
            -- ------------------------------------------------------------------
            -- Expected naming format:
            --   <table_name>_YYYYMMDD_HH
            -- Example:
            --   candles_snapshot_20260416_09
            v_suffix := substring(v_part.partition_name from '_(\d{8}_\d{2})$');

            -- Only proceed if suffix matches expected format
            IF v_suffix IS NOT NULL THEN

                -- --------------------------------------------------------------
                -- Convert Suffix to Timestamp
                -- --------------------------------------------------------------
                v_partition_start := to_timestamp(v_suffix, 'YYYYMMDD_HH24');

                -- --------------------------------------------------------------
                -- Drop Partition if Older Than Cutoff
                -- --------------------------------------------------------------
                IF v_partition_start < v_cutoff THEN

                    -- Dynamic SQL required for dropping table by name
                    EXECUTE format(
                        'DROP TABLE IF EXISTS coinbase.%I',
                        v_part.partition_name
                    );

                END IF;
            END IF;

        END LOOP;
    END LOOP;

END;
$$;