CREATE OR REPLACE FUNCTION coinbase.drop_old_coinbase_partitions()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_cutoff timestamptz :=
    date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '1 day';
    v_parent text;
    v_part record;
    v_suffix text;
    v_partition_start timestamptz;
BEGIN
    FOREACH v_parent IN ARRAY ARRAY[
        'market_trades_agg',
        'ticker_snapshot',
        'candles_snapshot'
    ]
    LOOP
        FOR v_part IN
            SELECT c.relname AS partition_name
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_class p ON p.oid = i.inhparent
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'coinbase'
              AND p.relname = v_parent
        LOOP
            v_suffix := substring(v_part.partition_name from '_(\d{8}_\d{2})$');

            IF v_suffix IS NOT NULL THEN
                v_partition_start := to_timestamp(v_suffix, 'YYYYMMDD_HH24');

                IF v_partition_start < v_cutoff THEN
                    EXECUTE format('DROP TABLE IF EXISTS coinbase.%I', v_part.partition_name);
                END IF;
            END IF;
        END LOOP;
    END LOOP;
END;
$$;