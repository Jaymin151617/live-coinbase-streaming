CREATE OR REPLACE FUNCTION coinbase.ensure_partition_hourly(
    p_parent_table text,
    p_partition_start timestamptz
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_partition_name text;
    v_full_name text;
    v_partition_end timestamptz;
BEGIN
    v_partition_name := format('%s_%s', p_parent_table, to_char(p_partition_start, 'YYYYMMDD_HH24'));
    v_full_name := format('coinbase.%I', v_partition_name);
    v_partition_end := p_partition_start + interval '1 hour';

    IF to_regclass(v_full_name) IS NULL THEN
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

CREATE OR REPLACE FUNCTION coinbase.ensure_coinbase_partitions()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_now timestamptz := now() AT TIME ZONE 'UTC';
    v_current_hour timestamptz;
    v_next_hour timestamptz;
    v_table text;
BEGIN
    v_current_hour := date_trunc('hour', v_now);
    v_next_hour := v_current_hour + interval '1 hour';

    FOREACH v_table IN ARRAY ARRAY[
        'market_trades_agg',
        'ticker_snapshot',
        'candles_snapshot'
    ]
    LOOP
        PERFORM coinbase.ensure_partition_hourly(v_table, v_current_hour);
        PERFORM coinbase.ensure_partition_hourly(v_table, v_next_hour);
    END LOOP;
END;
$$;