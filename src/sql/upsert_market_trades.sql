WITH inserted AS (
    INSERT INTO {raw_table} (
        trade_id,
        product_id,
        side,
        trade_value,
        size,
        trade_time_utc
    )
    SELECT
        trade_id,
        product_id,
        side,
        trade_value,
        size,
        trade_time_utc
    FROM {staging_table}
    WHERE trade_id IS NOT NULL
    ON CONFLICT (trade_id) DO NOTHING
    RETURNING
        product_id,
        side,
        trade_value,
        size,
        trade_time_utc
),
agg AS (
    SELECT
        product_id,
        date_trunc('minute', trade_time_utc) AS trade_time_utc,
        SUM(size) AS total_volume,
        SUM(trade_value) AS notional_value,
        COUNT(*) AS trade_count,
        SUM(CASE WHEN side = 'SELL' THEN size ELSE 0 END) AS buy_volume,
        SUM(CASE WHEN side = 'BUY' THEN size ELSE 0 END) AS sell_volume
    FROM inserted
    GROUP BY product_id, date_trunc('minute', trade_time_utc)
)
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
ON CONFLICT (product_id, trade_time_utc)
DO UPDATE SET
    total_volume = {final_table}.total_volume + EXCLUDED.total_volume,
    notional_value = {final_table}.notional_value + EXCLUDED.notional_value,
    trade_count = {final_table}.trade_count + EXCLUDED.trade_count,
    buy_volume = {final_table}.buy_volume + EXCLUDED.buy_volume,
    sell_volume = {final_table}.sell_volume + EXCLUDED.sell_volume;

TRUNCATE {staging_table};