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
FROM {staging_table}
WHERE trade_time_utc IS NOT NULL
ON CONFLICT (product_id, trade_time_utc)
DO UPDATE SET
    total_volume = {final_table}.total_volume + EXCLUDED.total_volume,
    notional_value = {final_table}.notional_value + EXCLUDED.notional_value,
    trade_count = {final_table}.trade_count + EXCLUDED.trade_count,
    buy_volume = {final_table}.buy_volume + EXCLUDED.buy_volume,
    sell_volume = {final_table}.sell_volume + EXCLUDED.sell_volume;

TRUNCATE {staging_table};