INSERT INTO {final_table} (
    product_id,
    open,
    high,
    low,
    close,
    range,
    avg_price,
    candle_ts_utc
)
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
WHERE candle_ts_utc IS NOT NULL
ORDER BY product_id, candle_ts_utc DESC
ON CONFLICT (product_id, candle_ts_utc)
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    range = EXCLUDED.range,
    avg_price = EXCLUDED.avg_price;

TRUNCATE {staging_table};