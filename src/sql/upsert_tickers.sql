INSERT INTO {final_table} (
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
)
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
WHERE ticker_ts_utc IS NOT NULL
ORDER BY product_id, ticker_ts_utc DESC
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

TRUNCATE {staging_table};