-- Fetch the latest traded price (ticker) for a given product
SELECT
    price   -- Most recent traded price
FROM
    coinbase.ticker_snapshot   -- Table storing real-time ticker updates
WHERE
    product_id = {{product_id}}  -- Filter for selected trading pair (Metabase variable)
ORDER BY
    ticker_ts_utc DESC           -- Sort by timestamp (latest first)
LIMIT 1                          -- Return only the most recent price
;