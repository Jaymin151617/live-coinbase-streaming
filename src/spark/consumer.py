import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, from_json, explode, to_timestamp, expr, round as _round, sum as _sum, count as _count,
    max as _max, current_timestamp, date_format, date_trunc, lit, row_number, when, coalesce
)

# config via env vars
BOOTSTRAP = os.environ.get("KAFKA_SERVER_URL")
TOPICS = "coinbase.ticker,coinbase.candles,coinbase.market_trades"

KEYSTORE = os.environ.get("KEYSTORE")
KEYSTORE_PASS = os.environ.get("KEYSTORE_PASS")
KEYSTORE_TYPE = os.environ.get("KEYSTORE_TYPE")
TRUSTSTORE = os.environ.get("TRUSTSTORE")
TRUSTSTORE_PASS = os.environ.get("TRUSTSTORE_PASS")

PROCESSING_TIME = "30 seconds"
TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

spark = SparkSession.builder.appName("spark-consumer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

PRODUCTS = ["BTC-USD", "ETH-USD", "ADA-USD", "LINK-USD", "SOL-USD"]
products_df = spark.createDataFrame([(p,) for p in PRODUCTS], StructType([StructField("product_id", StringType(), True)]))

# --- Schemas (same as before) ---
tradeSchema = StructType([
    StructField("price", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("side", StringType(), True),
    StructField("size", StringType(), True),
    StructField("time", StringType(), True),
    StructField("trade_id", StringType(), True),
])

tickerItemSchema = StructType([
    StructField("best_ask", StringType(), True),
    StructField("best_ask_quantity", StringType(), True),
    StructField("best_bid", StringType(), True),
    StructField("best_bid_quantity", StringType(), True),
    StructField("high_24_h", StringType(), True),
    StructField("high_52_w", StringType(), True),
    StructField("low_24_h", StringType(), True),
    StructField("low_52_w", StringType(), True),
    StructField("price", StringType(), True),
    StructField("price_percent_chg_24_h", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("volume_24_h", StringType(), True),
])

candleItemSchema = StructType([
    StructField("close", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("open", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("start", StringType(), True),
    StructField("volume", StringType(), True),
])

eventsSchema = StructType([
    StructField("trades", ArrayType(tradeSchema), True),
    StructField("tickers", ArrayType(tickerItemSchema), True),
    StructField("candles", ArrayType(candleItemSchema), True),
    StructField("type", StringType(), True),
])

topSchema = StructType([
    StructField("channel", StringType(), True),
    StructField("events", ArrayType(eventsSchema), True),
    StructField("sequence_num", LongType(), True),
    StructField("timestamp", StringType(), True),
])

# --- Read from Kafka and parse JSON, also capture the Kafka key (product like "BTC-USD") ---
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPICS)
    .option("startingOffsets", "latest")
    .option("kafka.security.protocol", "SSL")
    .option("kafka.ssl.truststore.location", TRUSTSTORE)
    .option("kafka.ssl.truststore.password", TRUSTSTORE_PASS)
    .option("kafka.ssl.keystore.location", KEYSTORE)
    .option("kafka.ssl.keystore.password", KEYSTORE_PASS)
    .option("kafka.ssl.keystore.type", KEYSTORE_TYPE)
    .option("kafka.ssl.key.password", KEYSTORE_PASS)
    .load()
)

parsed = raw.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("key").cast("string").alias("kafka_key"),        # <-- Kafka key as string (e.g. "BTC-USD")
    from_json(col("value").cast("string"), topSchema).alias("j")
).select("topic", "partition", "offset", "kafka_key", "j.*")

filtered = (
    parsed
    .withColumn("event", col("events").getItem(0))
    .where(col("event").isNotNull())
    .where(col("event.type") == "update")       # Ignore snapshots
)

# === Ensure offset is carried through when exploding ===
# For market trades:
market_trades_exploded = (
    filtered
    .withColumn("trades_arr", col("event.trades"))
    .where(col("trades_arr").isNotNull())
    .select("topic", "kafka_key", "offset", explode(col("trades_arr")).alias("trade"))
    .select(
        col("topic"),
        col("kafka_key"),
        col("offset").alias("kafka_offset"),
        col("trade.price").alias("price_str"),
        col("trade.product_id").alias("trade_product_id"),
        col("trade.side").alias("side"),
        col("trade.size").alias("size_str"),
        col("trade.time").alias("trade_time_str"),
        col("trade.trade_id").alias("trade_id")
    )
)

market_trades = (
    market_trades_exploded
    .withColumn("product_id", coalesce(col("trade_product_id"), col("kafka_key")))
    .withColumn("price", col("price_str").cast("double"))
    .withColumn("size", col("size_str").cast("double"))
    .withColumn("trade_time", to_timestamp(col("trade_time_str")))
    .withColumn("trade_value", expr("price * size"))
    .drop("price_str", "size_str", "trade_time_str", "trade_product_id")
)

# For tickers: carry offset
ticker_exploded = (
    filtered
    .withColumn("ticker_arr", col("event.tickers"))
    .where(col("ticker_arr").isNotNull())
    .select("topic", "kafka_key", "offset", explode(col("ticker_arr")).alias("ticker"))
    .select(
        col("topic"),
        col("kafka_key"),
        col("offset").alias("kafka_offset"),
        col("ticker.product_id").alias("ticker_product_id"),
        col("ticker.price").alias("price_str"),
        col("ticker.best_bid").alias("best_bid_str"),
        col("ticker.best_ask").alias("best_ask_str"),
        col("ticker.price_percent_chg_24_h").alias("chg24_str"),
        col("ticker.volume_24_h").alias("volume_24h_str")
    )
)

ticker_df = (
    ticker_exploded
    .withColumn("product_id", coalesce(col("ticker_product_id"), col("kafka_key")))
    .withColumn("price", col("price_str").cast("double"))
    .withColumn("best_bid", col("best_bid_str").cast("double"))
    .withColumn("best_ask", col("best_ask_str").cast("double"))
    .withColumn("price_pct_chg_24h", col("chg24_str").cast("double"))
    .withColumn("volume_24h", col("volume_24h_str").cast("double"))
    .withColumn("mid_price", expr("(best_bid + best_ask) / 2"))
    .withColumn("spread", expr("best_ask - best_bid"))
    .drop("price_str", "best_bid_str", "best_ask_str", "chg24_str", "volume_24h_str", "ticker_product_id")
)

# For candles: carry offset
candles_exploded = (
    filtered
    .withColumn("candles_arr", col("event.candles"))
    .where(col("candles_arr").isNotNull())
    .select("topic", "kafka_key", "offset", explode(col("candles_arr")).alias("candle"))
    .select(
        col("topic"),
        col("kafka_key"),
        col("offset").alias("kafka_offset"),
        col("candle.product_id").alias("candle_product_id"),
        col("candle.open").alias("open_str"),
        col("candle.high").alias("high_str"),
        col("candle.low").alias("low_str"),
        col("candle.close").alias("close_str"),
        col("candle.start").alias("start_str"),
        col("candle.volume").alias("volume_str"),
    )
)

candles_df = (
    candles_exploded
    .withColumn("product_id", coalesce(col("candle_product_id"), col("kafka_key")))
    .withColumn("open", col("open_str").cast("double"))
    .withColumn("high", col("high_str").cast("double"))
    .withColumn("low", col("low_str").cast("double"))
    .withColumn("close", col("close_str").cast("double"))
    .withColumn("start_unix", col("start_str").cast("long"))
    .withColumn("start_ts", when(col("start_unix").isNotNull(), to_timestamp(col("start_unix"))).otherwise(None))
    .withColumn("volume", col("volume_str").cast("double"))
    .withColumn("range", expr("high - low"))
    .withColumn("avg_price", expr("(open + high + low + close) / 4"))
    .drop("open_str", "high_str", "low_str", "close_str", "start_str", "volume_str", "candle_product_id")
)

# === Helper write functions per micro-batch ===

def write_market_trades(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[market_trades] batch {batch_id} empty")
        return

    # aggregation
    agg = (
        batch_df
        .groupBy("product_id")
        .agg(
            _sum("size").alias("total_volume"),
            (_sum(expr("price * size")) / _sum("size")).alias("vwap"),
            _count("*").alias("trade_count"),
            _sum(when(col("side") == "BUY", col("size"))).alias("sell_volume"), # Original offer was a buy, seller matched it. Price goes down.
            _sum(when(col("side") == "SELL", col("size"))).alias("buy_volume"),  # Original offer was a sell, buyer matched it. Price goes up.
            _max("trade_time").alias("latest_trade_ts")
        )
    )

    # ensure 1 row per product
    out = (
        products_df
        .join(agg, on="product_id", how="left")
        .withColumn("trade_count", coalesce(col("trade_count"), lit(0)))
        .withColumn("total_volume", _round(coalesce(col("total_volume"), lit(0.0)), 8))
        .withColumn("buy_volume", _round(coalesce(col("buy_volume"), lit(0.0)), 8))
        .withColumn("sell_volume", _round(coalesce(col("sell_volume"), lit(0.0)), 8))
        .withColumn("vwap", _round(when(col("total_volume") == 0, None).otherwise(col("vwap")), 4))
        .withColumn(
            "latest_trade_ts",
            date_format(date_trunc("second", col("latest_trade_ts")), TIME_FORMAT)
        )
        .withColumn(
            "processed_at",
            date_format(date_trunc("second", current_timestamp()), TIME_FORMAT)
        )
    )

    print(f"[market_trades] batch {batch_id} results:")
    out.show(truncate=False)


def write_ticker(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[ticker] batch {batch_id} empty")
        return

    # pick the latest record per product by kafka_offset within this batch
    w = Window.partitionBy("product_id").orderBy(col("kafka_offset").desc())
    latest = batch_df.withColumn("rn", row_number().over(w)).where(col("rn") == 1).drop("rn")

    # left join to full product list so missing products still appear
    out = (
        products_df
        .join(
            latest.select(
                "product_id",
                "price",
                "best_bid",
                "best_ask",
                "mid_price",
                "spread",
                "price_pct_chg_24h",
                "volume_24h"
            ),
            on="product_id",
            how="left"
        )
        .withColumn("price", _round(coalesce(col("price"), lit(0.0)), 4))
        .withColumn("best_bid", _round(coalesce(col("best_bid"), lit(0.0)), 4))
        .withColumn("best_ask", _round(coalesce(col("best_ask"), lit(0.0)), 4))
        .withColumn("mid_price", _round(coalesce(col("mid_price"), lit(0.0)), 4))
        .withColumn("spread", _round(coalesce(col("spread"), lit(0.0)), 4))
        .withColumn("price_pct_chg_24h", _round(coalesce(col("price_pct_chg_24h"), lit(0.0)), 4))
        .withColumn("volume_24h", _round(coalesce(col("volume_24h"), lit(0.0)), 8))
        .withColumn(
            "processed_at",
            date_format(date_trunc("second", current_timestamp()), TIME_FORMAT)
        )
    )

    print(f"[ticker] batch {batch_id} latest per product:")
    out.show(truncate=False)


def write_candles(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[candles] batch {batch_id} empty")
        return

    w = Window.partitionBy("product_id").orderBy(col("kafka_offset").desc())
    latest = batch_df.withColumn("rn", row_number().over(w)).where(col("rn") == 1).drop("rn")

    out = (
        products_df
        .join(
            latest.select(
                "product_id",
                "open",
                "high",
                "low",
                "close",
                "start_ts",
                "range",
                "avg_price"
            ),
            on="product_id",
            how="left"
        )
        .withColumn("open", _round(coalesce(col("open"), lit(0.0)), 4))
        .withColumn("high", _round(coalesce(col("high"), lit(0.0)), 4))
        .withColumn("low", _round(coalesce(col("low"), lit(0.0)), 4))
        .withColumn("close", _round(coalesce(col("close"), lit(0.0)), 4))
        .withColumn("range", _round(coalesce(col("range"), lit(0.0)), 4))
        .withColumn("avg_price", _round(coalesce(col("avg_price"), lit(0.0)), 4))
        .withColumn(
            "start_ts",
            date_format(date_trunc("second", col("start_ts")), TIME_FORMAT)
        )
        .withColumn(
            "processed_at",
            date_format(date_trunc("second", current_timestamp()), TIME_FORMAT)
        )
    )

    print(f"[candles] batch {batch_id} latest per product:")
    out.show(truncate=False)

# === Attach the foreachBatch writers and triggers ===

market_trades_query = (
    market_trades.writeStream
    .trigger(processingTime=PROCESSING_TIME)
    .foreachBatch(write_market_trades)
    .start()
)

ticker_query = (
    ticker_df.writeStream
    .trigger(processingTime=PROCESSING_TIME)
    .foreachBatch(write_ticker)
    .start()
)

candles_query = (
    candles_df.writeStream
    .trigger(processingTime=PROCESSING_TIME)
    .foreachBatch(write_candles)
    .start()
)

spark.streams.awaitAnyTermination()
