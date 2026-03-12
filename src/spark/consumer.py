import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, from_json, explode, to_timestamp, expr, sum as _sum, avg as _avg, count as _count,
    window, when, coalesce
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

spark = SparkSession.builder.appName("spark-consumer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

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

first_event = col("events").getItem(0)

# --- Market trades: use kafka_key as fallback product_id ---
market_trades_exploded = (
    parsed
    .withColumn("event", first_event)
    .withColumn("trades_arr", first_event.getField("trades"))
    .where(col("trades_arr").isNotNull())
    .select("topic", "kafka_key", explode(col("trades_arr")).alias("trade"))
    .select(
        col("topic"),
        col("kafka_key"),
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
    .withColumn("product_id", coalesce(col("trade_product_id"), col("kafka_key")))  # use key if product_id missing
    .withColumn("price", col("price_str").cast("double"))
    .withColumn("size", col("size_str").cast("double"))
    .withColumn("trade_time", to_timestamp(col("trade_time_str")))
    .withColumn("trade_value", expr("price * size"))
    .drop("price_str", "size_str", "trade_time_str", "trade_product_id")
)

market_trades_agg = (
    market_trades
    .withWatermark("trade_time", "2 minutes")
    .groupBy(window(col("trade_time"), "1 minute"), col("product_id"))
    .agg(
        _sum("size").alias("total_volume"),
        (_sum(expr("price * size")) / _sum("size")).alias("vwap"),
        _count("*").alias("trade_count")
    )
    .selectExpr("window.start as window_start", "window.end as window_end", "product_id", "total_volume", "vwap", "trade_count")
)

# --- Ticker DF: again fallback to kafka_key ---
ticker_exploded = (
    parsed
    .withColumn("event", first_event)
    .withColumn("ticker_arr", first_event.getField("tickers"))
    .where(col("ticker_arr").isNotNull())
    .select("topic", "kafka_key", explode(col("ticker_arr")).alias("ticker"))
    .select(
        col("topic"),
        col("kafka_key"),
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

# --- Candles DF: fallback to kafka_key ---
candles_exploded = (
    parsed
    .withColumn("event", first_event)
    .withColumn("candles_arr", first_event.getField("candles"))
    .where(col("candles_arr").isNotNull())
    .select("topic", "kafka_key", explode(col("candles_arr")).alias("candle"))
    .select(
        col("topic"),
        col("kafka_key"),
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


market_trades_query = (
    market_trades_agg.writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 50)
    .outputMode("update")
    .option("maxOffsetsPerTrigger", 10000)
    .trigger(processingTime=PROCESSING_TIME)
    .start()
)

ticker_query = (
    ticker_df.writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 50)
    .outputMode("append")
    .option("maxOffsetsPerTrigger", 10000)
    .trigger(processingTime=PROCESSING_TIME)
    .start()
)

candles_query = (
    candles_df.writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 50)
    .outputMode("append")
    .option("maxOffsetsPerTrigger", 10000)
    .trigger(processingTime=PROCESSING_TIME)
    .start()
)

spark.streams.awaitAnyTermination()
