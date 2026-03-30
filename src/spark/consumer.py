import os
import logging
from pathlib import Path

import signal
import threading
from py4j.protocol import Py4JError, Py4JJavaError, Py4JNetworkError

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, from_json, explode, to_timestamp, expr, round as _round, sum as _sum, count as _count,
    date_trunc, lit, row_number, when, coalesce
)

from psycopg2 import pool

# -------------------------------------------------------------------
# Paths / logging
# -------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[2]

# Logging
(ROOT_DIR / "logs").mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=ROOT_DIR / "logs" / "consumer.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("spark-consumer")

# -------------------------------------------------------------------
# config via env vars
# -------------------------------------------------------------------
BOOTSTRAP = os.environ.get("KAFKA_SERVER_URL")
TOPICS = "coinbase.ticker,coinbase.candles,coinbase.market_trades"

KEYSTORE = os.environ.get("KEYSTORE")
KEYSTORE_PASS = os.environ.get("KEYSTORE_PASS")
KEYSTORE_TYPE = os.environ.get("KEYSTORE_TYPE")
TRUSTSTORE = os.environ.get("TRUSTSTORE")
TRUSTSTORE_PASS = os.environ.get("TRUSTSTORE_PASS")

PROCESSING_TIME = "30 seconds"

required_env = {
    "KAFKA_SERVER_URL": BOOTSTRAP,
    "KEYSTORE": KEYSTORE,
    "KEYSTORE_PASS": KEYSTORE_PASS,
    "KEYSTORE_TYPE": KEYSTORE_TYPE,
    "TRUSTSTORE": TRUSTSTORE,
    "TRUSTSTORE_PASS": TRUSTSTORE_PASS,
}

missing_env = [name for name, value in required_env.items() if not value]
if missing_env:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_env)}")

try:
    spark = SparkSession.builder.appName("spark-consumer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session started")
except Exception:
    logger.exception("Failed to initialize SparkSession")
    raise


shutdown_requested = threading.Event()

def _request_shutdown(signum, frame):
    shutdown_requested.set()

def parse_message_timestamp(ts_col):
    return coalesce(
        to_timestamp(ts_col, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX"),
        to_timestamp(ts_col, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"),
        to_timestamp(ts_col, "yyyy-MM-dd'T'HH:mm:ssX"),
    )

PRODUCTS = ["BTC-USD", "ETH-USD", "ADA-USD", "LINK-USD", "SOL-USD"]
products_df = spark.createDataFrame(
    [(p,) for p in PRODUCTS],
    StructType([StructField("product_id", StringType(), True)])
)

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
try:
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
    logger.info("Kafka stream configured for topics: %s", TOPICS)
except Exception:
    logger.exception("Failed to configure Kafka read stream")
    raise

parsed = raw.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("key").cast("string").alias("kafka_key"),        # <-- Kafka key as string (e.g. "BTC-USD")
    from_json(col("value").cast("string"), topSchema).alias("j")
).select("topic", "partition", "offset", "kafka_key", "j.*")

parsed = parsed.withColumn(
    "message_ts_utc",
    parse_message_timestamp(col("timestamp"))
)

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
    .select("topic", "kafka_key", "offset", "message_ts_utc", explode(col("ticker_arr")).alias("ticker"))
    .select(
        col("topic"),
        col("kafka_key"),
        col("offset").alias("kafka_offset"),
        col("message_ts_utc"),
        col("ticker.product_id").alias("ticker_product_id"),
        col("ticker.price").alias("price_str"),
        col("ticker.best_bid").alias("best_bid_str"),
        col("ticker.best_ask").alias("best_ask_str"),
        col("ticker.price_percent_chg_24_h").alias("chg24_str"),
        col("ticker.volume_24_h").alias("volume_24h_str"),
        col("ticker.high_24_h").alias("high_24_h_str"),
        col("ticker.low_24_h").alias("low_24_h_str"),
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
    .withColumn("high_24h", col("high_24_h_str").cast("double"))
    .withColumn("low_24h", col("low_24_h_str").cast("double"))
    .withColumn("mid_price", expr("(best_bid + best_ask) / 2"))
    .withColumn("spread", expr("best_ask - best_bid"))
    .drop("price_str", "best_bid_str", "best_ask_str", "chg24_str", "volume_24h_str", "high_24_h_str", "low_24_h_str", "ticker_product_id")
)

# For candles: carry offset
candles_exploded = (
    filtered
    .withColumn("candles_arr", col("event.candles"))
    .where(col("candles_arr").isNotNull())
    .select("topic", "kafka_key", "offset", "message_ts_utc", explode(col("candles_arr")).alias("candle"))
    .select(
        col("topic"),
        col("kafka_key"),
        col("offset").alias("kafka_offset"),
        col("message_ts_utc"),
        col("candle.product_id").alias("candle_product_id"),
        col("candle.open").alias("open_str"),
        col("candle.high").alias("high_str"),
        col("candle.low").alias("low_str"),
        col("candle.close").alias("close_str"),
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
    .withColumn("volume", col("volume_str").cast("double"))
    .withColumn("range", expr("high - low"))
    .withColumn("avg_price", expr("(open + high + low + close) / 4"))
    .drop("open_str", "high_str", "low_str", "close_str", "start_str", "volume_str", "candle_product_id")
)

# -------------------------------------------------------------------
# Helper write functions per micro-batch
# -------------------------------------------------------------------
def write_market_trades(batch_df, batch_id):
    try:
        if batch_df.rdd.isEmpty():
            logger.warning("[market_trades] batch %s empty", batch_id)
            return

        agg = (
            batch_df
            .withColumn("minute_bucket", date_trunc("minute", col("trade_time")))
            .groupBy("product_id", "minute_bucket")
            .agg(
                _sum("size").alias("total_volume"),
                (when(_sum("size") == 0, None).otherwise(_sum(expr("price * size")) / _sum("size"))).alias("vwap"),
                _count("*").alias("trade_count"),
                _sum(when(col("side") == "BUY", col("size"))).alias("sell_volume"),
                _sum(when(col("side") == "SELL", col("size"))).alias("buy_volume")
            )
        )

        out = (
            agg
            .withColumn("trade_count", coalesce(col("trade_count"), lit(0)))
            .withColumn("total_volume", _round(coalesce(col("total_volume"), lit(0.0)), 8))
            .withColumn("buy_volume", _round(coalesce(col("buy_volume"), lit(0.0)), 8))
            .withColumn("sell_volume", _round(coalesce(col("sell_volume"), lit(0.0)), 8))
            .withColumn("vwap", _round(col("vwap"), 4))
            .withColumn("trade_time_utc", col("minute_bucket"))
            .drop("minute_bucket")
        )

        logger.debug("[market_trades] batch %s results:", batch_id)
        out.show(truncate=False)

    except Exception:
        logger.exception("[market_trades] failed for batch %s", batch_id)


def write_ticker(batch_df, batch_id):
    try:
        if batch_df.rdd.isEmpty():
            logger.warning("[ticker] batch %s empty", batch_id)
            return

        w = Window.partitionBy("product_id", "minute_bucket").orderBy(col("kafka_offset").desc())
        latest = (
            batch_df
            .withColumn("minute_bucket", date_trunc("minute", col("message_ts_utc")))
            .withColumn("rn", row_number().over(w))
            .where(col("rn") == 1)
            .drop("rn")
        )

        out = (
            latest.select(
                "product_id",
                "price",
                "best_bid",
                "best_ask",
                "mid_price",
                "spread",
                "price_pct_chg_24h",
                "volume_24h",
                "high_24h",
                "low_24h",
                "minute_bucket"
            )
            .withColumn("price", _round(coalesce(col("price"), lit(0.0)), 4))
            .withColumn("best_bid", _round(coalesce(col("best_bid"), lit(0.0)), 4))
            .withColumn("best_ask", _round(coalesce(col("best_ask"), lit(0.0)), 4))
            .withColumn("mid_price", _round(coalesce(col("mid_price"), lit(0.0)), 4))
            .withColumn("spread", _round(coalesce(col("spread"), lit(0.0)), 4))
            .withColumn("price_pct_chg_24h", _round(coalesce(col("price_pct_chg_24h"), lit(0.0)), 4))
            .withColumn("volume_24h", _round(coalesce(col("volume_24h"), lit(0.0)), 8))
            .withColumn("high_24h", _round(coalesce(col("high_24h"), lit(0.0)), 4))
            .withColumn("low_24h", _round(coalesce(col("low_24h"), lit(0.0)), 4))
            .withColumn("ticker_ts_utc", col("minute_bucket"))
            .drop("minute_bucket")
        )

        logger.debug("[ticker] batch %s latest per product:", batch_id)
        # out.show(truncate=False)

        # Step 1: write to staging
        jdbc_write(out, "coinbase.stg_ticker_snapshot")

        # Step 2: merge into final table
        upsert_query = """
        INSERT INTO coinbase.ticker_snapshot (
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
        FROM coinbase.stg_ticker_snapshot
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
        """
        run_pg_query(upsert_query)

        # Step 3
        run_pg_query("TRUNCATE coinbase.stg_ticker_snapshot;")

    except Exception:
        logger.exception("[ticker] failed for batch %s", batch_id)


def write_candles(batch_df, batch_id):
    try:
        if batch_df.rdd.isEmpty():
            logger.warning("[candles] batch %s empty", batch_id)
            return

        w = Window.partitionBy("product_id", "minute_bucket").orderBy(col("kafka_offset").desc())
        latest = (
            batch_df
            .withColumn("minute_bucket", date_trunc("minute", col("message_ts_utc")))
            .withColumn("rn", row_number().over(w))
            .where(col("rn") == 1)
            .drop("rn")
        )

        out = (
            latest.select(
                "product_id",
                "open",
                "high",
                "low",
                "close",
                "range",
                "avg_price",
                "minute_bucket"
            )
            .withColumn("open", _round(coalesce(col("open"), lit(0.0)), 4))
            .withColumn("high", _round(coalesce(col("high"), lit(0.0)), 4))
            .withColumn("low", _round(coalesce(col("low"), lit(0.0)), 4))
            .withColumn("close", _round(coalesce(col("close"), lit(0.0)), 4))
            .withColumn("range", _round(coalesce(col("range"), lit(0.0)), 4))
            .withColumn("avg_price", _round(coalesce(col("avg_price"), lit(0.0)), 4))
            .withColumn("candle_ts_utc", col("minute_bucket"))
            .drop("minute_bucket")
        )

        logger.debug("[candles] batch %s latest per product:", batch_id)
        # out.show(truncate=False)

        # Step 1: write to staging
        jdbc_write(out, "coinbase.stg_candles_snapshot")

        # Step 2: merge into final table
        upsert_query = """
        INSERT INTO coinbase.candles_snapshot (
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
        FROM coinbase.stg_candles_snapshot
        WHERE candle_ts_utc IS NOT NULL
        ORDER BY product_id, candle_ts_utc DESC
        ON CONFLICT (product_id, candle_ts_utc)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            range = EXCLUDED.range,
            avg_price = EXCLUDED.avg_price
        """
        run_pg_query(upsert_query)

        # Step 3
        run_pg_query("TRUNCATE coinbase.stg_candles_snapshot;")

    except Exception:
        logger.exception("[candles] failed for batch %s", batch_id)

# -------------------------------------------------------------------
# Attach the foreachBatch writers and triggers
# -------------------------------------------------------------------
try:
    market_trades_query = (
        market_trades.writeStream
        .trigger(processingTime=PROCESSING_TIME)
        .foreachBatch(write_market_trades)
        .start()
    )
    logger.info("Started market_trades stream")

    ticker_query = (
        ticker_df.writeStream
        .trigger(processingTime=PROCESSING_TIME)
        .foreachBatch(write_ticker)
        .start()
    )
    logger.info("Started ticker stream")

    candles_query = (
        candles_df.writeStream
        .trigger(processingTime=PROCESSING_TIME)
        .foreachBatch(write_candles)
        .start()
    )
    logger.info("Started candles stream")

except Exception:
    logger.exception("Failed to start one or more streaming queries")
    raise


signal.signal(signal.SIGINT, _request_shutdown)
signal.signal(signal.SIGTERM, _request_shutdown)

queries = [market_trades_query, ticker_query, candles_query]

try:
    logger.info("Streaming started")
    while not shutdown_requested.is_set():
        try:
            # wake up every second so Ctrl+C is noticed cleanly
            spark.streams.awaitAnyTermination(1)
        except Py4JError:
            # ignore JVM-side noise if we're already shutting down
            if shutdown_requested.is_set():
                break
            raise

finally:
    logger.info("Stopping streaming queries")
    for q in queries:
        try:
            if q.isActive:
                q.stop()
        except (Py4JError, Py4JJavaError, Py4JNetworkError, ConnectionRefusedError):
            logger.info("Query already stopped")
        except Exception:
            logger.exception("Failed to stop query cleanly")

    try:
        spark.stop()
    except (Py4JError, Py4JJavaError, Py4JNetworkError, ConnectionRefusedError):
        logger.info("Spark already stopped (expected during shutdown)")
    except Exception:
        logger.exception("Failed to stop Spark cleanly")
