import logging
import os
import sys
import json
import signal
import threading
from pathlib import Path
from functools import lru_cache

from py4j.protocol import Py4JError, Py4JJavaError, Py4JNetworkError
from psycopg2 import pool
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    coalesce,
    count as _count,
    date_trunc,
    explode,
    expr,
    from_json,
    lit,
    row_number,
    round as _round,
    sum as _sum,
    to_timestamp,
    when,
    current_timestamp,
)
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# -------------------------------------------------------------------
# Paths / logging
# -------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT_DIR / "src" / "config.json"
SQL_PATH = ROOT_DIR / "src" / "sql"

# Logging
(ROOT_DIR / "logs").mkdir(parents=True, exist_ok=True)
(ROOT_DIR / "checkpoints" / "coinbase_consumer").mkdir(parents=True, exist_ok=True)

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
SPARK_THREADS = os.environ.get("SPARK_THREADS", "2")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "1g")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "1g")
SPARK_CHECKPOINTS_TO_RETAIN = os.environ.get("SPARK_CHECKPOINTS_TO_RETAIN", "10")

TOPICS = "coinbase.ticker,coinbase.candles,coinbase.market_trades"

KEYSTORE = os.environ.get("KEYSTORE")
KEYSTORE_PASS = os.environ.get("KEYSTORE_PASS")
KEYSTORE_TYPE = os.environ.get("KEYSTORE_TYPE")
TRUSTSTORE = os.environ.get("TRUSTSTORE")
TRUSTSTORE_PASS = os.environ.get("TRUSTSTORE_PASS")

PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT")
PG_DATABASE = os.environ.get("PG_DATABASE")
PG_USERNAME = os.environ.get("PG_USERNAME")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

PROCESSING_TIME = os.environ.get("PROCESSING_TIME", "30 seconds")
CHECKPOINT_DIR = str(ROOT_DIR / "checkpoints" / "coinbase_consumer")
JDBC_WRITE_PARTITIONS = int(os.environ.get("JDBC_WRITE_PARTITIONS", "4"))
JDBC_BATCHSIZE = int(os.environ.get("JDBC_BATCHSIZE", "1000"))

required_env = {
    "KAFKA_SERVER_URL": BOOTSTRAP,
    "KEYSTORE": KEYSTORE,
    "KEYSTORE_PASS": KEYSTORE_PASS,
    "KEYSTORE_TYPE": KEYSTORE_TYPE,
    "TRUSTSTORE": TRUSTSTORE,
    "TRUSTSTORE_PASS": TRUSTSTORE_PASS,
    "PG_HOST": PG_HOST,
    "PG_PORT": PG_PORT,
    "PG_DATABASE": PG_DATABASE,
    "PG_USERNAME": PG_USERNAME,
    "PG_PASSWORD": PG_PASSWORD,
}
missing_env = [name for name, value in required_env.items() if not value]
if missing_env:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_env)}")

# --- Configuration ---
try:
    with open(CONFIG_PATH, "r") as cfg:
        CONFIG = json.load(cfg)

    TOPICS = ",".join(CONFIG["channels"].values())

    SCHEMA = CONFIG["schema"]

    TABLES = {
        key: {
            "staging": f"{SCHEMA}.{value['staging']}",
            "final": f"{SCHEMA}.{value['final']}"
        }
        for key, value in CONFIG["tables"].items()
    }

    TICKER_STAGING_TABLE = TABLES['ticker']['staging']
    TICKER_FINAL_TABLE   = TABLES['ticker']['final']

    CANDLES_STAGING_TABLE = TABLES['candles']['staging']
    CANDLES_FINAL_TABLE   = TABLES['candles']['final']

    MARKET_TRADES_STAGING_TABLE = TABLES['market_trades']['staging']
    MARKET_TRADES_FINAL_TABLE   = TABLES['market_trades']['final']

except FileNotFoundError:
    logger.exception("Config file not found.")
    sys.exit(1)

except json.JSONDecodeError:
    logger.exception("Invalid JSON in config file.")
    sys.exit(1)

except KeyError:
    logger.exception("Config file in wrong format.")
    sys.exit(1)


pg_pool = pool.SimpleConnectionPool(
    1,
    5,
    host=PG_HOST,
    port=PG_PORT,
    database=PG_DATABASE,
    user=PG_USERNAME,
    password=PG_PASSWORD,
    sslmode="require",
)

try:
    spark = (
        SparkSession.builder.appName("spark-consumer")
        .master(f"local[{SPARK_THREADS}]")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.minBatchesToRetain", SPARK_CHECKPOINTS_TO_RETAIN)
        .getOrCreate()
    )
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

PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}?sslmode=require"
PG_PROPS = {
    "user": PG_USERNAME,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
    "batchsize": str(JDBC_BATCHSIZE),
}

def jdbc_write(df, table_name):
    write_partitions = min(max(df.rdd.getNumPartitions(), 1), JDBC_WRITE_PARTITIONS)
    staged = df.coalesce(write_partitions)
    (
        staged.write.format("jdbc")
        .option("url", PG_JDBC_URL)
        .option("dbtable", table_name)
        .options(**PG_PROPS)
        .mode("append")
        .save()
    )

# Cache SQL files to avoid repeated disk reads
@lru_cache(maxsize=10)
def load_sql(file_path: str) -> str:
    path = Path(file_path)

    if not path.exists():
        logger.exception(f"SQL file not found: {file_path}")
        sys.exit(1)

    try:
        with open(path, "r") as f:
            return f.read()
    except Exception:
        logger.exception(f"Failed to read SQL file: {file_path}")
        sys.exit(1)

def run_pg_query(file_path: str = None, **kwargs):

    query_template = load_sql(file_path)
    try:
        query = query_template.format(**kwargs)
    except KeyError as e:
        raise ValueError(f"Missing SQL placeholder: {e}")

    conn = pg_pool.getconn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(query)
    finally:
        pg_pool.putconn(conn)

def truncate_staging_tables():
    try:
        logger.info("Truncating staging tables at startup")

        run_pg_query(
            file_path=SQL_PATH / "truncate_tables.sql",
            tables=", ".join([
                MARKET_TRADES_STAGING_TABLE,
                TICKER_STAGING_TABLE,
                CANDLES_STAGING_TABLE
            ])
        )

        logger.info("Staging tables truncated successfully")

    except Exception:
        logger.exception("Failed to truncate staging tables")
        raise


# --- Schemas ---
tradeSchema = StructType(
    [
        StructField("price", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("side", StringType(), True),
        StructField("size", StringType(), True),
        StructField("time", StringType(), True),
        StructField("trade_id", StringType(), True),
    ]
)

tickerItemSchema = StructType(
    [
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
    ]
)

candleItemSchema = StructType(
    [
        StructField("close", StringType(), True),
        StructField("high", StringType(), True),
        StructField("low", StringType(), True),
        StructField("open", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("start", StringType(), True),
        StructField("volume", StringType(), True),
    ]
)

eventsSchema = StructType(
    [
        StructField("trades", ArrayType(tradeSchema), True),
        StructField("tickers", ArrayType(tickerItemSchema), True),
        StructField("candles", ArrayType(candleItemSchema), True),
        StructField("type", StringType(), True),
    ]
)

topSchema = StructType(
    [
        StructField("channel", StringType(), True),
        StructField("events", ArrayType(eventsSchema), True),
        StructField("sequence_num", LongType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

# -------------------------------------------------------------------
# Read from Kafka and parse JSON once
# -------------------------------------------------------------------
try:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", False)
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

parsed = (
    raw.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("key").cast("string").alias("kafka_key"),
        from_json(col("value").cast("string"), topSchema).alias("j"),
    )
    .select("topic", "partition", "offset", "kafka_key", "j.*")
    .withColumn("message_ts_utc", parse_message_timestamp(col("timestamp")))
)

filtered = (
    parsed.withColumn("event", col("events").getItem(0))
    .where(col("event").isNotNull())
    .where(col("event.type") == "update")       # Ignore snapshots
)

# -------------------------------------------------------------------
# Batch preparation helpers
# -------------------------------------------------------------------
def prepare_market_trades(batch_df):
    return (
        batch_df.withColumn("trades_arr", col("event.trades"))
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
            col("trade.trade_id").alias("trade_id"),
        )
        .dropDuplicates(["trade_id"])
        .withColumn("product_id", coalesce(col("trade_product_id"), col("kafka_key")))
        .withColumn("price", col("price_str").cast("double"))
        .withColumn("size", col("size_str").cast("double"))
        .withColumn("trade_time", to_timestamp(col("trade_time_str")))
        .withColumn("trade_value", expr("price * size"))
        .drop("price_str", "size_str", "trade_time_str", "trade_product_id")
    )


def prepare_tickers(batch_df):
    exploded = (
        batch_df.withColumn("ticker_arr", col("event.tickers"))
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

    return (
        exploded.withColumn("product_id", coalesce(col("ticker_product_id"), col("kafka_key")))
        .withColumn("price", col("price_str").cast("double"))
        .withColumn("best_bid", col("best_bid_str").cast("double"))
        .withColumn("best_ask", col("best_ask_str").cast("double"))
        .withColumn("price_pct_chg_24h", col("chg24_str").cast("double"))
        .withColumn("volume_24h", col("volume_24h_str").cast("double"))
        .withColumn("high_24h", col("high_24_h_str").cast("double"))
        .withColumn("low_24h", col("low_24_h_str").cast("double"))
        .withColumn("mid_price", expr("(best_bid + best_ask) / 2"))
        .withColumn("spread", expr("best_ask - best_bid"))
        .drop(
            "price_str",
            "best_bid_str",
            "best_ask_str",
            "chg24_str",
            "volume_24h_str",
            "high_24_h_str",
            "low_24_h_str",
            "ticker_product_id",
        )
    )


def prepare_candles(batch_df):
    exploded = (
        batch_df.withColumn("candles_arr", col("event.candles"))
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

    return (
        exploded.withColumn("product_id", coalesce(col("candle_product_id"), col("kafka_key")))
        .withColumn("open", col("open_str").cast("double"))
        .withColumn("high", col("high_str").cast("double"))
        .withColumn("low", col("low_str").cast("double"))
        .withColumn("close", col("close_str").cast("double"))
        .withColumn("volume", col("volume_str").cast("double"))
        .withColumn("range", expr("high - low"))
        .withColumn("avg_price", expr("(open + high + low + close) / 4"))
        .drop("open_str", "high_str", "low_str", "close_str", "volume_str", "candle_product_id")
    )


def write_market_trades(batch_df, batch_id):
    # Step 1: aggregate only this batch
    agg = (
        batch_df.withColumn("minute_bucket", date_trunc("minute", col("trade_time")))
        .groupBy("product_id", "minute_bucket")
        .agg(
            _sum("size").alias("total_volume"),
            _sum(expr("price * size")).alias("notional_value"),
            _count("*").alias("trade_count"),
            _sum(when(col("side") == "BUY", col("size"))).alias("buy_volume"),
            _sum(when(col("side") == "SELL", col("size"))).alias("sell_volume"),
        )
    )

    out = (
        agg.withColumn("trade_count", coalesce(col("trade_count"), lit(0)))
        .withColumn("total_volume", _round(coalesce(col("total_volume"), lit(0.0)), 8))
        .withColumn("buy_volume", _round(coalesce(col("buy_volume"), lit(0.0)), 8))
        .withColumn("sell_volume", _round(coalesce(col("sell_volume"), lit(0.0)), 8))
        .withColumn("notional_value", _round(coalesce(col("notional_value"), lit(0.0)), 4))
        .withColumn("trade_time_utc", col("minute_bucket"))
        .drop("minute_bucket")
    )

    # Step 2: write batch aggregates to staging (optional but fine)
    jdbc_write(out, MARKET_TRADES_STAGING_TABLE)

    # Step 3: incremental UPSERT (ADD, not replace)
    run_pg_query(
        file_path=SQL_PATH / "upsert_market_trades.sql",
        staging_table=MARKET_TRADES_STAGING_TABLE,
        final_table=MARKET_TRADES_FINAL_TABLE
    )

    logger.debug("[market_trades] batch %s results:", batch_id)


def write_ticker(batch_df, batch_id):
    w = Window.partitionBy("product_id", "minute_bucket").orderBy(col("kafka_offset").desc())
    latest = (
        batch_df.withColumn("minute_bucket", date_trunc("minute", col("message_ts_utc")))
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
            "minute_bucket",
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

    jdbc_write(out, TICKER_STAGING_TABLE)

    run_pg_query(
        file_path=SQL_PATH / "upsert_tickers.sql",
        staging_table=TICKER_STAGING_TABLE,
        final_table=TICKER_FINAL_TABLE
    )

    logger.debug("[ticker] batch %s results:", batch_id)


def write_candles(batch_df, batch_id):
    w = Window.partitionBy("product_id", "minute_bucket").orderBy(col("kafka_offset").desc())
    latest = (
        batch_df.withColumn("minute_bucket", date_trunc("minute", col("message_ts_utc")))
        .withColumn("rn", row_number().over(w))
        .where(col("rn") == 1)
        .drop("rn")
    )

    out = (
        latest.select("product_id", "open", "high", "low", "close", "range", "avg_price", "minute_bucket")
        .withColumn("open", _round(coalesce(col("open"), lit(0.0)), 4))
        .withColumn("high", _round(coalesce(col("high"), lit(0.0)), 4))
        .withColumn("low", _round(coalesce(col("low"), lit(0.0)), 4))
        .withColumn("close", _round(coalesce(col("close"), lit(0.0)), 4))
        .withColumn("range", _round(coalesce(col("range"), lit(0.0)), 4))
        .withColumn("avg_price", _round(coalesce(col("avg_price"), lit(0.0)), 4))
        .withColumn("candle_ts_utc", col("minute_bucket"))
        .drop("minute_bucket")
    )

    jdbc_write(out, CANDLES_STAGING_TABLE)

    run_pg_query(
        file_path=SQL_PATH / "upsert_candles.sql",
        staging_table=CANDLES_STAGING_TABLE,
        final_table=CANDLES_FINAL_TABLE
    )

    logger.debug("[candles] batch %s results:", batch_id)


def process_batch(batch_df, batch_id):
    """
    One micro-batch from the parsed Kafka stream.
    We persist because the batch is reused for trades, tickers, and candles.
    """
    batch_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        logger.info("Processing batch %s", batch_id)

        trades_df = prepare_market_trades(batch_df)
        tickers_df = prepare_tickers(batch_df)
        candles_df = prepare_candles(batch_df)

        if not trades_df.rdd.isEmpty():
            write_market_trades(trades_df, batch_id)
        else:
            logger.info("[market_trades] batch %s empty", batch_id)

        if not tickers_df.rdd.isEmpty():
            write_ticker(tickers_df, batch_id)
        else:
            logger.info("[ticker] batch %s empty", batch_id)

        if not candles_df.rdd.isEmpty():
            write_candles(candles_df, batch_id)
        else:
            logger.info("[candles] batch %s empty", batch_id)

    except Exception:
        logger.exception("Failed processing batch %s", batch_id)
        raise
    finally:
        batch_df.unpersist()

# -------------------------------------------------------------------
# Main method
# -------------------------------------------------------------------

def main():
    try:
        logger.info("Starting application")

        # Step 1: clean staging tables
        truncate_staging_tables()

        # Step 2: start streaming query
        query = (
            filtered.writeStream
            .trigger(processingTime=PROCESSING_TIME)
            .option("checkpointLocation", CHECKPOINT_DIR)
            .foreachBatch(process_batch)
            .start()
        )

        logger.info("Streaming query started")

        # Step 3: handle shutdown signals
        signal.signal(signal.SIGINT, _request_shutdown)
        signal.signal(signal.SIGTERM, _request_shutdown)

        # Step 4: wait loop
        logger.info("Streaming running...")
        while not shutdown_requested.is_set():
            try:
                spark.streams.awaitAnyTermination(1)
            except Py4JError:
                if shutdown_requested.is_set():
                    break
                raise

    except Exception:
        logger.exception("Fatal error in main")
        raise

    finally:
        logger.info("Stopping streaming query")

        try:
            if query and query.isActive:
                query.stop()
        except (Py4JError, Py4JJavaError, Py4JNetworkError, ConnectionRefusedError):
            logger.info("Query already stopped")
        except Exception:
            logger.exception("Error stopping query")

        try:
            spark.stop()
        except (Py4JError, Py4JJavaError, Py4JNetworkError, ConnectionRefusedError):
            logger.info("Spark already stopped")
        except Exception:
            logger.exception("Error stopping Spark")

if __name__ == "__main__":
    main()
