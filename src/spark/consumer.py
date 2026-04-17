"""
Coinbase Kafka -> Spark Streaming -> PostgreSQL pipeline.

Overview
--------
- Consumes real-time market data from Kafka topics (Coinbase channels).
- Parses and flattens nested JSON messages using Spark Structured Streaming.
- Processes data into three streams:
    * Market trades
    * Tickers
    * Candles
- Writes processed data into PostgreSQL using staging + upsert pattern.
- Ensures fault tolerance using Spark checkpoints and controlled shutdown.

Architecture
------------
Kafka (Coinbase topics)
    -> Spark Structured Streaming (micro-batches)
        -> Parse + filter + explode events
        -> Prepare datasets (trades / tickers / candles)
        -> Write to PostgreSQL (staging tables)
        -> Execute SQL upserts into final tables

Core Flow
---------
Kafka Stream
    -> Raw ingestion (binary Kafka records)
    -> JSON parsing using predefined schema
    -> Filtering valid "update" events
    -> Batch processing (foreachBatch)

Batch Processing (per micro-batch)
    -> Extract trades, tickers, candles
    -> Transform and enrich data
    -> Write to staging tables via JDBC
    -> Run SQL upserts to final tables

Core Functions
--------------

process_batch()
    Entry point for each Spark micro-batch. Coordinates all transformations
    and writes (trades, tickers, candles).

prepare_market_trades()
    Extracts and transforms trade-level data from nested events.

prepare_tickers()
    Extracts ticker data and computes derived metrics (mid_price, spread, etc.).

prepare_candles()
    Extracts candle data and computes derived metrics (range, avg_price).

write_market_trades()
    Writes trade data to staging, then upserts into raw and final tables.

write_ticker()
    Deduplicates latest ticker per minute and writes to PostgreSQL.

write_candles()
    Deduplicates latest candle per minute and writes to PostgreSQL.

main()
    Initializes streaming query, registers shutdown handlers,
    and manages application lifecycle.

Helper Functions
----------------

_parse_message_timestamp()
    Parses incoming timestamp strings with multiple supported formats.

_jdbc_write()
    Writes Spark DataFrame to PostgreSQL using JDBC with partitioning.

_load_sql()
    Loads SQL file from disk with caching for performance.

_run_pg_query()
    Formats and executes SQL queries (typically upserts).

_shutdown_watcher()
    Ensures graceful shutdown by stopping the stream only after current batch finishes.

_request_shutdown()
    Signal handler that triggers controlled shutdown.

Notes
-----
- Requires Kafka SSL configuration and PostgreSQL credentials via env vars.
- Assumes consistent Coinbase message schema.
- Designed for near real-time ingestion with controlled batching.
"""

# ---------------------------------------------------------------------
# Standard Library Imports
# ---------------------------------------------------------------------
import os                                              # Environment variables and OS interaction
import sys                                             # System-specific parameters and exit handling
import json                                            # JSON parsing and serialization
import time                                            # Time utilities (sleep, timestamps)
import signal                                          # Signal handling for graceful shutdown
import threading                                       # Threading primitives (Event, Thread)
import logging                                         # Logging framework
from pathlib import Path                               # File system path handling (object-oriented)
from functools import lru_cache                        # Caching decorator for SQL file loading
from logging.handlers import TimedRotatingFileHandler  # Log rotation (daily logs)


# ---------------------------------------------------------------------
# Database (PostgreSQL)
# ---------------------------------------------------------------------
from psycopg2 import pool                     # Connection pooling for PostgreSQL


# ---------------------------------------------------------------------
# Spark (Distributed Processing)
# ---------------------------------------------------------------------
from pyspark import StorageLevel              # Persistence levels for DataFrames
from pyspark.sql import SparkSession, Window  # Spark entry point and window functions


# ---------------------------------------------------------------------
# Spark SQL Functions (Data Transformations)
# ---------------------------------------------------------------------
from pyspark.sql.functions import (
    col,                                    # Column reference
    coalesce,                               # First non-null value
    date_trunc,                             # Truncate timestamp to time unit (e.g., minute)
    explode,                                # Expand array into multiple rows
    expr,                                   # SQL expression evaluation
    from_json,                              # Parse JSON into structured columns
    lit,                                    # Create literal column
    row_number,                             # Window ranking function
    round as _round,                        # Numeric rounding (aliased to avoid conflict)
    size,                                   # Array size
    to_timestamp,                           # Convert string → timestamp
    to_utc_timestamp                        # Convert timestamp to UTC
)


# ---------------------------------------------------------------------
# Spark SQL Types (Schema Definitions)
# ---------------------------------------------------------------------
from pyspark.sql.types import (
    ArrayType,                              # Array column type
    LongType,                               # Long/integer type
    StringType,                             # String type
    StructField,                            # Field definition in schema
    StructType,                             # Schema definition
)


# =====================================================================
# File System Paths
# =====================================================================

# Root directory of the project
# __file__ -> current file location
# .resolve() -> absolute path
# .parents[2] -> move up 2 levels to reach project root
ROOT_DIR = Path(__file__).resolve().parents[2]

# --- Configuration files ---
CONFIG_PATH = ROOT_DIR / "src" / "config.json"   # Main config (topics, schema, tables)
SQL_PATH = ROOT_DIR / "src" / "sql"              # Directory containing SQL query files (upserts, etc.)

# Ensure required directories exist before starting the application
# - logs: for application and Spark logs
# - checkpoints: for Spark Structured Streaming state management
(ROOT_DIR / "logs").mkdir(parents=True, exist_ok=True)
(ROOT_DIR / "checkpoints" / "coinbase_consumer").mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR = str(ROOT_DIR / "checkpoints" / "coinbase_consumer")

# --- Log file path ---
LOG_FILE = ROOT_DIR / "logs" / "consumer.log"


# =====================================================================
# Logging
# =====================================================================

# Rotate logs daily at midnight
# Keep only last 2 rotated files to limit disk usage
handler = TimedRotatingFileHandler(
    filename=LOG_FILE,
    when="midnight",
    interval=1,
    backupCount=2,
)

# Define log format (timestamp + level + message)
formatter = logging.Formatter(
    "%(asctime)s level=%(levelname)s %(message)s"
)
handler.setFormatter(formatter)

# --- Application logger ---
logger = logging.getLogger("spark-consumer")
logger.setLevel(logging.INFO)  # Default log level

# Prevent duplicate handlers if logger is re-initialized
if not logger.handlers:
    logger.addHandler(handler)


# =====================================================================
# Configuration & Constants
# =====================================================================

# --- Kafka configuration ---
BOOTSTRAP = os.environ.get("KAFKA_SERVER_URL")                           # Kafka broker address

# --- Spark configuration ---
SPARK_THREADS = os.environ.get("SPARK_THREADS", "2")                     # Number of local Spark threads
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "1g")        # Driver memory allocation
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "1g")    # Executor memory allocation
SPARK_CHECKPOINTS_TO_RETAIN = os.environ.get(
    "SPARK_CHECKPOINTS_TO_RETAIN", "10"
)                                                                        # Number of streaming checkpoints to retain

# --- Kafka SSL configuration ---
KEYSTORE = os.environ.get("KEYSTORE")               # Path to client keystore
KEYSTORE_PASS = os.environ.get("KEYSTORE_PASS")     # Keystore password
KEYSTORE_TYPE = os.environ.get("KEYSTORE_TYPE")     # Keystore type (e.g., JKS, PKCS12)
TRUSTSTORE = os.environ.get("TRUSTSTORE")           # Path to truststore (CA certs)
TRUSTSTORE_PASS = os.environ.get("TRUSTSTORE_PASS") # Truststore password

# --- PostgreSQL configuration ---
PG_HOST = os.environ.get("PG_HOST")                 # Database host
PG_PORT = os.environ.get("PG_PORT")                 # Database port
PG_DATABASE = os.environ.get("PG_DATABASE")         # Database name
PG_USERNAME = os.environ.get("PG_USERNAME")         # Username
PG_PASSWORD = os.environ.get("PG_PASSWORD")         # Password

# --- Streaming / processing configuration ---
PROCESSING_TIME = os.environ.get("PROCESSING_TIME", "43 seconds")          # Micro-batch trigger interval
SPARK_BATCHSIZE = os.environ.get("SPARK_BATCHSIZE", "2000")                # Max Kafka offsets per batch
JDBC_WRITE_PARTITIONS = int(os.environ.get("JDBC_WRITE_PARTITIONS", "4"))  # Parallel DB write partitions
JDBC_BATCHSIZE = int(os.environ.get("JDBC_BATCHSIZE", "2000"))             # JDBC batch insert size


# ---------------------------------------------------------------------
# Environment Validation
# ---------------------------------------------------------------------

# Required environment variables for startup
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

# Identify missing variables
missing_env = [
    name for name, value in required_env.items() if not value
]

# Fail fast if any required configuration is missing
if missing_env:
    logger.error(
        "startup_validation_failed missing_env_vars=%s",
        ",".join(missing_env),
    )
    raise ValueError(
        f"Missing required environment variables: {', '.join(missing_env)}"
    )


# =====================================================================
# Load Config
# =====================================================================

try:
    # Load JSON configuration file
    with open(CONFIG_PATH, "r") as cfg:
        CONFIG = json.load(cfg)

    # --- Kafka topics ---
    # Extract topic names from config channels (values)
    TOPICS = ",".join(CONFIG["channels"].values())

    # --- Database schema ---
    SCHEMA = CONFIG["schema"]  # Base schema name for all tables

    # --- Table mappings ---
    # Build fully qualified table names (schema.table)
    TABLES = {
        key: {
            "staging": f"{SCHEMA}.{value['staging']}",   # staging table (raw ingest)
            "raw": f"{SCHEMA}.{value.get('raw')}",       # optional raw table
            "final": f"{SCHEMA}.{value['final']}",       # final aggregated table
        }
        for key, value in CONFIG["tables"].items()
    }

    # --- Specific table references ---

    # Ticker tables
    TICKER_STAGING_TABLE = TABLES["ticker"]["staging"]
    TICKER_FINAL_TABLE   = TABLES["ticker"]["final"]

    # Candle tables
    CANDLES_STAGING_TABLE = TABLES["candles"]["staging"]
    CANDLES_FINAL_TABLE   = TABLES["candles"]["final"]

    # Market trades tables
    MARKET_TRADES_STAGING_TABLE = TABLES["market_trades"]["staging"]
    MARKET_TRADES_RAW_TABLE     = TABLES["market_trades"]["raw"]
    MARKET_TRADES_FINAL_TABLE   = TABLES["market_trades"]["final"]

except FileNotFoundError:
    # Config file missing at expected path
    logger.exception("config_file_not_found path=%s", CONFIG_PATH)
    sys.exit(1)

except json.JSONDecodeError:
    # Config file contains invalid JSON
    logger.exception("config_invalid_json path=%s", CONFIG_PATH)
    sys.exit(1)

except KeyError:
    # Required keys missing (e.g., channels, tables, schema)
    logger.exception("config_invalid_format path=%s", CONFIG_PATH)
    sys.exit(1)


# =====================================================================
# Connection Initialization & Utilites
# =====================================================================

# Create a connection pool to reuse DB connections efficiently
# minconn = 1 → minimum open connections
# maxconn = 5 → maximum concurrent connections
pg_pool = pool.SimpleConnectionPool(
    1,
    5,
    host=PG_HOST,
    port=PG_PORT,
    database=PG_DATABASE,
    user=PG_USERNAME,
    password=PG_PASSWORD,
)

# Spark Session Initialization
try:
    spark = (
        SparkSession.builder
        .appName("spark-consumer")                               # Application name (visible in Spark UI)
        .master(f"local[{SPARK_THREADS}]")                       # Run locally with N threads
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)      # Driver memory allocation
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)  # Executor memory allocation
        .config("spark.sql.session.timeZone", "UTC")             # Ensure consistent timestamp handling
        .config(
            "spark.sql.streaming.minBatchesToRetain",
            SPARK_CHECKPOINTS_TO_RETAIN
        )                                                        # Retain limited checkpoint history
        .config(
            "spark.sql.streaming.metricsEnabled",
            "false"
        )                                                        # Disable extra metrics (reduces overhead)
        .getOrCreate()
    )

    # Reduce Spark internal logging noise
    spark.sparkContext.setLogLevel("WARN")

    logger.info("spark_session_started app=spark-consumer")

except Exception:
    logger.exception("spark_session_init_failed")
    raise

# JDBC connection string for PostgreSQL
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# JDBC write properties
PG_PROPS = {
    "user": PG_USERNAME,                      # DB username
    "password": PG_PASSWORD,                  # DB password
    "driver": "org.postgresql.Driver",        # PostgreSQL JDBC driver
    "batchsize": str(JDBC_BATCHSIZE),         # Batch size for inserts (performance tuning)
}

# Indicates that shutdown has been requested (via signal handler)
shutdown_requested = threading.Event()

# Tracks whether a micro-batch is currently being processed
batch_in_progress = threading.Event()

# Reference to active streaming query (set later in main)
query = None


# =====================================================================
# Schema
# =====================================================================

# --- Trade schema ---
# Represents individual trade events inside "trades" array
tradeSchema = StructType(
    [
        StructField("price", StringType(), True),        # Trade price
        StructField("product_id", StringType(), True),   # Trading pair (e.g., BTC-USD)
        StructField("side", StringType(), True),         # Buy / Sell
        StructField("size", StringType(), True),         # Quantity traded
        StructField("time", StringType(), True),         # Trade timestamp
        StructField("trade_id", StringType(), True),     # Unique trade identifier
    ]
)

# --- Ticker schema ---
# Represents real-time market summary data inside "tickers" array
tickerItemSchema = StructType(
    [
        StructField("best_ask", StringType(), True),                # Lowest ask price
        StructField("best_ask_quantity", StringType(), True),       # Quantity at best ask
        StructField("best_bid", StringType(), True),                # Highest bid price
        StructField("best_bid_quantity", StringType(), True),       # Quantity at best bid
        StructField("high_24_h", StringType(), True),               # 24h high price
        StructField("high_52_w", StringType(), True),               # 52-week high
        StructField("low_24_h", StringType(), True),                # 24h low price
        StructField("low_52_w", StringType(), True),                # 52-week low
        StructField("price", StringType(), True),                   # Current price
        StructField("price_percent_chg_24_h", StringType(), True),  # 24h % change
        StructField("product_id", StringType(), True),              # Trading pair
        StructField("type", StringType(), True),                    # Message type
        StructField("volume_24_h", StringType(), True),             # 24h traded volume
    ]
)

# --- Candle schema ---
# Represents OHLCV data inside "candles" array
candleItemSchema = StructType(
    [
        StructField("close", StringType(), True),        # Closing price
        StructField("high", StringType(), True),         # Highest price
        StructField("low", StringType(), True),          # Lowest price
        StructField("open", StringType(), True),         # Opening price
        StructField("product_id", StringType(), True),   # Trading pair
        StructField("start", StringType(), True),        # Candle start time
        StructField("volume", StringType(), True),       # Volume during interval
    ]
)

# --- Events schema ---
# Wrapper containing different event types (trades, tickers, candles)
eventsSchema = StructType(
    [
        StructField("trades", ArrayType(tradeSchema), True),       # List of trades
        StructField("tickers", ArrayType(tickerItemSchema), True), # List of ticker updates
        StructField("candles", ArrayType(candleItemSchema), True), # List of candles
        StructField("type", StringType(), True),                   # Event type (e.g., "update")
    ]
)

# --- Top-level schema ---
# Root structure of incoming Kafka message
topSchema = StructType(
    [
        StructField("channel", StringType(), True),            # Channel name (e.g., ticker, trades)
        StructField("events", ArrayType(eventsSchema), True),  # Nested event payload
        StructField("sequence_num", LongType(), True),         # Sequence number (ordering)
        StructField("timestamp", StringType(), True),          # Message timestamp
    ]
)


# =====================================================================
# Core Functions
# =====================================================================

def prepare_market_trades(batch_df):
    """
    Extract and transform trade-level data from a Spark micro-batch.

    This function processes nested trade events from the incoming Kafka stream,
    flattens them into a tabular format, converts data types, and derives
    additional fields required for downstream storage.

    Parameters
    ----------
    batch_df : DataFrame
        Input Spark DataFrame containing parsed Kafka messages with nested events.

    Returns
    -------
    DataFrame
        Transformed DataFrame with one row per trade, including normalized
        numeric fields and UTC timestamps.

    Notes
    -----
    - Extracts trades from nested `event.trades` array.
    - Uses `explode` to convert array elements into individual rows.
    - Falls back to `kafka_key` if `product_id` is missing.
    - Casts numeric fields (price, size) from string → double.
    - Converts trade timestamps to UTC.
    - Computes derived metric:
        * `trade_value = price * size`
    - Drops intermediate/raw string columns to keep output clean.
    """

    return (
        batch_df

        # Extract trades array from event payload
        .withColumn("trades_arr", col("event.trades"))
        .where(col("trades_arr").isNotNull())

        # Explode array → one row per trade
        .select("kafka_key", explode(col("trades_arr")).alias("trade"))

        # Flatten nested trade fields
        .select(
            col("kafka_key"),
            col("trade.price").alias("price_str"),
            col("trade.product_id").alias("trade_product_id"),
            col("trade.side").alias("side"),
            col("trade.size").alias("size_str"),
            col("trade.time").alias("trade_time_str"),
            col("trade.trade_id").alias("trade_id"),
        )

        # Resolve product_id (fallback to Kafka key if missing)
        .withColumn(
            "product_id",
            coalesce(col("trade_product_id"), col("kafka_key"))
        )

        # Convert numeric fields (string → double)
        .withColumn("price", col("price_str").cast("double"))
        .withColumn("size", col("size_str").cast("double"))

        # Convert timestamp to UTC
        .withColumn("trade_time_utc", to_timestamp(col("trade_time_str")))
        .withColumn(
            "trade_time_utc",
            to_utc_timestamp("trade_time_utc", "UTC")
        )

        # Derived metrics
        .withColumn("trade_value", expr("price * size"))

        # Cleanup intermediate columns
        .drop(
            "price_str",
            "size_str",
            "trade_time_str",
            "trade_product_id",
            "price",
            "kafka_key"
        )
    )


def prepare_tickers(batch_df):
    """
    Extract and transform ticker-level data from a Spark micro-batch.

    This function processes nested ticker events from the incoming Kafka stream,
    flattens them into a tabular format, converts data types, and derives
    additional market metrics for downstream storage.

    Parameters
    ----------
    batch_df : DataFrame
        Input Spark DataFrame containing parsed Kafka messages with nested events.

    Returns
    -------
    DataFrame
        Transformed DataFrame with one row per ticker, including normalized
        numeric fields and derived metrics.

    Notes
    -----
    - Extracts tickers from nested `event.tickers` array.
    - Uses `explode` to convert array elements into individual rows.
    - Falls back to `kafka_key` if `product_id` is missing.
    - Casts numeric fields from string → double.
    - Computes derived metrics:
        * `mid_price = (best_bid + best_ask) / 2`
        * `spread = best_ask - best_bid`
    - Retains Kafka offset for ordering/deduplication downstream.
    - Drops intermediate/raw string columns to keep output clean.
    """

    # -----------------------------------------------------------------
    # Extract and explode ticker array
    # -----------------------------------------------------------------
    exploded = (
        batch_df

        # Extract ticker array from event payload
        .withColumn("ticker_arr", col("event.tickers"))
        .where(col("ticker_arr").isNotNull())

        # Explode array → one row per ticker
        .select(
            "kafka_key",
            "offset",
            "message_ts_utc",
            explode(col("ticker_arr")).alias("ticker")
        )

        # Flatten nested ticker fields
        .select(
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
        exploded

        # Resolve product_id (fallback to Kafka key if missing)
        .withColumn(
            "product_id",
            coalesce(col("ticker_product_id"), col("kafka_key"))
        )

        # Convert numeric fields (string → double)
        .withColumn("price", col("price_str").cast("double"))
        .withColumn("best_bid", col("best_bid_str").cast("double"))
        .withColumn("best_ask", col("best_ask_str").cast("double"))
        .withColumn("price_pct_chg_24h", col("chg24_str").cast("double"))
        .withColumn("volume_24h", col("volume_24h_str").cast("double"))
        .withColumn("high_24h", col("high_24_h_str").cast("double"))
        .withColumn("low_24h", col("low_24_h_str").cast("double"))

        # Derived metrics
        .withColumn("mid_price", expr("(best_bid + best_ask) / 2"))
        .withColumn("spread", expr("best_ask - best_bid"))

        # Cleanup intermediate columns
        .drop(
            "price_str",
            "best_bid_str",
            "best_ask_str",
            "chg24_str",
            "volume_24h_str",
            "high_24_h_str",
            "low_24_h_str",
            "ticker_product_id",
            "kafka_key"
        )
    )


def prepare_candles(batch_df):
    """
    Extract and transform candle (OHLCV) data from a Spark micro-batch.

    This function processes nested candle events from the incoming Kafka stream,
    flattens them into a tabular format, converts data types, and derives
    additional metrics for downstream storage.

    Parameters
    ----------
    batch_df : DataFrame
        Input Spark DataFrame containing parsed Kafka messages with nested events.

    Returns
    -------
    DataFrame
        Transformed DataFrame with one row per candle, including normalized
        numeric fields and derived metrics.

    Notes
    -----
    - Extracts candles from nested `event.candles` array.
    - Uses `explode` to convert array elements into individual rows.
    - Falls back to `kafka_key` if `product_id` is missing.
    - Casts numeric fields from string → double.
    - Computes derived metrics:
        * `range = high - low`
        * `avg_price = (open + high + low + close) / 4`
    - Retains Kafka offset for ordering/deduplication downstream.
    - Drops intermediate/raw string columns to keep output clean.
    """

    # Extract and explode candle array
    exploded = (
        batch_df

        # Extract candle array from event payload
        .withColumn("candles_arr", col("event.candles"))
        .where(col("candles_arr").isNotNull())

        # Explode array → one row per candle
        .select(
            "kafka_key",
            "offset",
            "message_ts_utc",
            explode(col("candles_arr")).alias("candle")
        )

        # Flatten nested candle fields
        .select(
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
        exploded

        # Resolve product_id (fallback to Kafka key if missing)
        .withColumn(
            "product_id",
            coalesce(col("candle_product_id"), col("kafka_key"))
        )

        # Convert numeric fields (string → double)
        .withColumn("open", col("open_str").cast("double"))
        .withColumn("high", col("high_str").cast("double"))
        .withColumn("low", col("low_str").cast("double"))
        .withColumn("close", col("close_str").cast("double"))
        .withColumn("volume", col("volume_str").cast("double"))

        # Derived metrics
        .withColumn("range", expr("high - low"))
        .withColumn("avg_price", expr("(open + high + low + close) / 4"))

        # Cleanup intermediate columns
        .drop(
            "open_str",
            "high_str",
            "low_str",
            "close_str",
            "volume_str",
            "candle_product_id",
            "kafka_key"
        )
    )


def write_market_trades(batch_df, batch_id):
    """
    Write trade data to PostgreSQL using staging + upsert pattern.

    This function:
    - Deduplicates trades within the batch
    - Writes data to staging table
    - Executes SQL to upsert into raw and final tables

    Parameters
    ----------
    batch_df : DataFrame
        Input DataFrame containing one row per trade.
    batch_id : int
        Spark micro-batch identifier.

    Returns
    -------
    None

    Notes
    -----
    - Assumes input is already flattened by `prepare_market_trades`.
    - Deduplication is done using `trade_id`.
    - Uses staging → raw → final pipeline for idempotent writes.
    """

    # Deduplicate trades within batch
    raw_out = (
        batch_df.select(
            "trade_id",
            "product_id",
            "side",
            "trade_value",
            "size",
            "trade_time_utc",
        )
        .dropDuplicates(["trade_id"])
    )

    # Write to staging table
    _jdbc_write(raw_out, MARKET_TRADES_STAGING_TABLE)

    # Upsert into raw + final tables
    _run_pg_query(
        file_path=SQL_PATH / "upsert_market_trades.sql",
        staging_table=MARKET_TRADES_STAGING_TABLE,
        raw_table=MARKET_TRADES_RAW_TABLE,
        final_table=MARKET_TRADES_FINAL_TABLE,
    )

    logger.debug("market_trades_batch_written batch_id=%s", batch_id)


def write_ticker(batch_df, batch_id):
    """
    Write ticker data to PostgreSQL with per-minute deduplication.

    This function:
    - Keeps only the latest ticker per product per minute
    - Normalizes numeric precision
    - Writes to staging and upserts into final table

    Parameters
    ----------
    batch_df : DataFrame
        Input DataFrame containing ticker records.
    batch_id : int
        Spark micro-batch identifier.

    Returns
    -------
    None

    Notes
    -----
    - Uses Kafka offset to determine latest record.
    - Aggregation granularity is 1 minute.
    """

    # Window for latest record per product per minute
    w = Window.partitionBy("product_id", "minute_bucket") \
              .orderBy(col("kafka_offset").desc())

    latest = (
        batch_df
        .withColumn("minute_bucket", date_trunc("minute", col("message_ts_utc")))
        .withColumn("rn", row_number().over(w))
        .where(col("rn") == 1)
        .drop("rn")
    )

    # Normalize values and prepare output
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

        # Normalize numeric precision
        .withColumn("price", _round(coalesce(col("price"), lit(0.0)), 4))
        .withColumn("best_bid", _round(coalesce(col("best_bid"), lit(0.0)), 4))
        .withColumn("best_ask", _round(coalesce(col("best_ask"), lit(0.0)), 4))
        .withColumn("mid_price", _round(coalesce(col("mid_price"), lit(0.0)), 4))
        .withColumn("spread", _round(coalesce(col("spread"), lit(0.0)), 4))
        .withColumn("price_pct_chg_24h", _round(coalesce(col("price_pct_chg_24h"), lit(0.0)), 4))
        .withColumn("volume_24h", _round(coalesce(col("volume_24h"), lit(0.0)), 8))
        .withColumn("high_24h", _round(coalesce(col("high_24h"), lit(0.0)), 4))
        .withColumn("low_24h", _round(coalesce(col("low_24h"), lit(0.0)), 4))

        # Normalize timestamp
        .withColumn("ticker_ts_utc", col("minute_bucket"))
        .withColumn("ticker_ts_utc", to_utc_timestamp("ticker_ts_utc", "UTC"))

        .drop("minute_bucket")
    )

    # Write to DB
    _jdbc_write(out, TICKER_STAGING_TABLE)

    _run_pg_query(
        file_path=SQL_PATH / "upsert_tickers.sql",
        staging_table=TICKER_STAGING_TABLE,
        final_table=TICKER_FINAL_TABLE
    )

    logger.debug("ticker_batch_written batch_id=%s", batch_id)


def write_candles(batch_df, batch_id):
    """
    Write candle (OHLCV) data to PostgreSQL with per-minute deduplication.

    This function:
    - Keeps latest candle per product per minute
    - Normalizes numeric precision
    - Writes to staging and upserts into final table

    Parameters
    ----------
    batch_df : DataFrame
        Input DataFrame containing candle records.
    batch_id : int
        Spark micro-batch identifier.

    Returns
    -------
    None

    Notes
    -----
    - Uses Kafka offset to determine latest record.
    - Aggregation granularity is 1 minute.
    """

    # Window for latest record per product per minute
    w = Window.partitionBy("product_id", "minute_bucket") \
              .orderBy(col("kafka_offset").desc())

    latest = (
        batch_df
        .withColumn("minute_bucket", date_trunc("minute", col("message_ts_utc")))
        .withColumn("rn", row_number().over(w))
        .where(col("rn") == 1)
        .drop("rn")
    )

    # Normalize values and prepare output
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

        # Normalize numeric precision
        .withColumn("open", _round(coalesce(col("open"), lit(0.0)), 4))
        .withColumn("high", _round(coalesce(col("high"), lit(0.0)), 4))
        .withColumn("low", _round(coalesce(col("low"), lit(0.0)), 4))
        .withColumn("close", _round(coalesce(col("close"), lit(0.0)), 4))
        .withColumn("range", _round(coalesce(col("range"), lit(0.0)), 4))
        .withColumn("avg_price", _round(coalesce(col("avg_price"), lit(0.0)), 4))

        # Normalize timestamp
        .withColumn("candle_ts_utc", col("minute_bucket"))
        .withColumn("candle_ts_utc", to_utc_timestamp("candle_ts_utc", "UTC"))

        .drop("minute_bucket")
    )

    # Write to DB
    _jdbc_write(out, CANDLES_STAGING_TABLE)

    _run_pg_query(
        file_path=SQL_PATH / "upsert_candles.sql",
        staging_table=CANDLES_STAGING_TABLE,
        final_table=CANDLES_FINAL_TABLE
    )

    logger.debug("candles_batch_written batch_id=%s", batch_id)


def process_batch(batch_df, batch_id):
    """
    Process a single Spark micro-batch from the Kafka stream.

    This function orchestrates the full data pipeline for each batch:
    - Extracts trades, tickers, and candles
    - Transforms each dataset independently
    - Writes results to PostgreSQL using staging + upsert pattern

    Parameters
    ----------
    batch_df : DataFrame
        Input Spark DataFrame representing one micro-batch of parsed Kafka data.
    batch_id : int
        Unique identifier for the micro-batch (provided by Spark).

    Returns
    -------
    None

    Notes
    -----
    - Batch is persisted because it is reused across multiple transformations.
    - Uses MEMORY_AND_DISK to avoid recomputation and handle large batches.
    - Honors graceful shutdown:
        * Skips new batches if shutdown is requested
        * Ignores errors during shutdown phase
    - Ensures cleanup via unpersist() in all cases.
    """

    # Skip processing if shutdown already requested
    if shutdown_requested.is_set():
        logger.info("batch_skipped_during_shutdown batch_id=%s", batch_id)
        return

    # Mark batch as in-progress (used by shutdown coordination)
    batch_in_progress.set()

    # Persist batch since it will be reused multiple times
    batch_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        logger.info("batch_processing_started batch_id=%s", batch_id)

        # Prepare datasets (independent transformations)
        trades_df = prepare_market_trades(batch_df)
        tickers_df = prepare_tickers(batch_df)
        candles_df = prepare_candles(batch_df)

        # Write datasets to PostgreSQL
        write_market_trades(trades_df, batch_id)
        write_ticker(tickers_df, batch_id)
        write_candles(candles_df, batch_id)

        logger.info("batch_processing_completed batch_id=%s", batch_id)

    except Exception as e:
        # During shutdown, ignore errors to allow graceful exit
        if shutdown_requested.is_set():
            logger.info(
                "batch_error_ignored_during_shutdown batch_id=%s error=%s",
                batch_id,
                e,
            )
            return

        # Otherwise, log and propagate error
        logger.exception("batch_failed batch_id=%s", batch_id)
        raise

    finally:
        # Cleanup
        batch_df.unpersist()         # Release cached data
        batch_in_progress.clear()    # Mark batch as completed


# =====================================================================
# Helpers
# =====================================================================

def _request_shutdown(signum, frame):
    """
    Signal handler that initiates a graceful application shutdown.

    This function is registered with the OS to handle termination signals
    (e.g., Ctrl+C or container stop). Instead of stopping the application
    immediately, it triggers a controlled shutdown sequence.

    Parameters
    ----------
    signum : int
        Signal number received from the OS.
        Common values:
        - signal.SIGINT  → triggered by Ctrl+C
        - signal.SIGTERM → triggered by system/container shutdown

    frame : frame
        Current execution frame (provided by Python signal system).
        Not used in this implementation.

    Returns
    -------
    None

    Notes
    -----
    Why this function exists:
    - Abrupt termination can interrupt Spark jobs mid-batch.
    - That can lead to:
        * Partial database writes
        * Duplicate data on restart
        * Corrupted downstream state

    What this function does:
    - Logs that shutdown has been requested.
    - Sets a shared `shutdown_requested` flag.

    What it DOES NOT do:
    - It does NOT stop Spark directly.
    - It does NOT kill threads immediately.

    How shutdown actually happens:
    1. This function sets `shutdown_requested`.
    2. `_shutdown_watcher` detects the flag.
    3. It waits for the current batch to finish (`batch_in_progress`).
    4. Then safely stops the Spark streaming query.
    """

    logger.info("shutdown_signal_received signal=%s", signum)

    # Set global shutdown flag (checked by watcher thread and batch processor)
    shutdown_requested.set()


def _shutdown_watcher():
    """
    Background watcher that performs a safe, coordinated shutdown of the Spark stream.

    This function runs in a separate thread and is responsible for stopping
    the Spark Structured Streaming query only at a safe point — after the
    current micro-batch has fully completed.

    Returns
    -------
    None

    Notes
    -----
    Shutdown flow (end-to-end):
    1. `_request_shutdown()` sets `shutdown_requested` flag
    2. This watcher detects the flag
    3. Waits for active batch to complete (`batch_in_progress`)
    4. Stops the Spark query cleanly
    """

    # Step 1: Wait for shutdown signal
    # Blocks here until `_request_shutdown()` sets the flag
    shutdown_requested.wait()

    # Step 2: Wait for current batch to finish
    # If a batch is currently running, we wait until it's done
    # This prevents:
    #   - partial DB writes
    #   - inconsistent aggregation state
    while batch_in_progress.is_set():
        time.sleep(0.25)  # Small polling interval (non-busy wait)

    # Step 3: Stop the Spark streaming query safely
    global query

    # Ensure query exists and is still active before stopping
    if query is not None and query.isActive:
        logger.info("stopping_query_at_batch_boundary")

        # This triggers Spark to stop after the current micro-batch
        query.stop()


def _parse_message_timestamp(ts_col):
    """
    Parse timestamp column with support for multiple formats.

    Parameters
    ----------
    ts_col : Column
        Input timestamp column (string format).

    Returns
    -------
    Column
        Parsed timestamp column.

    Notes
    -----
    - Supports both nanosecond and microsecond precision formats.
    - Uses `coalesce` to fallback between formats.
    """

    return coalesce(
        to_timestamp(ts_col, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX"),
        to_timestamp(ts_col, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"),
    )


def _jdbc_write(df, table_name):
    """
    Write a Spark DataFrame to PostgreSQL using JDBC in parallel.

    This function handles the final step of moving processed data from Spark
    into PostgreSQL. It is optimized for batch inserts using partitioning
    and JDBC batching.

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame containing transformed data to be persisted.
    table_name : str
        Target PostgreSQL table (fully qualified: schema.table).

    Returns
    -------
    None

    Notes
    -----
    - `df.repartition(N)` splits data into N partitions.
    - Each partition is written in parallel via separate JDBC connections.
    - Without this:
        → Single-threaded write (VERY slow)
    - With too many partitions:
        → Too many DB connections → DB overload
    - `.mode("append")` inserts new rows
    - Does NOT overwrite existing data
    - Safe for streaming/micro-batch pipelines
    """

    logger.debug(
        "jdbc_write_started table=%s partitions=%d",
        table_name,
        JDBC_WRITE_PARTITIONS,
    )

    # Ensures multiple JDBC connections write data concurrently
    staged = df.repartition(JDBC_WRITE_PARTITIONS)

    try:
        # Perform JDBC write
        (
            staged.write
            .format("jdbc")
            .option("url", PG_JDBC_URL)                     # DB connection string
            .option("dbtable", table_name)                  # Target table
            .option("numPartitions", JDBC_WRITE_PARTITIONS) # Parallel writers
            .options(**PG_PROPS)                            # Credentials + batch size
            .mode("append")                                 # Insert mode
            .save()
        )

        logger.debug("jdbc_write_completed table=%s", table_name)

    except Exception:
        # Failure handling
        logger.exception("jdbc_write_failed table=%s", table_name)

        # Re-raise so Spark knows batch failed
        raise


@lru_cache(maxsize=10)
def _load_sql(file_path: str) -> str:
    """
    Load a SQL file from disk and cache its contents.

    This function reads SQL query templates from disk and caches them
    in memory to avoid repeated file I/O during streaming execution.

    Parameters
    ----------
    file_path : str
        Path to the SQL file.

    Returns
    -------
    str
        SQL query as a string.

    Notes
    -----
    - File is read once
    - Subsequent calls return from memory (very fast)
    - `lru_cache(maxsize=10)` stores up to 10 SQL files in memory.

    """

    path = Path(file_path)

    # Validate file existence
    if not path.exists():
        logger.exception("sql_file_not_found path=%s", file_path)
        sys.exit(1)

    try:
        # Read SQL file content
        with open(path, "r") as f:
            return f.read()

    except Exception:
        # Handle read failure
        logger.exception("sql_file_read_failed path=%s", file_path)
        sys.exit(1)


def _run_pg_query(file_path: str = None, **kwargs):
    """
    Execute a parameterized SQL query against PostgreSQL using a connection pool.

    This function loads a SQL template from disk, injects runtime parameters,
    and executes it within a transaction. It is typically used for upsert
    operations (staging → final tables) in the pipeline.

    Parameters
    ----------
    file_path : str
        Path to the SQL file containing the query template.
    **kwargs : dict
        Key-value pairs used to replace placeholders in the SQL template.

    Returns
    -------
    None

    Notes
    -----
    Transaction behavior:
    - `autocommit = False` ensures:
        → All operations succeed together OR fail together
    - On success:
        → `commit()` persists changes
    - On failure:
        → `rollback()` prevents partial writes
    """

    # Load SQL template (cached)
    query_template = _load_sql(file_path)

    # Inject parameters into SQL
    try:
        query = query_template.format(**kwargs)
    except KeyError as e:
        # Missing required placeholder in SQL template
        raise ValueError(f"Missing SQL placeholder: {e}")

    # Execute query using connection pool
    conn = pg_pool.getconn()

    try:
        # Disable autocommit to control transaction manually
        conn.autocommit = False

        # Execute SQL
        with conn.cursor() as cur:
            cur.execute(query)

        # Commit transaction if successful
        conn.commit()

    except Exception:
        # Handle failure (rollback)
        logger.exception("sql_execution_failed path=%s", file_path)

        conn.rollback()  # undo partial changes
        raise            # propagate error to Spark

    finally:
        # Return connection to pool
        pg_pool.putconn(conn)


# =====================================================================
# Kafka Stream Ingestion (Spark Structured Streaming)
# =====================================================================

try:
    # Read streaming data from Kafka
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)     # Kafka broker address
        .option("subscribe", TOPICS)                      # Topics to consume
        .option("startingOffsets", "latest")              # Start from latest messages
        .option("maxOffsetsPerTrigger", SPARK_BATCHSIZE)  # Max records per micro-batch
        .option("failOnDataLoss", False)                  # Continue if offsets are missing

        # --- SSL configuration for secure Kafka connection ---
        .option("kafka.security.protocol", "SSL")
        .option("kafka.ssl.truststore.location", TRUSTSTORE)
        .option("kafka.ssl.truststore.password", TRUSTSTORE_PASS)
        .option("kafka.ssl.keystore.location", KEYSTORE)
        .option("kafka.ssl.keystore.password", KEYSTORE_PASS)
        .option("kafka.ssl.keystore.type", KEYSTORE_TYPE)
        .option("kafka.ssl.key.password", KEYSTORE_PASS)

        .load()
    )

    logger.info(
        "kafka_stream_configured topics=%s batch_size=%s trigger=%s",
        TOPICS,
        SPARK_BATCHSIZE,
        PROCESSING_TIME,
    )

except Exception:
    # Fail fast if Kafka stream cannot be initialized
    logger.exception("kafka_stream_configuration_failed")
    raise

# Parse Kafka Messages (JSON → Structured Columns)
parsed = (
    raw.select(
        col("offset"),                                       # Kafka offset (for ordering/debugging)
        col("key").cast("string").alias("kafka_key"),        # Message key (product_id fallback)
        from_json(
            col("value").cast("string"),
            topSchema
        ).alias("j"),                                        # Parse JSON payload using schema
    )
    .select("offset", "kafka_key", "j.*")                    # Flatten parsed JSON
    .withColumn(
        "message_ts_utc",
        _parse_message_timestamp(col("timestamp"))           # Normalize timestamp to UTC
    )
)

# Filter Relevant Events
filtered = (
    parsed
    .where(col("events").isNotNull())                      # Ensure events exist
    .where(size(col("events")) > 0)                        # Non-empty events array

    # Extract first event (Coinbase sends events as array)
    .withColumn("event", col("events").getItem(0))

    .where(col("event").isNotNull())                       # Ensure event is valid

    # Only process "update" events (ignore snapshots / other types)
    .where(col("event.type") == "update")
)


# =====================================================================
# Core Logic
# =====================================================================

def main():
    """
    Entry point of the streaming application.

    This function initializes the Spark streaming query, registers shutdown
    handlers, and manages the full application lifecycle from startup to
    graceful termination.

    Returns
    -------
    None

    Notes
    -----
    - Register OS signal handlers for graceful shutdown
    - Start Spark Structured Streaming query
    - Launch shutdown watcher thread
    - Block until query termination
    - Ensure all resources are cleaned up properly
    """

    global query

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _request_shutdown)   # Ctrl+C
    signal.signal(signal.SIGTERM, _request_shutdown)  # Termination signal

    # Start Spark streaming query
    query = (
        filtered.writeStream

        # Micro-batch trigger interval (e.g., "43 seconds")
        .trigger(processingTime=PROCESSING_TIME)

        # Checkpoint directory for fault tolerance & recovery
        .option("checkpointLocation", CHECKPOINT_DIR)

        # Custom batch processing logic
        .foreachBatch(process_batch)

        # Start the streaming query
        .start()
    )

    # Start shutdown watcher thread
    # Runs independently to monitor shutdown flag and stop query safely
    threading.Thread(
        target=_shutdown_watcher,
        daemon=True  # exits automatically when main thread exits
    ).start()

    try:
        # Block until query stops
        # This keeps the application alive while streaming is active
        query.awaitTermination()

    finally:
        # Cleanup resources (always executed)
        # Stop Spark query if still running
        try:
            if query is not None and query.isActive:
                query.stop()
        except Exception:
            logger.exception("query_stop_failed")

        # Stop Spark session (releases cluster resources)
        try:
            spark.stop()
        except Exception:
            logger.exception("spark_stop_failed")

        # Close PostgreSQL connection pool
        try:
            pg_pool.closeall()
        except Exception:
            logger.exception("postgres_pool_close_failed")

        logger.info("shutdown_complete")

if __name__ == "__main__":
    main()
