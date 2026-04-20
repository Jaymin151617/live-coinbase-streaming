#!/usr/bin/env python3
"""
Coinbase Advanced Trade -> Kafka bridge.

Overview
--------
- Maintains one WebSocket connection per product (each in its own thread).
- WebSocket callbacks are lightweight: only extract minimal fields and enqueue messages.
- A shared bounded queue provides backpressure (drop-on-overflow policy).
- Worker threads batch messages and send them to Kafka efficiently.
- Supports graceful shutdown and automatic WebSocket reconnection.
- Optimized for high-throughput, low-latency streaming.

Architecture
------------
WebSocket Threads (per product)
    -> Parse minimal fields (channel, timestamp)
    -> Push raw messages to queue

Queue (bounded)
    -> Prevents memory overflow
    -> Drops messages if overwhelmed

Kafka Worker Threads
    -> Batch messages
    -> Send to Kafka with retries

Core Functions
--------------

kafka_worker()
    Consumes messages from the shared queue, batches them, and sends them to Kafka.

make_callbacks()
    Creates lightweight WebSocket callbacks (on_open, on_message, etc.) bound to a product.

run_ws_for_product(product)
    Runs a persistent WebSocket connection for a product with auto-reconnect.

start_workers_and_ws()
    Starts Kafka worker threads and WebSocket threads for all configured products.

shutdown()
    Gracefully shuts down WebSockets, workers, and Kafka producer.

main()
    Entry point: initializes signal handlers, starts workers, and keeps the service running.

Helper Functions
----------------

_build_jwt()
    Generates a JWT token required for authenticating with Coinbase WebSocket.

_delivery_report()
    Kafka delivery callback for logging success/failure of produced messages.

_sanitize_topic()
    Normalizes channel names into Kafka topic format.

_serialize_key()
    Encodes Kafka message key into bytes.

_serialize_value()
    Converts message payload into bytes (handles string and dict).

_produce_with_retry()
    Sends message to Kafka with retry handling and backpressure awareness.

_drain_batch()
    Iterates over a batch and sends each message to Kafka.

_send_subscription_message()
    Sends subscription messages for all configured channels over WebSocket.

_fast_extract_channel()
    Fast string-based extraction of 'channel' field from JSON message.

_fast_extract_timestamp()
    Fast string-based extraction of 'timestamp' field from JSON message.

_to_epoch_ms()
    Converts ISO8601 timestamp string to epoch milliseconds.
    Falls back to current time on parsing failure.
"""

# ---------------------------------------------------------------------
# Standard Library Imports
# ---------------------------------------------------------------------
import os                                   # OS utilities (env vars, file paths, process info)
import json                                 # JSON parsing and serialization
import logging                              # Logging system for debugging and monitoring
import queue                                # Thread-safe queue implementation
import signal                               # Signal handling (graceful shutdown, interrupts)
import ssl                                  # SSL/TLS support for secure connections
import socket                               # Low-level networking interface
import sys                                  # System-specific parameters and functions
import threading                            # Multithreading support
import time                                 # Time-related functions (sleep, timestamps)
import calendar                             # Calendar-related utilities (dates, ranges)
from pathlib import Path                    # Object-oriented filesystem paths
from typing import Tuple                    # Type hinting for tuple structures


# ---------------------------------------------------------------------
# External APIs / SDKs
# ---------------------------------------------------------------------
from coinbase import jwt_generator          # Coinbase JWT authentication generator


# ---------------------------------------------------------------------
# Streaming / Messaging Systems
# ---------------------------------------------------------------------
from confluent_kafka import Producer, KafkaException
# Kafka Producer for sending messages to topics
# KafkaException for handling Kafka-related errors


# ---------------------------------------------------------------------
# WebSocket Communication
# ---------------------------------------------------------------------
import websocket                            # WebSocket client for real-time data streaming


# =====================================================================
# Configuration & Constants
# =====================================================================

# Root directory of the project
# __file__ -> current file
# .resolve() -> absolute path
# .parents[2] -> go up 2 levels (adjust based on project structure)
ROOT_DIR = Path(__file__).resolve().parents[2]

# --- Secrets (sensitive credentials & certificates) ---
COINBASE_API_KEY_PATH = ROOT_DIR / "secrets" / "cb_api_key.key"               # Coinbase API key (public identifier)
COINBASE_SECRET_KEY_PATH = ROOT_DIR / "secrets" / "cb_secret_key.pem"         # Coinbase private key (used for JWT signing)
KAFKA_CA_PATH = ROOT_DIR / "secrets" / "kafka_ca.pem"                         # CA cert to verify Kafka broker
KAFKA_SERVICE_CERT_PATH = ROOT_DIR / "secrets" / "kafka_service.cert"         # Client SSL certificate for Kafka auth
KAFKA_SERVICE_KEY_PATH = ROOT_DIR / "secrets" / "kafka_service.key"           # Client private key for Kafka SSL

# --- Application configuration ---
CONFIG_PATH = ROOT_DIR / "src" / "config.json"  # Main config file (products, channels, etc.)

KAFKA_SERVER_URL = os.environ.get("KAFKA_SERVER_URL")                         # Kafka cluster address
COINBASE_WEBSOCKET_URL = os.environ.get("COINBASE_WEBSOCKET_URL")             # Coinbase advanced-trade websocket

# Performance / reliability tuning
SO_RCVBUF_BYTES = int(os.environ.get("SO_RCVBUF_BYTES", 4 * 1024 * 1024))     # Socket receive buffer size (4MB default)
MESSAGE_QUEUE_MAXSIZE = int(os.environ.get("MESSAGE_QUEUE_MAXSIZE", 50_000))  # Max queue size to prevent memory overflow
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", 2))                         # Number of Kafka producer worker threads
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 500))                           # Messages per batch sent to Kafka
LINGER_MS = int(os.environ.get("LINGER_MS", 50))                              # Wait time to accumulate batch (ms)
KAFKA_BATCH_BYTES = int(os.environ.get("KAFKA_BATCH_BYTES", 256 * 1024))      # Max Kafka batch size in bytes (256KB)
KAFKA_RETRIES = int(os.environ.get("KAFKA_RETRIES", 5))                       # Retry attempts on Kafka send failure
QUEUE_PUT_TIMEOUT = int(os.environ.get("QUEUE_PUT_TIMEOUT", 0.005))           # Timeout when enqueueing messages (seconds)
RETRY_BACKOFF = int(os.environ.get("RETRY_BACKOFF", 1000))                    # Delay between retries (ms)
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", 30000))               # Kafka request timeout (ms)


# =====================================================================
# Logging
# =====================================================================

# Enable debug logs via environment variable (set DEBUG_LOGS=1 to enable)
DEBUG_LOGS = os.environ.get("DEBUG_LOGS", "0") == "1"

# Set log level dynamically based on debug flag
LOG_LEVEL = logging.DEBUG if DEBUG_LOGS else logging.INFO

# Define a consistent log format:
# - timestamp
# - log level
# - thread name (useful since we have multiple threads)
# - actual log message
formatter = logging.Formatter(
    "%(asctime)s level=%(levelname)s thread=%(threadName)s event=%(message)s"
)

# Create console handler to output logs to stdout
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

# Create main application logger
logger = logging.getLogger("cb-kafka-bridge")
logger.setLevel(LOG_LEVEL)

# Avoid adding duplicate handlers if logger is already configured
if not logger.handlers:
    logger.addHandler(console_handler)

# Reduce verbosity of Kafka internal logs (only show warnings and errors)
logging.getLogger("kafka").setLevel(logging.WARNING)


# =====================================================================
# Load Config
# =====================================================================

# --- Load Coinbase API credentials ---
try:
    # Public API key (used for JWT generation)
    with open(COINBASE_API_KEY_PATH, "r") as f:
        COINBASE_API_KEY = f.read().strip()

    # Private key (PEM format, used for signing JWT)
    with open(COINBASE_SECRET_KEY_PATH, "r") as f:
        COINBASE_SECRET_PEM = f.read()

except FileNotFoundError:
    # Fail fast if required credentials are missing
    logger.exception("startup_failed missing_coinbase_secrets")
    sys.exit(1)


# --- Load application config (products + channels) ---
try:
    with open(CONFIG_PATH, "r") as cfg:
        CONFIG = json.load(cfg)

    # Extract enabled products only
    # Each product must have "symbol" and optional "enabled" flag
    PRODUCTS = [
        product["symbol"]
        for product in CONFIG["products"]
        if product.get("enabled", False)
    ]

    # Extract all channel names (keys of "channels" dict)
    CHANNELS = list(CONFIG["channels"].keys())


# --- Error handling for config loading ---
except FileNotFoundError:
    # Config file not found at expected path
    logger.exception("startup_failed config_not_found")
    sys.exit(1)

except json.JSONDecodeError:
    # Config file exists but contains invalid JSON
    logger.exception("startup_failed invalid_config_json")
    sys.exit(1)

except KeyError:
    # Expected keys like "products" or "channels" missing
    logger.exception("startup_failed config_format_invalid")
    sys.exit(1)


# =====================================================================
# Kafka Producer & Utilities
# =====================================================================

# Kafka server URL must be provided via environment variable
if not KAFKA_SERVER_URL:
    logger.error("startup_failed kafka_url_missing")
    sys.exit(1)

producer_conf = {
    "bootstrap.servers": KAFKA_SERVER_URL,                     # Kafka broker(s) to connect to
    "security.protocol": "SSL",                                # Use SSL for secure communication
    "ssl.ca.location": str(KAFKA_CA_PATH),                     # CA certificate to verify broker
    "ssl.certificate.location": str(KAFKA_SERVICE_CERT_PATH),  # Client certificate
    "ssl.key.location": str(KAFKA_SERVICE_KEY_PATH),           # Client private key

    "acks": "all",                                             # Wait for all replicas to acknowledge (strong durability)
    "retries": KAFKA_RETRIES,                                  # Number of retry attempts on failure
    "enable.idempotence": True,                                # Prevent duplicate messages on retries

    "retry.backoff.ms": RETRY_BACKOFF,                         # Delay between retry attempts (ms)
    "request.timeout.ms": REQUEST_TIMEOUT,                     # Max time Kafka waits for broker response (ms)

    "linger.ms": LINGER_MS,                                    # Wait time to batch messages before sending (improves throughput)
    "batch.size": KAFKA_BATCH_BYTES,                           # Max size of a batch in bytes (controls batching efficiency)
}


# Initialize Kafka producer
try:
    producer = Producer(producer_conf)
except Exception:
    # Fail fast if producer cannot be created (misconfig / SSL issues / broker unreachable)
    logger.exception("startup_failed kafka_producer_init")
    sys.exit(1)


# Thread-safe queue used to pass messages from WebSocket threads -> Kafka workers
# Tuple format: (topic, product, raw_message, event_timestamp_ms)
msg_queue: "queue.Queue[Tuple[str, str, str, int]]" = queue.Queue(
    maxsize=MESSAGE_QUEUE_MAXSIZE  # Prevent unbounded memory growth under load
)

# Counter for messages dropped when queue is full (backpressure indicator)
dropped_messages = 0

# Lock to safely update dropped_messages across threads
dropped_lock = threading.Lock()

# Track active WebSocket clients (one per product)
ws_clients: "dict[str, websocket.WebSocketApp]" = {}

# Lock to safely access/modify ws_clients across threads
ws_clients_lock = threading.Lock()

# Event used to signal graceful shutdown to all threads
stop_event = threading.Event()


# =====================================================================
# Core Functions
# =====================================================================

def kafka_worker(worker_id: int) -> None:
    """
    Consume messages from the shared queue, batch them, and send to Kafka.

    This function runs in a dedicated thread and continuously pulls messages
    from `msg_queue`, groups them into batches, and forwards them to Kafka
    using `_drain_batch()`.

    Parameters
    ----------
    worker_id : int
        Unique identifier for the worker thread (used for logging/debugging).

    Returns
    -------
    None

    Notes
    -----
    - Uses both size-based and time-based batching for efficiency.
    - Ensures low latency by limiting queue wait time (`timeout=0.2`).
    - Flush conditions:
        * Batch size reaches `BATCH_SIZE`
        * Time since last flush exceeds `LINGER_MS`
        * Shutdown signal received (flush remaining messages)
    - Gracefully exits only when:
        * Shutdown signal is set
        * Queue is empty
        * No pending messages in batch
    """

    logger.info("worker_started id=%s", worker_id)

    # Batch buffer:
    # Each item = (topic, product, raw_message, event_timestamp_ms)
    batch: list[tuple[str, str, str, int]] = []

    # Track last batch flush time (monotonic avoids system clock issues)
    last_flush: float = time.monotonic()

    # Convert linger time from milliseconds to seconds
    linger_seconds: float = LINGER_MS / 1000.0

    # Main processing loop
    while True:

        # Exit condition:
        # Stop only when:
        # - Shutdown signal is set
        # - Queue is empty
        # - No pending messages in batch
        if stop_event.is_set() and msg_queue.empty() and not batch:
            break

        # Attempt to fetch one message from queue
        # Small timeout keeps thread responsive (important for shutdown)
        try:
            item: tuple[str, str, str, int] = msg_queue.get(timeout=0.2)
            batch.append(item)

        except queue.Empty:
            # No message available → continue loop
            pass

        # Current time for evaluating flush conditions
        now: float = time.monotonic()

        # Determine whether batch should be flushed
        should_flush: bool = (
            batch  # Only flush if batch is not empty
            and (
                len(batch) >= BATCH_SIZE                         # Size-based flush
                or (now - last_flush) >= linger_seconds          # Time-based flush
                or (stop_event.is_set() and msg_queue.empty())   # Shutdown flush
            )
        )

        # Flush batch to Kafka if required
        if should_flush:
            logger.debug(
                "batch_flush worker_id=%s batch_size=%d",
                worker_id,
                len(batch),
            )

            _drain_batch(batch)   # Send messages to Kafka
            batch.clear()         # Reset batch buffer
            last_flush = now      # Update last flush timestamp

    # Final flush (safety net in case loop exits with pending data)
    if batch:
        _drain_batch(batch)

    logger.info("worker_stopped id=%s", worker_id)


def make_callbacks(product: str):
    """
    Create WebSocket callback functions bound to a specific product.

    This factory function returns a set of callbacks (`on_open`, `on_message`,
    `on_error`, `on_close`) tailored for a given product. Each WebSocket
    connection uses its own set of callbacks.

    Parameters
    ----------
    product : str
        The product symbol (e.g., BTC-USD) associated with the WebSocket.

    Returns
    -------
    tuple
        (on_open, on_message, on_error, on_close) callback functions.

    Notes
    -----
    - Designed to keep WebSocket callbacks lightweight and non-blocking.
    - Heavy processing is avoided here; messages are quickly enqueued.
    - Uses a shared queue (`msg_queue`) for communication with Kafka workers.
    - Implements drop-on-overflow strategy when queue is full.
    """

    # Called when WebSocket connection is established
    def on_open(ws) -> None:
        try:
            logger.info("ws_connected product=%s", product)

            # Subscribe to configured channels for this product
            _send_subscription_message(ws, product)

        except Exception:
            logger.exception("ws_subscription_error product=%s", product)

    # Called when a message is received from WebSocket
    def on_message(ws, raw_message) -> None:
        """
        Minimal processing:
        - Decode message
        - Extract channel + timestamp
        - Push raw payload to queue
        """

        global dropped_messages

        try:
            # Decode message if received as bytes
            if isinstance(raw_message, bytes):
                text = raw_message.decode("utf-8")
            else:
                text = raw_message

            # Extract routing info
            channel = _fast_extract_channel(text)
            topic = _sanitize_topic(str(channel))

            # Extract event timestamp
            ts_str = _fast_extract_timestamp(text)
            event_ts_ms = _to_epoch_ms(ts_str)

            # Enqueue message for Kafka workers
            # Use small timeout to avoid blocking WebSocket thread
            try:
                msg_queue.put(
                    (topic, product, text, event_ts_ms),
                    timeout=QUEUE_PUT_TIMEOUT
                )

            except queue.Full:
                # Queue is full → drop message (backpressure handling)
                # Log periodically to avoid log flooding
                with dropped_lock:
                    dropped_messages += 1
                    if dropped_messages % 1000 == 0:
                        logger.warning("queue_dropped total=%d", dropped_messages)

        except Exception:
            logger.exception("ws_message_error product=%s", product)

    # Called when WebSocket encounters an error
    def on_error(ws, err) -> None:
        logger.error("ws_error product=%s error=%s", product, err)

    # Called when WebSocket connection is closed
    def on_close(ws, close_status_code, close_msg) -> None:
        logger.info(
            "ws_closed product=%s code=%s",
            product,
            close_status_code,
        )

    # Return all callbacks as a tuple
    return on_open, on_message, on_error, on_close


def run_ws_for_product(product: str) -> None:
    """
    Start and maintain a WebSocket connection for a given product.

    This function runs in its own thread and is responsible for:
    - Creating a WebSocket connection
    - Attaching product-specific callbacks
    - Maintaining a persistent connection with auto-reconnect
    - Registering the connection for graceful shutdown handling

    Parameters
    ----------
    product : str
        The product symbol (e.g., BTC-USD) to subscribe to.

    Returns
    -------
    None

    Notes
    -----
    - Runs an infinite loop until `stop_event` is set.
    - Automatically reconnects on failure with a small backoff.
    - Uses socket and SSL options for performance and security.
    - Registers WebSocket instance in `ws_clients` for coordinated shutdown.
    """

    # Create product-specific callbacks (lightweight handlers)
    on_open, on_message, on_error, on_close = make_callbacks(product)

    # Initialize WebSocket client
    ws = websocket.WebSocketApp(
        COINBASE_WEBSOCKET_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # Register this WebSocket instance so it can be closed during shutdown
    with ws_clients_lock:
        ws_clients[product] = ws

    # Socket options:
    # Increase receive buffer size to handle high-throughput streams
    sockopts = (
        (socket.SOL_SOCKET, socket.SO_RCVBUF, SO_RCVBUF_BYTES),
    )

    # SSL options:
    # Require valid certificates for secure connection
    sslopt = {"cert_reqs": ssl.CERT_REQUIRED}

    # Connection loop (auto-reconnect)
    try:
        while not stop_event.is_set():
            try:
                # Start WebSocket event loop (blocking call)
                ws.run_forever(
                    sockopt=sockopts,
                    sslopt=sslopt,
                    ping_interval=20,   # send ping every 20s to keep connection alive
                    ping_timeout=10,    # wait 10s for pong before considering connection dead
                )

            except Exception:
                # Log and retry on unexpected failure
                logger.warning("ws_reconnect product=%s", product)
                time.sleep(5)

            # Small delay before reconnect attempt (prevents tight retry loop)
            time.sleep(1)

    # Cleanup on shutdown
    finally:
        # Remove reference so shutdown logic doesn't try to close it again
        with ws_clients_lock:
            ws_clients.pop(product, None)

        logger.info("ws_thread_stopped product=%s", product)


def start_workers_and_ws() -> tuple[list[threading.Thread], list[threading.Thread]]:
    """
    Start Kafka worker threads and WebSocket threads for all products.

    This function initializes:
    - A pool of Kafka worker threads (for message batching and sending)
    - One WebSocket thread per product (for real-time data ingestion)

    Returns
    -------
    tuple
        (
            workers: list of Kafka worker threads,
            ws_threads: list of WebSocket threads (one per product)
        )

    Notes
    -----
    - Kafka workers consume from the shared queue (`msg_queue`).
    - Each WebSocket thread independently connects to Coinbase for a product.
    - Threads are non-daemon to ensure graceful shutdown handling.
    """

    # Start Kafka worker threads
    workers: list[threading.Thread] = []

    for i in range(WORKER_COUNT):
        t = threading.Thread(
            target=kafka_worker,         # Worker function
            args=(i,),                   # Pass worker ID
            name=f"kafka-worker-{i}",    # Helpful for logging/debugging
            daemon=False,                # Ensure controlled shutdown
        )

        t.start()
        workers.append(t)

    # Start WebSocket threads (one per product)
    ws_threads: list[threading.Thread] = []

    for product in PRODUCTS:
        t = threading.Thread(
            target=run_ws_for_product,   # WebSocket runner
            args=(product,),             # Pass product symbol
            name=f"ws-{product}",        # Thread name for logs
            daemon=False,                # Ensure proper lifecycle management
        )

        t.start()
        ws_threads.append(t)

    # Return thread references for lifecycle management (join/shutdown)
    return workers, ws_threads


def shutdown(signum=None, frame=None) -> None:
    """
    Gracefully shut down WebSocket connections, worker threads, and Kafka producer.

    This function is typically triggered by OS signals (SIGINT, SIGTERM) and ensures:
    - All WebSocket connections are closed
    - Worker threads stop consuming new messages
    - Kafka producer flushes pending messages before closing

    Parameters
    ----------
    signum : int, optional
        Signal number that triggered shutdown (e.g., SIGINT, SIGTERM).
    frame : optional
        Current stack frame (unused, required for signal handler signature).

    Returns
    -------
    None

    Notes
    -----
    - Closing WebSockets causes `run_forever()` to exit in their threads.
    - `stop_event` signals worker threads to finish processing and exit.
    - Kafka producer is flushed with a timeout to avoid indefinite blocking.
    """

    logger.info("shutdown_requested signal=%s", signum)

    # Copy items to avoid mutation issues while iterating
    with ws_clients_lock:
        items = list(ws_clients.items())

    for product, ws in items:
        try:
            logger.info("ws_closing product=%s", product)
            ws.close()  # Triggers run_forever() to exit
        except Exception:
            logger.exception("ws_close_error product=%s", product)

    # Signal worker threads to stop
    stop_event.set()

    # Flush pending messages (timeout = 5 seconds)
    try:
        producer.flush(5)
    except Exception:
        logger.exception("producer_flush_error")

    # Close producer (release resources, connections)
    try:
        producer.close()
    except Exception:
        logger.exception("producer_close_error")


# =====================================================================
# Helpers
# =====================================================================

def _build_jwt() -> str:
    """
    Generate a JWT token for authenticating with Coinbase WebSocket.

    This token is required when subscribing to channels on the
    Coinbase Advanced Trade WebSocket API.

    Returns
    -------
    str
        Signed JWT token.

    Notes
    -----
    - Uses API key and private key loaded at startup.
    - Delegates actual JWT creation to Coinbase SDK (`jwt_generator`).
    """

    return jwt_generator.build_ws_jwt(
        COINBASE_API_KEY,
        COINBASE_SECRET_PEM,
    )


def _delivery_report(err, msg) -> None:
    """
    Kafka delivery callback to log message delivery status.

    This function is invoked by the Kafka producer when a message
    is successfully delivered or if delivery fails.

    Parameters
    ----------
    err : Exception or None
        Error information if delivery failed, otherwise None.
    msg : Message
        Kafka message object containing metadata (topic, partition, offset).

    Returns
    -------
    None

    Notes
    -----
    - Runs asynchronously via `producer.poll()`.
    - Success logs are kept at DEBUG level to avoid log noise.
    - Failures are logged at ERROR level for visibility.
    """

    # Delivery failed
    if err is not None:
        logger.error(
            "kafka_delivery_failed topic=%s error=%s",
            msg.topic(),
            err,
        )

    # Delivery succeeded
    else:
        logger.debug(
            "kafka_delivered topic=%s partition=%s offset=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def _sanitize_topic(channel: str) -> str:
    """
    Convert a WebSocket channel name into a Kafka topic name.

    Parameters
    ----------
    channel : str
        Channel name extracted from WebSocket message (e.g., "ticker", "level2").

    Returns
    -------
    str
        Normalized Kafka topic name (e.g., "coinbase.ticker").

    Notes
    -----
    - Replaces '-' with '_' to ensure valid topic naming.
    - Converts to lowercase for consistency.
    - Prefixes with 'coinbase.' to namespace topics.
    """

    c: str = channel.replace("-", "_").lower()
    return f"coinbase.{c}"


def _serialize_key(key) -> bytes:
    """
    Serialize Kafka message key into bytes.

    Parameters
    ----------
    key : Any
        Message key (typically product symbol).

    Returns
    -------
    bytes
        UTF-8 encoded key if input is string, otherwise unchanged.

    Notes
    -----
    - Kafka expects keys as bytes.
    - Non-string keys are assumed to already be in correct format.
    """

    return key.encode("utf-8") if isinstance(key, str) else key


def _serialize_value(value) -> bytes:
    """
    Serialize Kafka message value into bytes.

    Parameters
    ----------
    value : Any
        Message payload (string, dict, or already serialized).

    Returns
    -------
    bytes
        UTF-8 encoded representation of the value.

    Notes
    -----
    - Strings are encoded directly.
    - Dictionaries are converted to JSON before encoding.
    - Other types are returned as-is (assumed pre-serialized).
    """

    if isinstance(value, str):
        return value.encode("utf-8")

    elif isinstance(value, dict):
        return json.dumps(value).encode("utf-8")

    return value


def _produce_with_retry(
        topic: str,
        product: str,
        msg: str,
        event_ts_ms: int
    ) -> bool:
    """
    Produce a message to Kafka with retry handling.

    Attempts to send a message to Kafka up to a fixed number of retries.
    Handles common failure scenarios like full internal buffer and Kafka errors.

    Parameters
    ----------
    topic : str
        Kafka topic to send the message to.
    product : str
        Message key (typically product symbol).
    msg : str
        Raw message payload.
    event_ts_ms : int
        Event timestamp in milliseconds (used for Kafka record timestamp).

    Returns
    -------
    bool
        True if message was successfully queued for delivery, False otherwise.

    Notes
    -----
    - Retries are limited (currently 3 attempts).
    - `BufferError` occurs when Kafka producer queue is full.
    - `producer.poll()` is required to:
        * trigger delivery callbacks
        * free up internal buffer space
    - Non-retryable errors immediately return False.
    """

    # Retry loop
    for _ in range(3):
        try:
            # Attempt to produce message to Kafka
            producer.produce(
                topic=topic,
                key=_serialize_key(product),
                value=_serialize_value(msg),
                timestamp=event_ts_ms,
                on_delivery=_delivery_report,
            )

            # Trigger delivery callbacks and internal housekeeping
            producer.poll(0)

            return True

        # Kafka internal buffer is full → wait briefly and retry
        except BufferError:
            producer.poll(0.1)

        # Kafka-specific failure (non-retryable)
        except KafkaException as ke:
            logger.error(
                "kafka_produce_failed topic=%s product=%s error=%s",
                topic,
                product,
                ke,
            )
            return False

        # Unexpected failure
        except Exception:
            logger.exception(
                "unexpected_kafka_produce_error topic=%s product=%s",
                topic,
                product,
            )
            return False

    # All retries exhausted → drop message
    logger.warning(
        "kafka_drop_after_retries topic=%s product=%s",
        topic,
        product,
    )

    return False


def _drain_batch(batch: list[tuple[str, str, str, int]]) -> None:
    """
    Send all messages in a batch to Kafka.

    Parameters
    ----------
    batch : list of tuples
        Each item = (topic, product, message, timestamp_ms)

    Returns
    -------
    None

    Notes
    -----
    - Delegates actual send + retry logic to `_produce_with_retry`.
    - Processes messages sequentially (order preserved within batch).
    """

    for topic, product, msg, epoch in batch:
        _produce_with_retry(topic, product, msg, epoch)


def _send_subscription_message(ws, product: str) -> None:
    """
    Send subscription messages for all configured channels.

    Parameters
    ----------
    ws : WebSocketApp
        Active WebSocket connection.
    product : str
        Product symbol to subscribe to.

    Returns
    -------
    None

    Notes
    -----
    - Uses JWT authentication for each subscription.
    - Sends one message per channel.
    """

    # Generate authentication token
    token: str = _build_jwt()

    for channel in CHANNELS:
        msg = {
            "type": "subscribe",
            "channel": channel,
            "product_ids": [product],
            "jwt": token,
        }

        ws.send(json.dumps(msg))

        logger.debug(
            "subscription_sent product=%s channel=%s",
            product,
            channel,
        )


def _fast_extract_channel(msg: str) -> str:
    """
    Extract 'channel' field from raw JSON string (fast path).

    Parameters
    ----------
    msg : str
        Raw JSON message string.

    Returns
    -------
    str
        Extracted channel name or "unknown" if not found.

    Notes
    -----
    - Avoids full JSON parsing for performance.
    - Assumes consistent message format.
    """

    i = msg.find('"channel":"')
    if i == -1:
        return "unknown"

    start = i + 11
    end = msg.find('"', start)

    return msg[start:end]


def _fast_extract_timestamp(msg: str) -> str | None:
    """
    Extract 'timestamp' field from raw JSON string (fast path).

    Parameters
    ----------
    msg : str
        Raw JSON message string.

    Returns
    -------
    str or None
        Timestamp string if found, else None.

    Notes
    -----
    - Designed for speed (no JSON parsing).
    - Caller must handle None case.
    """

    i = msg.find('"timestamp":"')
    if i == -1:
        return None

    start = i + 13
    end = msg.find('"', start)

    return msg[start:end]


def _to_epoch_ms(ts_str: str) -> int:
    """
    Convert ISO8601 timestamp string to epoch milliseconds.

    Parameters
    ----------
    ts_str : str
        Timestamp string (e.g., "2024-01-01T12:34:56.789Z").

    Returns
    -------
    int
        Epoch time in milliseconds.

    Notes
    -----
    - Uses manual parsing for performance (avoids datetime overhead).
    - Handles fractional seconds (milliseconds).
    - Falls back to current time if parsing fails.
    """

    try:
        # Extract date components
        year = int(ts_str[0:4])
        month = int(ts_str[5:7])
        day = int(ts_str[8:10])

        # Extract time components
        hour = int(ts_str[11:13])
        minute = int(ts_str[14:16])
        second = int(ts_str[17:19])

        # Extract milliseconds (if present)
        ms = 0
        if '.' in ts_str:
            frac = ts_str[20:]
            frac = frac.replace('Z', '').split('+')[0]
            ms = int(frac[:3].ljust(3, '0'))  # ensure 3-digit ms

        # Convert to epoch seconds (UTC)
        epoch_sec = calendar.timegm((
            year, month, day,
            hour, minute, second
        ))

        return epoch_sec * 1000 + ms

    # Fallback: return current time if parsing fails
    except Exception:
        return int(time.time() * 1000)


# =====================================================================
# Core Logic
# =====================================================================

def main() -> None:
    """
    Script entry point.

    Initializes signal handlers, starts worker and WebSocket threads,
    and keeps the application running until a shutdown signal is received.

    Returns
    -------
    None

    Notes
    -----
    - Handles graceful shutdown via SIGINT (Ctrl+C) and SIGTERM.
    - Uses `stop_event` to coordinate shutdown across threads.
    - Ensures all threads are joined before exiting.
    - Performs a final Kafka flush/close as a safety measure.
    """

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, shutdown)   # Ctrl+C
    signal.signal(signal.SIGTERM, shutdown)  # Termination signal

    logger.info("startup products=%s", PRODUCTS)

    # Start worker threads and WebSocket threads
    workers, ws_threads = start_workers_and_ws()

    # Main loop (wait for shutdown signal)
    try:
        # Wait until stop_event is set by signal handler or programmatic call
        while not stop_event.is_set():
            stop_event.wait(timeout=1)
    except KeyboardInterrupt:
        # Fallback: handle Ctrl+C if signal not captured
        shutdown()

    # Wait for threads to finish
    for t in ws_threads:
        t.join(timeout=5)

    for t in workers:
        t.join(timeout=5)

    # Shutdown already attempts this, but we retry to be safe
    try:
        producer.flush(5)
    except Exception:
        logger.exception("final_flush_error")

    try:
        producer.close()
    except Exception:
        logger.exception("final_close_error")

    # Log shutdown summary
    logger.info("shutdown_complete dropped_messages=%d", dropped_messages)


if __name__ == "__main__":
    main()
