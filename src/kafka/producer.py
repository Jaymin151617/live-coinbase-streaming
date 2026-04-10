#!/usr/bin/env python3
"""
Production-ready Coinbase Advanced Trade -> Kafka bridge.

Confluent Kafka version.

- One websocket connection per product (separate threads).
- Minimal work in websocket callbacks: parse & enqueue only.
- Worker threads batch and send to Kafka.
- Bounded queue provides backpressure; drop-on-overflow policy (configurable).
- Large socket recv buffer configured via sockopt.
"""

import os
import json
import logging
import queue
import signal
import ssl
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Tuple
import calendar
from logging.handlers import TimedRotatingFileHandler

from coinbase import jwt_generator
from confluent_kafka import Producer, KafkaException
import websocket

# Paths
ROOT_DIR = Path(__file__).resolve().parents[2]

COINBASE_API_KEY_PATH = ROOT_DIR / "secrets" / "cb_api_key.key"
COINBASE_SECRET_KEY_PATH = ROOT_DIR / "secrets" / "cb_secret_key.pem"
KAFKA_CA_PATH = ROOT_DIR / "secrets" / "kafka_ca.pem"
KAFKA_SERVICE_CERT_PATH = ROOT_DIR / "secrets" / "kafka_service.cert"
KAFKA_SERVICE_KEY_PATH = ROOT_DIR / "secrets" / "kafka_service.key"
CONFIG_PATH = ROOT_DIR / "src" / "config.json"

# Kafka cluster address
KAFKA_SERVER_URL = os.environ.get("KAFKA_SERVER_URL")
COINBASE_WEBSOCKET_URL = os.environ.get("COINBASE_WEBSOCKET_URL")

# Performance / reliability tuning
SO_RCVBUF_BYTES = int(os.environ.get("SO_RCVBUF_BYTES", 4 * 1024 * 1024))         # 4MB receive buffer
MESSAGE_QUEUE_MAXSIZE = int(os.environ.get("MESSAGE_QUEUE_MAXSIZE", 50_000))      # bounded queue for backpressure
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", 2))                             # number of kafka sender threads
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 500))                               # messages per batch (worker)
LINGER_MS = int(os.environ.get("LINGER_MS", 50))                                  # ms to wait for a batch before send
KAFKA_BATCH_BYTES = int(os.environ.get("KAFKA_BATCH_BYTES", 256 * 1024))          # broker-side batch size hint (bytes)
KAFKA_RETRIES = int(os.environ.get("KAFKA_RETRIES", 5))
QUEUE_PUT_TIMEOUT = int(os.environ.get("QUEUE_PUT_TIMEOUT", 0.005))               # seconds to attempt enqueue
RETRY_BACKOFF = int(os.environ.get("RETRY_BACKOFF", 1000))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", 30000))

# Logging
(ROOT_DIR / "logs").mkdir(parents=True, exist_ok=True)

log_file = ROOT_DIR / "logs" / "producer.log"

# Create rotating handler (weekly rotation)
file_handler = TimedRotatingFileHandler(
    filename=log_file,
    when="W0",          # rotate every Monday
    interval=1,
    backupCount=4,      # keep last 4 weeks
    encoding="utf-8"
)

# Format
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(threadName)s %(message)s"
)
file_handler.setFormatter(formatter)

# Optional: also log to console (recommended for Docker)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Root logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Avoid duplicate handlers if reloaded
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# Kafka logs level
logging.getLogger("kafka").setLevel(logging.INFO)

# Your app logger
logger = logging.getLogger("cb-kafka-bridge")

# --- Load secrets ---
try:
    with open(COINBASE_API_KEY_PATH, "r") as f:
        COINBASE_API_KEY = f.read().strip()
    with open(COINBASE_SECRET_KEY_PATH, "r") as f:
        COINBASE_SECRET_PEM = f.read()
except FileNotFoundError:
    logger.exception("Coinbase API key / secret not found; ensure secret files exist.")
    sys.exit(1)

if not KAFKA_SERVER_URL:
    logger.error("KAFKA_SERVER_URL environment variable is not set.")
    sys.exit(1)

# --- Configuration ---
try:
    with open(CONFIG_PATH, "r") as cfg:
        CONFIG = json.load(cfg)

    PRODUCTS = [
        product["symbol"]
        for product in CONFIG["products"]
        if product.get("enabled", False)
    ]

    CHANNELS = list(CONFIG["channels"].keys())

except FileNotFoundError:
    logger.exception("Config file not found.")
    sys.exit(1)

except json.JSONDecodeError:
    logger.exception("Invalid JSON in config file.")
    sys.exit(1)

except KeyError:
    logger.exception("Config file in wrong format.")
    sys.exit(1)


# --- JWT builder ---
def build_jwt():
    return jwt_generator.build_ws_jwt(COINBASE_API_KEY, COINBASE_SECRET_PEM)


# --- Confluent Kafka producer ---
def delivery_report(err, msg):
    if err is not None:
        logger.error(
            "Delivery failed topic=%s key=%s error=%s",
            msg.topic(),
            msg.key().decode("utf-8", errors="replace") if msg.key() else None,
            err,
        )
    else:
        logger.debug(
            "Delivered topic=%s partition=%s offset=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


producer_conf = {
    "bootstrap.servers": KAFKA_SERVER_URL,
    "security.protocol": "SSL",
    "ssl.ca.location": str(KAFKA_CA_PATH),
    "ssl.certificate.location": str(KAFKA_SERVICE_CERT_PATH),
    "ssl.key.location": str(KAFKA_SERVICE_KEY_PATH),
    "acks": "all",
    "retries": KAFKA_RETRIES,
    "enable.idempotence": True,
    "retry.backoff.ms": RETRY_BACKOFF,
    "request.timeout.ms": REQUEST_TIMEOUT,
    "linger.ms": LINGER_MS,
    "batch.size": KAFKA_BATCH_BYTES,
}

try:
    producer = Producer(producer_conf)
except Exception:
    logger.exception("Failed to initialize Confluent Kafka producer")
    sys.exit(1)


# Shared queue for messages from all websocket threads
msg_queue: "queue.Queue[Tuple[str, str, str, int]]" = queue.Queue(maxsize=MESSAGE_QUEUE_MAXSIZE)

# A simple counter for dropped messages
dropped_messages = 0
dropped_lock = threading.Lock()

# Graceful shutdown event
stop_event = threading.Event()

ws_clients: "dict[str, websocket.WebSocketApp]" = {}
ws_clients_lock = threading.Lock()


def _sanitize_topic(channel: str) -> str:
    c = channel.replace("-", "_").lower()
    return f"coinbase.{c}"


def serialize_key(key):
    return key.encode("utf-8") if isinstance(key, str) else key


def serialize_value(value):
    if isinstance(value, str):
        return value.encode("utf-8")
    elif isinstance(value, dict):
        return json.dumps(value).encode("utf-8")
    return value


def _produce_with_retry(topic: str, product: str, msg: str, event_ts_ms: int) -> bool:

    for _ in range(3):
        try:
            producer.produce(
                topic=topic,
                key=serialize_key(product),
                value=serialize_value(msg),
                timestamp=event_ts_ms,
                on_delivery=delivery_report,
            )
            producer.poll(0)  # serve delivery callbacks
            return True
        except BufferError:
            producer.poll(0.1)
        except KafkaException as ke:
            logger.error("Kafka exception while producing topic=%s product=%s: %s", topic, product, ke)
            return False
        except Exception:
            logger.exception("Unexpected error while producing topic=%s product=%s", topic, product)
            return False

    logger.warning("Dropping message after producer queue stayed full topic=%s product=%s", topic, product)
    return False


def _drain_batch(batch):
    for topic, product, msg, epoch in batch:
        _produce_with_retry(topic, product, msg, epoch)


def kafka_worker(worker_id: int):
    logger.info("Kafka worker %s starting", worker_id)

    batch = []
    last_flush = time.monotonic()
    linger_seconds = LINGER_MS / 1000.0

    while True:
        if stop_event.is_set() and msg_queue.empty() and not batch:
            break

        # Try to fetch one item (keeps callback responsive)
        try:
            item = msg_queue.get(timeout=0.2)
            batch.append(item)
        except queue.Empty:
            pass

        now = time.monotonic()
        should_flush = (
            batch
            and (
                len(batch) >= BATCH_SIZE
                or (now - last_flush) >= linger_seconds
                or (stop_event.is_set() and msg_queue.empty())
            )
        )

        if should_flush:
            _drain_batch(batch)
            batch.clear()
            last_flush = now

    if batch:
        _drain_batch(batch)

    logger.info("Kafka worker %s exiting", worker_id)


def send_subscription_message(ws, product):
    token = build_jwt()
    for channel in CHANNELS:
        msg = {
            "type": "subscribe",
            "channel": channel,
            "product_ids": [product],
            "jwt": token,
        }
        ws.send(json.dumps(msg))
        logger.info("Subscribed %s channel for %s", channel, product)


def fast_extract_channel(msg: str):
    i = msg.find('"channel":"')
    if i == -1:
        return "unknown"
    start = i + 11
    end = msg.find('"', start)
    return msg[start:end]

def fast_extract_timestamp(msg: str):
    i = msg.find('"timestamp":"')
    if i == -1:
        return None
    start = i + 13
    end = msg.find('"', start)
    return msg[start:end]

def to_epoch_ms(ts_str: str):
    try:
        year = int(ts_str[0:4])
        month = int(ts_str[5:7])
        day = int(ts_str[8:10])

        hour = int(ts_str[11:13])
        minute = int(ts_str[14:16])
        second = int(ts_str[17:19])

        # Extract milliseconds
        ms = 0
        if '.' in ts_str:
            frac = ts_str[20:]
            frac = frac.replace('Z', '').split('+')[0]
            ms = int(frac[:3].ljust(3, '0'))

        epoch_sec = calendar.timegm((
            year, month, day,
            hour, minute, second
        ))

        return epoch_sec * 1000 + ms

    except Exception:
        return int(time.time() * 1000)

# --- WebSocket callbacks factory (per-product) ---
def make_callbacks(product: str):
    """Return callbacks bound to a specific product to keep per-ws minimal and fast."""
    def on_open(ws):
        try:
            send_subscription_message(ws, product)
        except Exception:
            logger.exception("Error sending subscribe message for product %s", product)

    def on_message(ws, raw_message):
        # Keep this callback minimal: parse enough to determine topic and enqueue raw payload
        global dropped_messages
        try:
            if isinstance(raw_message, bytes):
                text = raw_message.decode("utf-8")
            else:
                text = raw_message

            channel = fast_extract_channel(text)
            topic = _sanitize_topic(str(channel))

            ts_str = fast_extract_timestamp(text)
            event_ts_ms = to_epoch_ms(ts_str)

            # Enqueue with non-blocking put and tiny timeout to avoid blocking websocket thread
            try:
                msg_queue.put((topic, product, text, event_ts_ms), timeout=QUEUE_PUT_TIMEOUT)
            except queue.Full:
                # drop message if queue full (log sampling to avoid floods)
                with dropped_lock:
                    dropped_messages += 1
                    if dropped_messages % 1000 == 0:
                        logger.warning(
                            "Dropped %d messages so far due to full queue",
                            dropped_messages,
                        )
        except Exception:
            # Keep websocket thread resilient
            logger.exception("Exception in on_message (product=%s)", product)

    def on_error(ws, err):
        logger.error("Websocket error for %s: %s", product, err)

    def on_close(ws, close_status_code, close_msg):
        logger.info(
            "Websocket closed for %s: code=%s msg=%s",
            product,
            close_status_code,
            close_msg,
        )

    return on_open, on_message, on_error, on_close


def run_ws_for_product(product: str):
    """Start a WebSocketApp for the given product and run it forever (in its own thread)."""
    on_open, on_message, on_error, on_close = make_callbacks(product)
    ws = websocket.WebSocketApp(
        COINBASE_WEBSOCKET_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # register the ws so shutdown can close it
    with ws_clients_lock:
        ws_clients[product] = ws

    sockopts = ((socket.SOL_SOCKET, socket.SO_RCVBUF, SO_RCVBUF_BYTES),)
    sslopt = {"cert_reqs": ssl.CERT_REQUIRED}

    try:
        while not stop_event.is_set():
            try:
                ws.run_forever(
                    sockopt=sockopts,
                    sslopt=sslopt,
                    ping_interval=20,
                    ping_timeout=10,
                )
            except Exception:
                logger.exception(
                    "Websocket run_forever crashed for %s; reconnecting after backoff",
                    product,
                )
                time.sleep(5)
            # small backoff before reconnect
            time.sleep(1)
    finally:
        # make sure we remove the reference so shutdown won't try to close a gone ws
        with ws_clients_lock:
            ws_clients.pop(product, None)
        logger.info("Websocket thread for %s exiting", product)


def start_workers_and_ws():
    workers = []
    for i in range(WORKER_COUNT):
        t = threading.Thread(
            target=kafka_worker,
            args=(i,),
            name=f"kafka-worker-{i}",
            daemon=False,
        )
        t.start()
        workers.append(t)

    ws_threads = []
    for product in PRODUCTS:
        t = threading.Thread(
            target=run_ws_for_product,
            args=(product,),
            name=f"ws-{product}",
            daemon=False,
        )
        t.start()
        ws_threads.append(t)

    return workers, ws_threads


def shutdown(signum=None, frame=None):
    logger.info("Shutdown requested (signal=%s). Closing websockets and stopping workers...", signum)

    # Close websockets (closing them causes run_forever to return)
    with ws_clients_lock:
        items = list(ws_clients.items())

    for product, ws in items:
        try:
            logger.info("Closing websocket for %s", product)
            # WebSocketApp.close() is safe to call from another thread
            ws.close()
        except Exception:
            logger.exception("Error closing websocket for %s", product)

    # Signal workers to stop consuming
    stop_event.set()

    # Try a quick flush/close for producer (don't block forever)
    try:
        producer.flush(5)
    except Exception:
        logger.exception("Producer flush during shutdown failed")

    try:
        producer.close()
    except Exception:
        logger.exception("Producer close failed")


def main():
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info("Starting bridge (products=%s)", PRODUCTS)
    workers, ws_threads = start_workers_and_ws()

    try:
        # Wait until stop_event is set by signal handler or programmatic call
        while not stop_event.is_set():
            stop_event.wait(timeout=1)
    except KeyboardInterrupt:
        # as a fallback (e.g. if signal handler wasn't installed), ensure shutdown
        logger.info("KeyboardInterrupt caught in main; shutting down")
        shutdown()

    logger.info("Waiting for websocket threads to exit...")
    for t in ws_threads:
        t.join(timeout=5)

    logger.info("Waiting for workers to flush remaining messages...")
    for t in workers:
        t.join(timeout=5)

    # final flush/close already attempted in shutdown; attempt again defensively
    try:
        producer.flush(5)
    except Exception:
        logger.exception("Exception flushing producer on shutdown")

    try:
        producer.close()
    except Exception:
        logger.exception("Exception closing producer")

    logger.info("Shutdown complete. Dropped messages: %d", dropped_messages)


if __name__ == "__main__":
    main()
