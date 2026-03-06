#!/usr/bin/env python3
"""
Production-ready Coinbase Advanced Trade -> Kafka bridge.

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
from coinbase import jwt_generator

from kafka import KafkaProducer
import websocket

# --- Configuration ---
PRODUCTS = [
    "BTC-USD",     # Bitcoin
    "ETH-USD",     # Ethereum
    "LINK-USD",    # Chainlink
    "SOL-USD",     # Solana
    "ADA-USD"      # Cardano
]

CHANNELS = [
    "ticker",
    "candles",
    "market_trades",
    # "heartbeats"     # Required in production to verify sequence of messages
]

# Paths
ROOT_DIR = Path(__file__).resolve().parents[2]

COINBASE_API_KEY_PATH = ROOT_DIR / "secrets" / "cb_api_key.key"
COINBASE_SECRET_KEY_PATH = ROOT_DIR / "secrets" / "cb_secret_key.pem"
KAFKA_CA_PATH = ROOT_DIR / "secrets" / "kafka_ca.pem"
KAFKA_SERVICE_CERT_PATH = ROOT_DIR / "secrets" / "kafka_service.cert"
KAFKA_SERVICE_KEY_PATH = ROOT_DIR / "secrets" / "kafka_service.key"

# Kafka cluster address
KAFKA_SERVER_URL = os.environ.get("KAFKA_SERVER_URL")
COINBASE_WEBSOCKET_URL = "wss://advanced-trade-ws.coinbase.com"

# Performance / reliability tuning
SO_RCVBUF_BYTES = 4 * 1024 * 1024   # 4MB receive buffer
MESSAGE_QUEUE_MAXSIZE = 50_000      # bounded queue for backpressure
WORKER_COUNT = 2                    # number of kafka sender threads
BATCH_SIZE = 500                    # messages per batch (worker)
LINGER_MS = 50                      # ms to wait for a batch before send
KAFKA_BATCH_BYTES = 256 * 1024      # broker-side batch size hint (bytes)
KAFKA_RETRIES = 5
QUEUE_PUT_TIMEOUT = 0.005           # seconds to attempt enqueue

# Logging
logging.basicConfig(
    filename=ROOT_DIR / "logs" / "producer.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s %(message)s",
)
# enable kafka-python logs
logging.getLogger("kafka").setLevel(logging.INFO)
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

# --- JWT builder ---
def build_jwt():
    return jwt_generator.build_ws_jwt(COINBASE_API_KEY, COINBASE_SECRET_PEM)

# --- Kafka producer ---
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER_URL,
        security_protocol="SSL",
        ssl_cafile=KAFKA_CA_PATH,
        ssl_certfile=KAFKA_SERVICE_CERT_PATH,
        ssl_keyfile=KAFKA_SERVICE_KEY_PATH,
        acks="all",
        linger_ms=LINGER_MS,
        batch_size=KAFKA_BATCH_BYTES,
        retries=KAFKA_RETRIES,
        value_serializer=lambda v: v if isinstance(v, (bytes, bytearray)) else v.encode("utf-8"),
    )
except Exception:
    logger.exception("Failed to initialize Kafka producer")
    sys.exit(1)


# Shared queue for messages from all websocket threads
msg_queue: "queue.Queue[Tuple[str, str]]" = queue.Queue(maxsize=MESSAGE_QUEUE_MAXSIZE)

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


# --- Worker that reads from msg_queue and sends to Kafka in batches ---
def kafka_worker(worker_id: int):
    """Continuously read from msg_queue and send to Kafka in batches."""
    logger.info(f"Kafka worker {worker_id} starting")

    while True:
        # Exit when stop requested and nothing left to do
        if stop_event.is_set() and msg_queue.empty():
            break

        # Try to fetch one item (keeps callback responsive)
        try:
            item = msg_queue.get(timeout=0.2)
        except queue.Empty:
            # nothing available right now
            continue

        topic, msg = item
        producer.send(topic, msg)

    # Final defensive flush
    producer.flush(timeout=5)

    logger.info(f"Kafka worker {worker_id} exiting")


def send_subscription_message(ws, product):
    token = build_jwt()
    for channel in CHANNELS:
        msg = {
            "type": "subscribe",
            "channel": channel,
            "product_ids": [product],
            "jwt": token
        }
        ws.send(json.dumps(msg))
        logger.info(f"Subscribed {channel} channel for %s", product)


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

            # try a light parse to get channel/product; fallback to bound product
            try:
                d = json.loads(text)
                channel = d.get("channel")
            except Exception:
                # if parsing fails, still forward raw text
                pass

            topic = _sanitize_topic(str(channel))
            # Enqueue with non-blocking put and tiny timeout to avoid blocking websocket thread
            try:
                msg_queue.put((topic, text), timeout=QUEUE_PUT_TIMEOUT)
            except queue.Full:
                # drop message if queue full (log sampling to avoid floods)
                with dropped_lock:
                    dropped_messages += 1
                    if dropped_messages % 1000 == 0:
                        logger.warning("Dropped %d messages so far due to full queue", dropped_messages)
        except Exception:
            # Keep websocket thread resilient
            logger.exception("Exception in on_message (product=%s)", product)

    def on_error(ws, err):
        logger.error("Websocket error for %s: %s", product, err)

    def on_close(ws, close_status_code, close_msg):
        logger.info("Websocket closed for %s: code=%s msg=%s", product, close_status_code, close_msg)

    return on_open, on_message, on_error, on_close


def run_ws_for_product(product: str):
    """Start a WebSocketApp for the given product and run it forever (in its own thread)."""
    on_open, on_message, on_error, on_close = make_callbacks(product)
    ws = websocket.WebSocketApp(
        COINBASE_WEBSOCKET_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
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
                logger.exception("Websocket run_forever crashed for %s; reconnecting after backoff", product)
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
        t = threading.Thread(target=kafka_worker, args=(i,), name=f"kafka-worker-{i}", daemon=False)
        t.start()
        workers.append(t)

    ws_threads = []
    for product in PRODUCTS:
        t = threading.Thread(target=run_ws_for_product, args=(product,), name=f"ws-{product}", daemon=False)
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
        producer.flush(timeout=2)
    except Exception:
        logger.exception("Producer flush during shutdown failed or timed out")
    try:
        producer.close(timeout=2)
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
        producer.flush(timeout=2)
    except Exception:
        logger.exception("Exception flushing producer on shutdown")
    try:
        producer.close(timeout=2)
    except Exception:
        logger.exception("Exception closing producer")

    logger.info("Shutdown complete. Dropped messages: %d", dropped_messages)


if __name__ == "__main__":
    main()
