#!/usr/bin/env python3
"""
Kafka consumer -> per-minute NDJSON files (refactored).

The main loop logic has been separated into small helper functions to keep
the flow simple and unit-testable:
- create_consumer()
- handle_record()
- maybe_flush_batch()
- flush_topic_minute()
- periodic_flush()
- shutdown_flush()

Behavior and config are the same as before; only structure changed.
"""

import logging
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import threading

from kafka import KafkaConsumer
from kafka.errors import KafkaError

# --- Configuration (consistent with producer style) ---
ROOT_DIR = Path(__file__).resolve().parents[2]

DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TOPICS = ["coinbase.ticker", "coinbase.market_trades", "coinbase.candles"]

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_SERVER_URL")
CLIENT_ID = "coinbase-stream-consumer"
GROUP_ID = "coinbase-spark-group"

SSL_CAFILE = ROOT_DIR / "secrets" / "kafka_ca.pem"
SSL_CERTFILE = ROOT_DIR / "secrets" / "kafka_service.cert"
SSL_KEYFILE = ROOT_DIR / "secrets" / "kafka_service.key"

# Batching / durability tuning
FLUSH_BATCH = 500              # flush when this many messages collected for current minute
FLUSH_INTERVAL = 5.0           # seconds: flush any buffered messages at least this often
FILE_IO_RETRIES = 3
FILE_IO_RETRY_BACKOFF = 0.5    # seconds between file write retries

# Kafka consumer settings
AUTO_OFFSET_RESET = "latest"
ENABLE_AUTO_COMMIT = False
POLL_TIMEOUT_MS = 1000
MAX_POLL_RECORDS = 1000

# Logging (consistent with producer)
LOG_FILE = ROOT_DIR / "logs" / "consumer.log"
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s %(name)s %(message)s",
)
logger = logging.getLogger("cb-kafka-bridge")   # same logger namespace as producer
logging.getLogger("kafka").setLevel(logging.INFO)

# --- Globals for control ---
stop_event = threading.Event()


# --- Utilities ---
def sanitize_topic_for_filename(topic: str) -> str:
    return topic.replace("/", "_").replace("\\", "_").replace("..", "_").replace(":", "_").replace(".", "_")


def minute_str_from_ts(ts: Optional[float] = None) -> str:
    dt = datetime.fromtimestamp(ts if ts is not None else time.time())
    return dt.strftime("%Y%m%d_%H%M")


def ndjson_path_for_topic_and_minute(topic: str, minute_str: str) -> Path:
    safe = sanitize_topic_for_filename(topic)
    return DATA_DIR / f"{safe}_{minute_str}.ndjson"


def append_lines_to_file(path: Path, lines: List[str]) -> None:
    attempt = 0
    while True:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                for line in lines:
                    if not line.endswith("\n"):
                        f.write(line + "\n")
                    else:
                        f.write(line)
                f.flush()
                os.fsync(f.fileno())
            return
        except Exception as e:
            attempt += 1
            logger.exception("Failed to write to %s (attempt %d/%d): %s", path, attempt, FILE_IO_RETRIES, e)
            if attempt >= FILE_IO_RETRIES:
                raise
            time.sleep(FILE_IO_RETRY_BACKOFF)


# --- Consumer lifecycle helpers ---
def create_consumer() -> KafkaConsumer:
    """Create and return a configured KafkaConsumer (or exit on failure)."""
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id=CLIENT_ID,
            group_id=GROUP_ID,
            security_protocol="SSL",
            ssl_cafile=SSL_CAFILE,
            ssl_certfile=SSL_CERTFILE,
            ssl_keyfile=SSL_KEYFILE,
            auto_offset_reset=AUTO_OFFSET_RESET,
            enable_auto_commit=ENABLE_AUTO_COMMIT,
            consumer_timeout_ms=1000,
            max_poll_records=MAX_POLL_RECORDS,
        )
        return consumer
    except Exception:
        logger.exception("Failed to create KafkaConsumer — exiting")
        sys.exit(1)


def get_record_minute(record) -> str:
    """Return minute string for record timestamp (use record.timestamp if available)."""
    try:
        if hasattr(record, "timestamp") and record.timestamp:
            return minute_str_from_ts(record.timestamp / 1000.0)
    except Exception:
        pass
    return minute_str_from_ts()


def decode_value(record) -> str:
    try:
        if isinstance(record.value, (bytes, bytearray)):
            return record.value.decode("utf-8")
        return str(record.value)
    except Exception:
        return str(record.value)


def commit_offsets_safe(consumer: KafkaConsumer) -> None:
    try:
        consumer.commit()
    except Exception:
        logger.exception("Failed to commit offsets")


# --- Buffer & flush helpers (kept simple) ---
def flush_topic_minute(topic: str, minute_for_topic: str, buffers: Dict[str, List[str]], consumer: KafkaConsumer) -> None:
    """Flush the buffer for topic/minute to disk and commit offsets."""
    msgs = buffers.get(topic)
    if not msgs:
        return
    path = ndjson_path_for_topic_and_minute(topic, minute_for_topic)
    logger.info("Flushing %d messages for %s -> %s", len(msgs), topic, path)
    append_lines_to_file(path, msgs)
    buffers[topic].clear()
    commit_offsets_safe(consumer)


def maybe_flush_batch(topic: str, buffers: Dict[str, List[str]], current_minute: Dict[str, str], consumer: KafkaConsumer) -> None:
    """Flush when buffer hits FLUSH_BATCH for the current minute."""
    if len(buffers.get(topic, [])) >= FLUSH_BATCH:
        minute_for_topic = current_minute.get(topic, minute_str_from_ts())
        flush_topic_minute(topic, minute_for_topic, buffers, consumer)


def periodic_flush(buffers: Dict[str, List[str]], current_minute: Dict[str, str], consumer: KafkaConsumer) -> None:
    """Flush all non-empty topic buffers (used on time interval)."""
    topics = [t for t, v in buffers.items() if v]
    if not topics:
        return
    logger.info("Periodic flush for topics: %s", topics)
    for topic in topics:
        minute_for_topic = current_minute.get(topic, minute_str_from_ts())
        flush_topic_minute(topic, minute_for_topic, buffers, consumer)


def shutdown_flush(buffers: Dict[str, List[str]], current_minute: Dict[str, str], consumer: KafkaConsumer) -> None:
    """Flush remaining buffers during shutdown and commit final offsets."""
    pending = [t for t in buffers if buffers[t]]
    logger.info("Shutdown flush for %d topics: %s", len(pending), pending)
    for topic in pending:
        minute_for_topic = current_minute.get(topic, minute_str_from_ts())
        flush_topic_minute(topic, minute_for_topic, buffers, consumer)
    # final commit
    commit_offsets_safe(consumer)


# --- Record handling (single responsibility) ---
def handle_record(r, buffers: Dict[str, List[str]], current_minute: Dict[str, str], consumer: KafkaConsumer) -> None:
    """Process a single ConsumerRecord: decode, determine minute, buffer, and flush if needed."""
    topic = r.topic
    value_text = decode_value(r)
    msg_minute = get_record_minute(r)

    if topic not in current_minute:
        current_minute[topic] = msg_minute

    # If message minute differs -> rotate (flush old minute)
    if msg_minute != current_minute[topic]:
        if buffers.get(topic):
            logger.info("Minute rollover for %s: flushing %d messages (minute=%s)", topic, len(buffers[topic]), current_minute[topic])
            flush_topic_minute(topic, current_minute[topic], buffers, consumer)
        current_minute[topic] = msg_minute

    buffers[topic].append(value_text)
    maybe_flush_batch(topic, buffers, current_minute, consumer)


# --- Signal handler ---
def _signal_handler(signum, frame):
    logger.info("Shutdown signal received (signal=%s); requesting stop...", signum)
    stop_event.set()


# --- Main (simple, uses the helpers above) ---
def main():
    # install signals
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    logger.info("Starting consumer (topics=%s) writing to %s", TOPICS, DATA_DIR)
    consumer = create_consumer()

    buffers: Dict[str, List[str]] = defaultdict(list)
    current_minute: Dict[str, str] = {}
    last_flush_ts = time.monotonic()

    try:
        while not stop_event.is_set():
            try:
                records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=MAX_POLL_RECORDS)
            except KafkaError:
                logger.exception("Kafka poll error; backing off briefly")
                time.sleep(1.0)
                continue

            # Process polled records
            for tp, recs in records.items():
                for r in recs:
                    handle_record(r, buffers, current_minute, consumer)

            # Periodic flush by time
            if (time.monotonic() - last_flush_ts) >= FLUSH_INTERVAL:
                periodic_flush(buffers, current_minute, consumer)
                last_flush_ts = time.monotonic()

        # shutdown requested
        logger.info("Stop requested; performing shutdown flush")
        shutdown_flush(buffers, current_minute, consumer)

    except Exception:
        logger.exception("Unexpected exception in consumer main loop")
    finally:
        try:
            consumer.close()
        except Exception:
            logger.exception("Error closing consumer")
        logger.info("Consumer shutdown complete")


if __name__ == "__main__":
    main()
