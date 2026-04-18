# Coinbase Live Analytics Pipeline

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
![Python 3.12.12](https://img.shields.io/badge/Python-3.12.12-3776AB?logo=python&logoColor=white)
![Java 17.0.18](https://img.shields.io/badge/Java-17.0.18-007396?logo=openjdk&logoColor=white)
![Spark 3.5.8](https://img.shields.io/badge/Apache%20Spark-3.5.8-E25A1C?logo=apachespark&logoColor=white)
![Kafka 4.1.2](https://img.shields.io/badge/Apache%20Kafka-4.1.2-231F20?logo=apachekafka&logoColor=white)
![PostgreSQL 17.9](https://img.shields.io/badge/PostgreSQL-17.9-4169E1?logo=postgresql&logoColor=white)
![Metabase 0.59.6](https://img.shields.io/badge/Metabase-0.59.6-509EE3?logo=metabase&logoColor=white)
![Status](https://img.shields.io/badge/status-inprogress-orange)

A real-time analytics pipeline that streams Coinbase Advanced Trade market data into Kafka, processes it with Spark Structured Streaming, stores analytics-ready tables in PostgreSQL, and exposes the warehouse to Metabase.

This repository is currently set up for hands-on local or single-host deployment. The code is production-minded, but first-time bootstrap is still manual: you need to provide secrets, a `.env` file, Spark dependencies, and the initial PostgreSQL schema yourself.

## What This Project Does

- Streams the `ticker`, `candles`, and `market_trades` Coinbase channels
- Publishes raw channel payloads into an Aiven-managed Kafka cluster over SSL
- Consumes Kafka topics with Spark Structured Streaming
- Writes staged, deduplicated, and aggregated datasets into PostgreSQL
- Makes the resulting tables available for dashboarding in Metabase

The current configuration in [src/config.json](src/config.json) enables these products:

- `BTC-USD`

- `ETH-USD`
- `LINK-USD`
- `SOL-USD`

## Architecture

```text
Coinbase Advanced Trade WebSocket
        |
        v
Kafka producer (src/kafka/producer.py)
        |
        v
Kafka topics:
  - coinbase.ticker
  - coinbase.candles
  - coinbase.market_trades
        |
        v
Spark Structured Streaming consumer (src/spark/consumer.py)
        |
        v
PostgreSQL schema: coinbase
  - staging tables
  - raw trades table
  - final snapshot and aggregate tables
        |
        v
Metabase dashboards / ad hoc analysis
```

## Repository Layout

- [docker-compose.yml](docker-compose.yml): runs the producer container, PostgreSQL, and Metabase
- [src/kafka/producer.py](src/kafka/producer.py): subscribes to Coinbase WebSocket channels and writes raw messages to Kafka
- [src/spark/consumer.py](src/spark/consumer.py): reads Kafka topics, transforms payloads, and loads PostgreSQL
- [src/config.json](src/config.json): source of truth for enabled products, channels, schema name, and table mappings
- [src/sql/ddls](src/sql/ddls): PostgreSQL tables and maintenance functions
- [src/sql/upsert_market_trades.sql](src/sql/upsert_market_trades.sql): staging -> raw -> aggregated trade pipeline
- [src/sql/upsert_tickers.sql](src/sql/upsert_tickers.sql): staging -> final ticker snapshot upsert
- [src/sql/upsert_candles.sql](src/sql/upsert_candles.sql): staging -> final candle snapshot upsert
- [unused/reprocess.py](unused/reprocess.py): ad hoc batch reader for replaying a recent Kafka window

## Data Model

The Spark consumer writes into the `coinbase` schema and uses a staging-plus-upsert pattern:

- `coinbase.stg_market_trades`: landing table for incoming trade events

- `coinbase.raw_market_trades`: deduplicated source-of-truth trade records
- `coinbase.market_trades_agg`: minute-level aggregated trade metrics
- `coinbase.stg_ticker_snapshot`: landing table for ticker rows
- `coinbase.ticker_snapshot`: latest ticker snapshot per product and minute
- `coinbase.stg_candles_snapshot`: landing table for candle rows
- `coinbase.candles_snapshot`: latest candle snapshot per product and minute

The DDLs also include maintenance functions:

- [src/sql/ddls/ensure_partitions.sql](src/sql/ddls/ensure_partitions.sql): creates current and next-hour partitions
- [src/sql/ddls/delete_partitions.sql](src/sql/ddls/delete_partitions.sql): drops old partitions
- [src/sql/ddls/delete_raw_trades.sql](src/sql/ddls/delete_raw_trades.sql): trims old raw trade rows in batches

Those functions are defined in the database, but this repository does not currently include a scheduler for running them. Plan to call them from `cron`, `pg_cron`, or another job runner.

## Prerequisites

You will need:

- Docker and Docker Compose
- Python 3.12 for the producer image build
- Java 17
- Apache Spark 3.5.x on the machine that runs the consumer
- Aiven for Apache Kafka access details and SSL material
- Coinbase Advanced Trade API credentials

The Spark consumer also expects the following to be available on Spark's classpath:

- Spark Kafka integration jars
- Kafka client jars
- Commons Pool 2
- PostgreSQL JDBC driver

If you prefer the manual Spark installation pattern already used in this repo, one working approach is:

1. Install Java 17 and Spark 3.5.x.
2. Add the Spark Kafka connector jars to `$SPARK_HOME/jars`.
3. Add a PostgreSQL JDBC driver jar to `$SPARK_HOME/jars`.

## Required Secrets

Do not commit secrets. The `secrets/` directory is already ignored by git.

The code expects these files:

- `secrets/cb_api_key.key`: Coinbase API key identifier

- `secrets/cb_secret_key.pem`: Coinbase private key used to build JWTs
- `secrets/kafka_ca.pem`: Kafka CA certificate
- `secrets/kafka_service.cert`: Kafka client certificate
- `secrets/kafka_service.key`: Kafka client private key

If you run the Spark consumer with a keystore and truststore, you will also need derived files such as:

- `secrets/kafka_keystore.p12`

- `secrets/kafka_truststore.jks`

## Environment Variables

Create a `.env` file in the repository root. At minimum, document and populate the variables used directly by this repo:

### Core streaming

- `KAFKA_SERVER_URL`: Kafka bootstrap server(s)

- `COINBASE_WEBSOCKET_URL`: Coinbase market data WebSocket endpoint

For Coinbase Advanced Trade market data, the official production endpoint is `wss://advanced-trade-ws.coinbase.com`.

### Spark consumer and PostgreSQL

- `PG_HOST`: PostgreSQL host used by the Spark consumer

- `PG_PORT`: PostgreSQL port used by the Spark consumer
- `PG_DATABASE`: target database name
- `PG_USERNAME`: PostgreSQL username
- `PG_PASSWORD`: PostgreSQL password
- `KEYSTORE`: path to the Kafka client keystore used by Spark
- `KEYSTORE_PASS`: keystore password
- `KEYSTORE_TYPE`: keystore type, such as `PKCS12`
- `TRUSTSTORE`: path to the Kafka truststore used by Spark
- `TRUSTSTORE_PASS`: truststore password

### Docker Compose PostgreSQL settings

- `POSTGRES_PORT`: host port mapped to the PostgreSQL container

- `POSTGRES_MAX_CONNECTIONS`
- `POSTGRES_SHARED_BUFFERS`
- `POSTGRES_WORK_MEM`
- `POSTGRES_EFFECTIVE_CACHE_SIZE`

You will likely also need the standard PostgreSQL container initialization variables in `.env`, such as `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`, plus any Metabase-specific settings you want the container to read.

### Optional tuning

Producer tuning:

- `WORKER_COUNT`

- `BATCH_SIZE`
- `LINGER_MS`
- `MESSAGE_QUEUE_MAXSIZE`
- `SO_RCVBUF_BYTES`
- `KAFKA_BATCH_BYTES`
- `KAFKA_RETRIES`
- `QUEUE_PUT_TIMEOUT`
- `RETRY_BACKOFF`
- `REQUEST_TIMEOUT`
- `DEBUG_LOGS`

Consumer tuning:

- `SPARK_THREADS`

- `SPARK_DRIVER_MEMORY`
- `SPARK_EXECUTOR_MEMORY`
- `SPARK_CHECKPOINTS_TO_RETAIN`
- `PROCESSING_TIME`
- `SPARK_BATCHSIZE`
- `JDBC_WRITE_PARTITIONS`
- `JDBC_BATCHSIZE`

## Quick Start

### 1. Prepare secrets and environment

- Put the required keys and certificates under `secrets/`
- Create `.env` with the variables described above
- Keep the `PG_*` values aligned with the PostgreSQL instance you intend the Spark consumer to use

### 2. Start PostgreSQL and Metabase

```bash
docker compose up -d postgres metabase
```

Metabase will be available at `http://localhost:3000`.

### 3. Initialize the database schema

Create the `coinbase` schema and load the DDL files before starting the Spark consumer:

```bash
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USERNAME" -d "$PG_DATABASE" -c "CREATE SCHEMA IF NOT EXISTS coinbase;"
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USERNAME" -d "$PG_DATABASE" -f src/sql/ddls/market_trades.sql
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USERNAME" -d "$PG_DATABASE" -f src/sql/ddls/ticker_snapshot.sql
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USERNAME" -d "$PG_DATABASE" -f src/sql/ddls/candles_snapshot.sql
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USERNAME" -d "$PG_DATABASE" -f src/sql/ddls/ensure_partitions.sql
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USERNAME" -d "$PG_DATABASE" -f src/sql/ddls/delete_partitions.sql
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USERNAME" -d "$PG_DATABASE" -f src/sql/ddls/delete_raw_trades.sql
```

### 4. Start the Coinbase -> Kafka producer

```bash
docker compose up -d producer
```

To watch producer logs:

```bash
docker compose logs -f producer
```

### 5. Start the Spark consumer

Run the consumer from the host where Spark is installed:

```bash
spark-submit src/spark/consumer.py
```

If `spark-submit` is not on your `PATH`, use:

```bash
$SPARK_HOME/bin/spark-submit src/spark/consumer.py
```

### 6. Schedule database maintenance

After the pipeline is live, schedule these functions externally so partitions and raw-trade retention stay healthy:

- `SELECT coinbase.ensure_coinbase_partitions();`
- `SELECT coinbase.drop_old_coinbase_partitions();`
- `SELECT coinbase.cleanup_raw_trades();`

## Runtime Behavior

- The producer opens one Coinbase WebSocket connection per enabled product
- Raw messages are routed to Kafka topics named from the channel, such as `coinbase.ticker`
- The Spark consumer reads from Kafka with `startingOffsets=latest`
- Ticker and candle outputs are normalized to one row per product per minute
- Market trades are deduplicated into a raw table and aggregated into minute buckets
- Spark checkpoints are stored under `checkpoints/coinbase_consumer`
- Consumer logs are written to `logs/consumer.log`

## Local Operations

Useful commands while working locally:

```bash
docker compose ps
docker compose logs -f postgres
docker compose logs -f metabase
docker compose logs -f producer
```

## Current Repository Notes

- `docker-compose.yml` does not currently run the Spark consumer; that process is started separately
- There is no checked-in `.env.example`, migration runner, or bootstrap script yet
- The database maintenance functions exist, but scheduling them is still your responsibility
- The repo contains an `unused/` helper for replay experimentation, but the main runtime path is producer -> Kafka -> Spark -> PostgreSQL -> Metabase

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).