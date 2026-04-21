# Coinbase Live Analytics Pipeline

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
![Python 3.12.12](https://img.shields.io/badge/Python-3.12.12-3776AB?logo=python&logoColor=white)
![Java 17.0.18](https://img.shields.io/badge/Java-17.0.18-007396?logo=openjdk&logoColor=white)
![Spark 3.5.8](https://img.shields.io/badge/Apache%20Spark-3.5.8-E25A1C?logo=apachespark&logoColor=white)
![Kafka 4.1.2](https://img.shields.io/badge/Apache%20Kafka-4.1.2-231F20?logo=apachekafka&logoColor=white)
![PostgreSQL 17.9](https://img.shields.io/badge/PostgreSQL-17.9-4169E1?logo=postgresql&logoColor=white)
![Metabase 0.59.6](https://img.shields.io/badge/Metabase-0.59.6-509EE3?logo=metabase&logoColor=white)
![Docker 4.69.0](https://img.shields.io/badge/Docker-4.69.0-2496ED?logo=docker&logoColor=white)
![Status](https://img.shields.io/badge/status-complete-brightgreen)

## Table of Contents

- [Overview](#overview)
- [What This Project Does](#what-this-project-does)
- [Architecture](#architecture)
- [Repository Layout](#repository-layout)
- [Data Model](#data-model)
- [Prerequisites](#prerequisites)
- [Required Secrets](#required-secrets)
- [Environment Variables](#environment-variables)
- [Quick Start](#quick-start)
- [Runtime Behavior](#runtime-behavior)
- [Local Operations](#local-operations)
- [Current Repository Notes](#current-repository-notes)
- [License](#license)

## Overview

![Metabase dashboard](assets/coinbase_dashboard.gif)

A real-time analytics pipeline that streams Coinbase Advanced Trade market data into Kafka, processes it with Spark Structured Streaming, stores analytics-ready tables in PostgreSQL, and exposes the warehouse to Metabase.

This repository is currently set up for hands-on local or single-host deployment. The code is production-minded, but first-time bootstrap is still manual: you need to provide secrets, a `.env` file, Spark dependencies, and the initial PostgreSQL schema yourself.

## What This Project Does

- Streams the `ticker`, `candles`, and `market_trades` Coinbase channels
- Publishes raw channel payloads into an Aiven-managed Kafka cluster over SSL
- Consumes Kafka topics with Spark Structured Streaming
- Writes staged, deduplicated, and aggregated datasets into PostgreSQL
- Makes the resulting tables available for dashboarding in Metabase

This project demonstrates production-style data engineering patterns:

- Streaming-first architecture (Kafka + Spark)
- Idempotent ingestion (staging → upsert)
- Partitioned warehouse design
- Decoupled ingestion & processing
- BI-ready modeling

The current configuration in [src/config.json](src/config.json) enables these products:

- `BTC-USD` (Bitcoin)
- `ETH-USD` (Ethereum)
- `LINK-USD` (Chainlink)
- `SOL-USD` (Solana)

## Architecture

```text
Coinbase Advanced Trade WebSocket
        |
        v
Kafka producer
        |
        v
Kafka topics:
  - coinbase.ticker
  - coinbase.candles
  - coinbase.market_trades
        |
        v
Spark Structured Streaming consumer
        |
        v
PostgreSQL schema: coinbase
  - staging tables
  - raw trades table
  - final snapshot and aggregate tables
        |
        v
Metabase dashboard
```

## Repository Layout

```text
.
├── .dockerignore                 # docker build ignore rules
├── .env                          # environment variables (gitignored)
├── .gitignore                    # git ignore rules
├── LICENSE                       # project license (MIT)
├── README.md                     # project documentation

├── assets/                       # media files used in documentation
│   └── coinbase_dashboard.gif    # preview of the dashboard

├── checkpoints/                  # spark structured streaming checkpoints (gitignored)
│   └── coinbase_consumer/        # checkpoint data for consumer job
│       ├── commits/              # processed batch commit logs
│       ├── offsets/              # kafka offsets tracking
│       ├── sources/              # source metadata
│       └── metadata              # checkpoint metadata

├── docker-compose.yml            # defines postgres, metabase, and producer services

├── logs/                         # application logs (gitignored)
│   └── consumer.log              # spark consumer logs

├── secrets/                      # sensitive credentials and ssl certs (gitignored)
│   ├── cb_api_key.key            # coinbase api key
│   ├── cb_secret_key.pem         # coinbase private key
│   ├── kafka_*                   # kafka ssl certs, keys, keystore, truststore

├── src/                          # main application source code
│   ├── config.json               # pipeline config (products, topics, mappings)

│   ├── kafka/                    # kafka producer (coinbase websocket → kafka)
│   │   ├── Dockerfile            # container for producer
│   │   ├── producer.py           # websocket ingestion + kafka publishing logic
│   │   └── requirements.txt      # python dependencies for producer

│   ├── spark/                    # spark structured streaming consumer
│   │   └── consumer.py           # kafka → postgres processing pipeline

│   └── sql/                      # database schema and transformation logic
│       ├── ddls/                 # table definitions, maintenance functions
│       ├── metabase/             # queries used to create the questions
│       ├── upsert_*.sql          # staging → final upsert and aggregation queries

└── unused/                       # experimental or non-production scripts
```

## Data Model

The Spark consumer writes into the `coinbase` schema and uses a staging-plus-upsert pattern:

- `coinbase.stg_market_trades`: landing table for incoming trade events
- `coinbase.raw_market_trades`: deduplicated source-of-truth trade records
- `coinbase.market_trades_agg`: minute-level aggregated trade metrics
- `coinbase.stg_ticker_snapshot`: landing table for ticker rows
- `coinbase.ticker_snapshot`: latest ticker snapshot per product and minute
- `coinbase.stg_candles_snapshot`: landing table for candle rows
- `coinbase.candles_snapshot`: latest candle snapshot per product and minute

### Staging Tables (`stg_*`)
- No indexes or primary keys  
- No partitioning  
- Designed for maximum write throughput during ingestion  
- Used as transient landing tables before transformation  

### Raw Trades Table (`coinbase.raw_market_trades`)
- Primary Key: `trade_id` → prevents duplicate inserts, ensuring idempotent writes
- Index: `trade_time_utc` → enables efficient filtering for retention and cleanup  
- Acts as the source of truth for trade data  

### Final Tables (Snapshots & Aggregates)
- Primary Key: (`product_id`, timestamp)  
- Partitioned by: timestamp (time-based partitioning)  

### Benefits:
- Fast point lookups and upserts  
- Efficient time-range queries  
- Scalable data retention via partition pruning  
- Simplified deletion of old data using partition drops  

### Design Summary
| Layer     | Write Optimization | Read Optimization | Partitioning |
|----------|------------------|------------------|-------------|
| Staging  | High             | None             | No          |
| Raw      | Balanced         | Indexed          | No          |
| Final    | Balanced         | Optimized        | Yes         |

The DDLs also include maintenance functions:

- [ensure_partitions.sql](src/sql/ddls/ensure_partitions.sql): creates current and next-hour partitions
- [delete_partitions.sql](src/sql/ddls/delete_partitions.sql): drops old partitions
- [delete_raw_trades.sql](src/sql/ddls/delete_raw_trades.sql): trims old raw trade rows in batches

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

### Coinbase Secrets
Sign up: https://www.coinbase.com/en-in/developer-platform/products/advanced-trade-api

- `secrets/cb_api_key.key`: coinbase api key identifier  
- `secrets/cb_secret_key.pem`: coinbase private key used to build JWTs  

### Aiven Kafka Secrets
Sign up: https://console.aiven.io/signup

- `secrets/kafka_ca.pem`: kafka ca certificate  
- `secrets/kafka_service.cert`: kafka client certificate  
- `secrets/kafka_service.key`: kafka client private key  

If you run the Spark consumer with a keystore and truststore, you will also need derived files such as:

- `secrets/kafka_keystore.p12`
- `secrets/kafka_truststore.jks`

These can be generated from the provided certificates:

```bash
cd secrets

# Create PKCS12 keystore (client cert + key)
openssl pkcs12 -export \
  -in kafka_service.cert \
  -inkey kafka_service.key \
  -out kafka_keystore.p12 \
  -name kafka_service_key

# Create Java truststore (CA certificate)
keytool -import \
  -file kafka_ca.pem \
  -alias kafka_CA \
  -keystore kafka_truststore.jks
```

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

- `POSTGRES_PORT`: host port mapped to the PostgreSQL container for external access  
- `POSTGRES_MAX_CONNECTIONS`: maximum number of concurrent database connections  
- `POSTGRES_SHARED_BUFFERS`: memory used by PostgreSQL for caching data  
- `POSTGRES_WORK_MEM`: memory allocated per query operation (sorts, joins, aggregations)  
- `POSTGRES_EFFECTIVE_CACHE_SIZE`: estimated memory available for OS + PostgreSQL disk caching (planner hint)  

You will likely also need the standard PostgreSQL container initialization variables in `.env`, such as `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`, plus any Metabase-specific settings you want the container to read.

### Docker Compose Metabase settings

- `MB_DB_TYPE`: database type used by metabase for application metadata (e.g., postgres)  
- `MB_DB_DBNAME`: name of the database used by metabase to store its internal data  
- `MB_DB_PORT`: port of the metabase application database (default 5432 for postgres)  
- `MB_DB_USER`: username for connecting to the metabase application database  
- `MB_DB_PASS`: password for the metabase application database user  
- `MB_DB_HOST`: hostname of the metabase application database (e.g., postgres container)  

### Optional tuning

#### Producer tuning:

- `WORKER_COUNT`: number of parallel producer worker threads  
- `BATCH_SIZE`: number of messages batched before sending to Kafka  
- `LINGER_MS`: time to wait before sending a batch to allow more messages to accumulate  
- `MESSAGE_QUEUE_MAXSIZE`: maximum size of the internal message queue for backpressure control  
- `SO_RCVBUF_BYTES`: socket receive buffer size for network throughput  
- `KAFKA_BATCH_BYTES`: maximum batch size (in bytes) sent to Kafka  
- `KAFKA_RETRIES`: number of retry attempts for failed Kafka sends  
- `QUEUE_PUT_TIMEOUT`: timeout when adding messages to the internal queue  
- `RETRY_BACKOFF`: delay between retry attempts  
- `REQUEST_TIMEOUT`: maximum time to wait for Kafka response  
- `DEBUG_LOGS`: enables verbose logging for debugging  

#### Consumer tuning:

- `SPARK_THREADS`: number of threads used by Spark for processing  
- `SPARK_DRIVER_MEMORY`: memory allocated to the Spark driver  
- `SPARK_EXECUTOR_MEMORY`: memory allocated to Spark executors  
- `SPARK_CHECKPOINTS_TO_RETAIN`: number of checkpoint versions to retain  
- `PROCESSING_TIME`: micro-batch interval for structured streaming  
- `SPARK_BATCHSIZE`: number of records processed per batch  
- `JDBC_WRITE_PARTITIONS`: number of parallel partitions for JDBC writes  
- `JDBC_BATCHSIZE`: number of rows per batch when writing to PostgreSQL  

## Quick Start

### 1. Prepare secrets and environment

- Put the required keys and certificates under `secrets/`
- Create `.env` with the variables described above
- Keep the `PG_*` values aligned with the PostgreSQL instance you intend the Spark consumer to use

### 2. Start PostgreSQL and Metabase

```bash
docker compose up -d postgres metabase
```

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

### 7. Create the Dashboard

Once data starts flowing into PostgreSQL, open Metabase at `http://localhost:3000`.

#### Connect Metabase to PostgreSQL

- Click `+` next to **Data** in the left sidebar  
- Select **PostgreSQL**  
- Enter connection details:
  - host: postgres container name  
  - port: value defined in your setup (e.g., 5432)  
  - database, username, password: as configured in `.env`  
- Click **Save**

#### Create Questions (Charts)

Questions are the building blocks of dashboards.
- SQL queries are available in `src/metabase/questions/`
- Click `+ New` → **Question**
- Select your database → schema → table  
- Click the icon next to **Save** → **View the SQL**
- Convert to SQL mode  
- Replace the existing query with your query from the repo  
- Save the question  

Repeat this for each chart you want.

#### Create Dashboard
- Click `+ New` → **Dashboard**
- Enter a name and create  
- Click **Add a question**
- Select the questions you created  
- Arrange visuals as needed and **Save**

Your dashboard is now ready.

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