import os
from pyspark.sql import SparkSession
from datetime import datetime, timezone, timedelta

spark = (
    SparkSession.builder
    .appName("KafkaLast1HourBatchRead")
    .getOrCreate()
)

KAFKA_SERVER_URL = os.environ.get("KAFKA_SERVER_URL")
KEYSTORE = os.environ.get("KEYSTORE")
KEYSTORE_PASS = os.environ.get("KEYSTORE_PASS")
KEYSTORE_TYPE = os.environ.get("KEYSTORE_TYPE")
TRUSTSTORE = os.environ.get("TRUSTSTORE")
TRUSTSTORE_PASS = os.environ.get("TRUSTSTORE_PASS")

bootstrap_servers = KAFKA_SERVER_URL
topic = "coinbase.ticker"

# Compute time window in UTC
end_dt = datetime.now(timezone.utc)
start_dt = end_dt - timedelta(minutes=1)

# Spark Kafka timestamp options expect timestamp values as strings
start_ts_ms = str(int(start_dt.timestamp() * 1000))
end_ts_ms = str(int(end_dt.timestamp() * 1000))

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", topic)
    .option("startingTimestamp", start_ts_ms)
    .option("endingTimestamp", end_ts_ms)
    .option("failOnDataLoss", "false")
    .option("kafka.security.protocol", "SSL")
    .option("kafka.ssl.truststore.location", TRUSTSTORE)
    .option("kafka.ssl.truststore.password", TRUSTSTORE_PASS)
    .option("kafka.ssl.keystore.location", KEYSTORE)
    .option("kafka.ssl.keystore.password", KEYSTORE_PASS)
    .option("kafka.ssl.keystore.type", KEYSTORE_TYPE)
    .option("kafka.ssl.key.password", KEYSTORE_PASS)
    .load()
)

result = df.selectExpr(
    "CAST(key AS STRING) AS key",
    "CAST(value AS STRING) AS value",
    "topic",
    "partition",
    "offset",
    "timestamp",
)

result.show(truncate=False)

spark.stop()
