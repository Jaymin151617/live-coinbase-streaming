# test_aiven_stream.py
import os
from pyspark.sql import SparkSession

# config via env vars (safer) or replace defaults below
BOOTSTRAP = os.environ.get("KAFKA_SERVER_URL")
TOPIC = os.environ.get("TOPIC", "coinbase.ticker")

# paths & passwords for keystore/truststore
KEYSTORE = os.environ.get("KEYSTORE")
KEYSTORE_PASS = os.environ.get("KEYSTORE_PASS")
KEYSTORE_TYPE = os.environ.get("KEYSTORE_TYPE")

TRUSTSTORE = os.environ.get("TRUSTSTORE")
TRUSTSTORE_PASS = os.environ.get("TRUSTSTORE_PASS")

spark = SparkSession.builder.appName("aiven-kafka-test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

reader = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol", "SSL")
    .option("kafka.ssl.truststore.location", TRUSTSTORE)
    .option("kafka.ssl.truststore.password", TRUSTSTORE_PASS)
    .option("kafka.ssl.keystore.location", KEYSTORE)
    .option("kafka.ssl.keystore.password", KEYSTORE_PASS)
    .option("kafka.ssl.keystore.type", KEYSTORE_TYPE)
    .option("kafka.ssl.key.password", KEYSTORE_PASS)
)

df = reader.load()

# cast key/value to string and show to console
out = df.selectExpr("topic", "CAST(value AS STRING) as value", "partition", "offset")

query = (
    out.writeStream
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
