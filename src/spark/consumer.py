from kafka import KafkaConsumer

TOPIC_NAME = "coinbase.ticker"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers="kafka-239aa580-jayminmistry2000-6ba1.d.aivencloud.com:23593",
    client_id = "CONSUMER_CLIENT_ID",
    group_id = "CONSUMER_GROUP_ID",
    security_protocol="SSL",
    ssl_cafile="secrets/kafka_ca.pem",
    ssl_certfile="secrets/kafka_service.cert",
    ssl_keyfile="secrets/kafka_service.key"
)

while True:
    for message in consumer.poll().values():
        print("Got message using SSL: " + message[0].value.decode('utf-8'))
