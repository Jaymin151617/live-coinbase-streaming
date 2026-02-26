import time
from kafka import KafkaProducer

TOPIC_NAME = "coinbase.ticker"

producer = KafkaProducer(
    bootstrap_servers="kafka-239aa580-jayminmistry2000-6ba1.d.aivencloud.com:23593",
    security_protocol="SSL",
    ssl_cafile="secrets/kafka_ca.pem",
    ssl_certfile="secrets/kafka_service.cert",
    ssl_keyfile="secrets/kafka_service.key",
)

for i in range(100):
    message = f"Hello from Python using SSL {i + 1}!"
    producer.send(TOPIC_NAME, message.encode('utf-8'))
    print(f"Message sent: {message}")
    time.sleep(1)

producer.close()
