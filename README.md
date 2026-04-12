# Live Coinbase Streaming
A real-time analytics pipeline ingesting crypto trade data from the Coinbase Advanced Trade API into Kafka, processing with PySpark, and visualizing metrics in Tableau dashboards.

Setup Spark

1. sudo apt update
2. sudo apt install -y openjdk-17-jdk python3 python3-pip curl wget aria2
3. cd /opt
4. sudo aria2c -x 16 -s 16 https://downloads.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz
5. sudo tar xzf spark-3.5.8-bin-hadoop3.tgz
6. sudo mv spark-3.5.8-bin-hadoop3 /opt/spark
7. sudo rm spark-3.5.8-bin-hadoop3.tgz
8. cat >> ~/.bashrc <<'EOF'
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    export SPARK_HOME=/opt/spark
    export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
    EOF
9. source ~/.bashrc
10. cd $SPARK_HOME/jars
11. wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar
12. wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.8/spark-token-provider-kafka-0-10_2.12-3.5.8.jar
13. wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.jar
14. wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
15. cd secrets
    openssl pkcs12 -export -in kafka_service.cert -inkey kafka_service.key -out kafka_keystore.p12 -name kafka_service_key
    keytool -import -file kafka_ca.pem -alias kafka_CA -keystore kafka_truststore.jks
16. Set env variables
