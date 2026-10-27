FROM flink:2.1.3-scala_2.12-java17

USER root

ARG ICEBERG_VERSION=1.11.0
ARG FLINK_KAFKA_CONNECTOR_VERSION=5.0.0-2.1
ARG HADOOP_VERSION=3.4.3

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl --fail --location --retry 5 --retry-delay 2 \
       "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.1/${ICEBERG_VERSION}/iceberg-flink-runtime-2.1-${ICEBERG_VERSION}.jar" \
       --output "/opt/flink/lib/iceberg-flink-runtime-2.1-${ICEBERG_VERSION}.jar" \
    && curl --fail --location --retry 5 --retry-delay 2 \
       "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
       --output "/opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    && curl --fail --location --retry 5 --retry-delay 2 \
       "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar" \
       --output "/opt/flink/lib/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar" \
    && curl --fail --location --retry 5 --retry-delay 2 \
       "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar" \
       --output "/opt/flink/lib/hadoop-client-api-${HADOOP_VERSION}.jar" \
    && curl --fail --location --retry 5 --retry-delay 2 \
       "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar" \
       --output "/opt/flink/lib/hadoop-client-runtime-${HADOOP_VERSION}.jar"

# Enable Flink S3A filesystem for checkpoints/savepoints on Apache Ozone
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
    && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar \
       /opt/flink/plugins/s3-fs-hadoop/

USER flink
