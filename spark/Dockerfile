FROM apache/spark:3.5.1

USER root

# Bake the connector JARs into the image at build time (no Maven download at runtime).
# Versions must match the Spark version (3.5.1) and Scala 2.12.
ARG SPARK_KAFKA_JAR=spark-sql-kafka-0-10_2.12-3.5.1.jar
ARG KAFKA_CLIENTS_JAR=kafka-clients-3.4.1.jar
ARG COMMONS_POOL_JAR=commons-pool2-2.11.1.jar
ARG TOKEN_PROVIDER_JAR=spark-token-provider-kafka-0-10_2.12-3.5.1.jar
ARG PG_JAR=postgresql-42.7.3.jar

WORKDIR /opt/spark/jars

# Spark-Kafka connector + its transitive deps + Postgres JDBC driver
RUN set -eux; \
    base=https://repo1.maven.org/maven2; \
    curl -fSL -o ${SPARK_KAFKA_JAR}    ${base}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/${SPARK_KAFKA_JAR}; \
    curl -fSL -o ${TOKEN_PROVIDER_JAR} ${base}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/${TOKEN_PROVIDER_JAR}; \
    curl -fSL -o ${KAFKA_CLIENTS_JAR}  ${base}/org/apache/kafka/kafka-clients/3.4.1/${KAFKA_CLIENTS_JAR}; \
    curl -fSL -o ${COMMONS_POOL_JAR}   ${base}/org/apache/commons/commons-pool2/2.11.1/${COMMONS_POOL_JAR}; \
    curl -fSL -o ${PG_JAR}             ${base}/org/postgresql/postgresql/42.7.3/${PG_JAR}

WORKDIR /app
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
