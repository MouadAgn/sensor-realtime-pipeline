# Image Spark préchargée avec Delta Lake + connecteur Kafka + driver JDBC Postgres.
# Les jars sont BAKÉS dans l'image : `spark-submit` fonctionne hors-ligne
# (pas de --packages à la soumission => démo live fiable).
FROM apache/spark:3.5.3

USER root

# Versions alignées sur Spark 3.5.3 / Scala 2.12
ENV DELTA_VERSION=3.2.1 \
    KAFKA_CONNECTOR_VERSION=3.5.3 \
    KAFKA_CLIENTS_VERSION=3.4.1 \
    COMMONS_POOL2_VERSION=2.11.1 \
    POSTGRES_JDBC_VERSION=42.7.3

WORKDIR /opt/spark/jars

RUN set -eux; \
    base="https://repo1.maven.org/maven2"; \
    curl -fSL -o "delta-spark_2.12-${DELTA_VERSION}.jar" \
        "${base}/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar"; \
    curl -fSL -o "delta-storage-${DELTA_VERSION}.jar" \
        "${base}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"; \
    curl -fSL -o "spark-sql-kafka-0-10_2.12-${KAFKA_CONNECTOR_VERSION}.jar" \
        "${base}/org/apache/spark/spark-sql-kafka-0-10_2.12/${KAFKA_CONNECTOR_VERSION}/spark-sql-kafka-0-10_2.12-${KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fSL -o "spark-token-provider-kafka-0-10_2.12-${KAFKA_CONNECTOR_VERSION}.jar" \
        "${base}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${KAFKA_CONNECTOR_VERSION}/spark-token-provider-kafka-0-10_2.12-${KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fSL -o "kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
        "${base}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fSL -o "commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
        "${base}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    curl -fSL -o "postgresql-${POSTGRES_JDBC_VERSION}.jar" \
        "${base}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# Dossier de conf (l'image de base n'en fournit pas) + entrepôt/checkpoints inscriptibles
RUN mkdir -p /opt/spark/conf /opt/lakehouse /opt/checkpoints \
    && chown -R 185:185 /opt/spark/conf /opt/lakehouse /opt/checkpoints

# Rendre spark-submit/spark-sql/... disponibles sans chemin complet
# (utile pour `docker compose exec`, qui n'exécute pas l'entrypoint).
ENV PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}"

USER 185
