# ── Stage 1: Build ──────────────────────────────────────────────────────────
FROM maven:3.9.4-eclipse-temurin-17 AS builder
WORKDIR /build

# Cache Maven dependencies as a separate layer (only re-downloads on pom.xml change)
COPY pom.xml ./
RUN mvn dependency:go-offline -B

# Build the shaded JAR
COPY src ./src
RUN mvn clean package -DskipTests

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
# tabulario/spark-iceberg already bundles Spark + Iceberg + S3A jars,
# so no manual installation is needed.
FROM tabulario/spark-iceberg:latest

USER root
WORKDIR /opt/spark/work-dir

# Copy the shaded JAR from the builder stage
COPY --from=builder /build/target/ingestion-worker-1.0.0.jar ./ingestion-worker.jar

# uid 185 exists in the base image but has no /etc/passwd entry,
# which makes Hadoop's UserGroupInformation crash with
# "KerberosAuthException: invalid null input: name".
RUN echo "spark:x:185:0:Spark User:/opt/spark/work-dir:/bin/bash" >> /etc/passwd && \
    mkdir -p /opt/spark/work-dir/.ivy2 && \
    chown -R 185:0 /opt/spark/work-dir && \
    mkdir -p /home/iceberg/spark-events && \
    chown -R 185:0 /home/iceberg/spark-events

COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

ENV SPARK_HOME=/opt/spark
ENV HADOOP_USER_NAME=spark

# Drop privileges: uid 185 is the pre-created 'spark' user in the base image
USER 185

ENTRYPOINT ["/opt/spark/work-dir/entrypoint.sh"]
