#!/bin/bash
set -euo pipefail

# Ivy (Spark's dependency resolver) resolves its local cache via user.home,
# which Java reads from /etc/passwd — not the $HOME env var.
# Setting HOME here ensures both the env var and any bash-level path resolution
# point to a real writable directory.
export HOME=/opt/spark/work-dir

echo "========================================"
echo "  Ingestion Worker Starting"
echo "========================================"
echo "  RabbitMQ Host     : ${RABBITMQ_HOST}"
echo "  Redis Host        : ${REDIS_HOST}"
echo "  Iceberg Catalog   : ${ICEBERG_CATALOG_URI}"
echo "  MinIO Endpoint    : ${MINIO_ENDPOINT}"
echo "  Warehouse         : ${ICEBERG_WAREHOUSE}"
echo "========================================"

# exec replaces the shell with spark-submit — signals (SIGTERM, SIGINT)
# go directly to the JVM instead of being swallowed by bash.
exec /opt/spark/bin/spark-submit \
  --class com.abdelwahab.ingestion_worker.IngestionWorkerMain \
  --master local[*] \
  --driver-memory 2g \
  --driver-java-options "-Duser.name=spark -Duser.home=/opt/spark/work-dir" \
  \
  `# ── Ivy: point the local repo at an absolute writable path ─────────────` \
  `# Without this, Ivy falls back to user.home/.ivy2 from /etc/passwd,     ` \
  `# which may be '?' or unresolvable for uid 185 (spark).                 ` \
  --conf spark.jars.ivy=/opt/spark/work-dir/.ivy2 \
  \
  `# ── Iceberg SQL extensions ──────────────────────────────────────────────` \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  \
  `# ── Iceberg REST catalog ────────────────────────────────────────────────` \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=rest \
  --conf spark.sql.catalog.iceberg.uri=${ICEBERG_CATALOG_URI} \
  --conf spark.sql.catalog.iceberg.warehouse=${ICEBERG_WAREHOUSE} \
  --conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.iceberg.s3.endpoint=${MINIO_ENDPOINT} \
  --conf spark.sql.catalog.iceberg.s3.path-style-access=true \
  --conf spark.sql.catalog.iceberg.s3.access-key-id=${AWS_ACCESS_KEY_ID} \
  --conf spark.sql.catalog.iceberg.s3.secret-access-key=${AWS_SECRET_ACCESS_KEY} \
  --conf spark.sql.catalog.iceberg.client.region=${AWS_REGION} \
  --conf spark.sql.defaultCatalog=iceberg \
  \
  `# ── S3A / MinIO for reading uploaded files ──────────────────────────────` \
  --conf spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT} \
  --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
  --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.region=${AWS_REGION} \
  \
  /opt/spark/work-dir/ingestion-worker.jar
