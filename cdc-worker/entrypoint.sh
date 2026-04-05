#!/bin/bash
set -euo pipefail

# Ivy (Spark's dependency resolver) resolves its local cache via user.home,
# which Java reads from /etc/passwd — not the $HOME env var.
export HOME=/opt/spark/work-dir

echo "========================================"
echo "  CDC Worker Starting"
echo "========================================"
echo "  Kafka Bootstrap  : ${KAFKA_BOOTSTRAP_SERVERS}"
echo "  Redis Host       : ${REDIS_HOST}"
echo "  Iceberg Catalog  : ${ICEBERG_CATALOG_URI}"
echo "  MinIO Endpoint   : ${MINIO_ENDPOINT}"
echo "  Warehouse        : ${ICEBERG_WAREHOUSE}"
echo "  CDC Namespace    : ${CDC_TARGET_NAMESPACE}"
echo "  Trigger Interval : ${CDC_TRIGGER_INTERVAL}"
echo "========================================"

# exec replaces the shell with spark-submit — signals (SIGTERM, SIGINT)
# go directly to the JVM instead of being swallowed by bash.
exec /opt/spark/bin/spark-submit \
  --class com.abdelwahab.cdc_worker.CdcWorkerMain \
  --master local[*] \
  --driver-memory 2g \
  --driver-java-options "-Duser.name=spark -Duser.home=/opt/spark/work-dir" \
  \
  `# ── Ivy: point the local repo at an absolute writable path ─────────────` \
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
  `# ── Disable Iceberg CachingCatalog (ensures immediate schema visibility) ` \
  --conf spark.sql.catalog.iceberg.cache-enabled=false \
  \
  `# ── S3A / MinIO for checkpoints and data ────────────────────────────────` \
  --conf spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT} \
  --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
  --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.region=${AWS_REGION} \
  \
  /opt/spark/work-dir/cdc-worker.jar "$@"
