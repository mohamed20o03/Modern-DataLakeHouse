# Quickstart: CDC Pipeline & Iceberg Compaction

**Feature**: 001-cdc-pipeline-compaction  
**Prerequisites**: Docker, Docker Compose

## 1. Start All Services

```bash
docker compose up -d
```

This starts all existing services PLUS the new ones:
- `source-postgres` — Source database with sample data and logical replication
- `kafka` — KRaft broker (no Zookeeper)
- `debezium-connect` — Kafka Connect with Debezium PostgreSQL connector
- `cdc-worker` — Spark Structured Streaming application
- `register-connector` — Init container that registers the Debezium connector

Wait ~60 seconds for all services to become healthy.

## 2. Verify Source Database

```bash
# Check sample data (10 seed rows)
docker exec source-postgres psql -U debezium -d sourcedb -c "SELECT id, name, email, status FROM customers ORDER BY id;"
```

Expected output:
```
 id |      name       |            email             |  status
----+-----------------+------------------------------+-----------
  1 | Ahmed Hassan    | ahmed.hassan@example.com     | active
  2 | Fatma Ali       | fatma.ali@example.com        | active
  3 | Mohamed Ibrahim | mohamed.ibrahim@example.com  | active
  4 | Sara Mahmoud    | sara.mahmoud@example.com     | inactive
  5 | Omar Khaled     | omar.khaled@example.com      | active
  6 | Nour El-Din     | nour.eldin@example.com       | active
  7 | Yasmin Tarek    | yasmin.tarek@example.com     | suspended
  8 | Karim Mostafa   | karim.mostafa@example.com    | active
  9 | Hana Saeed      | hana.saeed@example.com       | active
 10 | Amr Fathy       | amr.fathy@example.com        | inactive
(10 rows)
```

```bash
# Verify logical replication is enabled
docker exec source-postgres psql -U debezium -d sourcedb -c "SHOW wal_level;"
# Expected: logical
```

## 3. Verify CDC Pipeline

### Check Debezium Connector

```bash
curl -s http://localhost:8083/connectors/source-postgres-connector/status | python3 -m json.tool
```

Expected: connector `RUNNING`, task `RUNNING`.

### Check Kafka Topic

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep cdc
# Expected: cdc.public.customers
```

### Check CDC Worker

```bash
docker logs cdc-worker 2>&1 | grep "MERGE INTO completed"
# Expected: Batch 0 — MERGE INTO completed successfully
```

### Verify Initial Snapshot in Iceberg

```bash
# All 10 seed rows should be in the Iceberg table
docker exec cdc-worker /opt/spark/bin/spark-sql \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.iceberg.type=rest" \
  --conf "spark.sql.catalog.iceberg.uri=http://iceberg-rest-catalog:8181" \
  --conf "spark.sql.catalog.iceberg.warehouse=s3://warehouse" \
  --conf "spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  --conf "spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000" \
  --conf "spark.sql.catalog.iceberg.s3.path-style-access=true" \
  --conf "spark.sql.catalog.iceberg.s3.access-key-id=admin" \
  --conf "spark.sql.catalog.iceberg.s3.secret-access-key=changeme_in_production" \
  --conf "spark.sql.defaultCatalog=iceberg" \
  -e "SELECT id, name, status, _cdc_op FROM cdc_namespace.customers ORDER BY id;" 2>/dev/null
```

## 4. Test INSERT → UPDATE → DELETE

```bash
# INSERT a new row
docker exec source-postgres psql -U debezium -d sourcedb -c \
  "INSERT INTO customers (name, email, status) VALUES ('Test User', 'test@example.com', 'active');"

# UPDATE a row
docker exec source-postgres psql -U debezium -d sourcedb -c \
  "UPDATE customers SET email = 'updated@example.com', status = 'premium' WHERE id = 1;"

# DELETE a row
docker exec source-postgres psql -U debezium -d sourcedb -c \
  "DELETE FROM customers WHERE id = 10;"
```

Wait ~30 seconds (one trigger interval), then query the Iceberg table to verify:
- **INSERT**: new row with `_cdc_op = c`
- **UPDATE**: id=1 has updated email/status, `_cdc_op = u`
- **DELETE**: id=10 is no longer in results

## 5. Run Compaction (Manual)

After several micro-batches create small files:

```bash
# Check file count before compaction
docker exec cdc-worker /opt/spark/bin/spark-sql \
  <same catalog configs as above> \
  -e "SELECT count(*) as file_count FROM cdc_namespace.customers.files WHERE content = 0;" 2>/dev/null

# Run compaction
docker compose run --rm cdc-worker --compact iceberg.cdc_namespace.customers

# Check file count after — should be reduced
docker exec cdc-worker /opt/spark/bin/spark-sql \
  <same catalog configs as above> \
  -e "SELECT count(*) as file_count FROM cdc_namespace.customers.files WHERE content = 0;" 2>/dev/null
```

## 6. Tag a Snapshot

```bash
# Tag the current snapshot for ML training
docker compose run --rm cdc-worker --tag iceberg.cdc_namespace.customers training-v1

# Verify the tag exists
docker exec cdc-worker /opt/spark/bin/spark-sql \
  <same catalog configs as above> \
  -e "SELECT * FROM cdc_namespace.customers.refs;" 2>/dev/null
```

## 7. Expire Snapshots (Tagged Survive)

```bash
# Expire all untagged snapshots
docker compose run --rm cdc-worker --expire-snapshots iceberg.cdc_namespace.customers 0

# Verify tagged snapshot still exists
docker exec cdc-worker /opt/spark/bin/spark-sql \
  <same catalog configs as above> \
  -e "SELECT * FROM cdc_namespace.customers.refs;" 2>/dev/null
```

## Environment Variables (New)

| Variable | Default | Description |
|----------|---------|-------------|
| `SOURCE_POSTGRES_HOST` | `source-postgres` | Source database hostname |
| `SOURCE_POSTGRES_PORT` | `5432` | Source database port |
| `SOURCE_POSTGRES_DB` | `sourcedb` | Source database name |
| `SOURCE_POSTGRES_USER` | `debezium` | Source database user |
| `SOURCE_POSTGRES_PASSWORD` | `debezium` | Source database password |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address |
| `CDC_TOPIC_PREFIX` | `cdc` | Debezium topic prefix |
| `CDC_TABLE_INCLUDE_LIST` | `public.customers` | Tables to capture |
| `CDC_CHECKPOINT_DIR` | `s3a://warehouse/checkpoints/cdc` | Spark checkpoint location |
| `CDC_TRIGGER_INTERVAL` | `30 seconds` | Spark micro-batch interval |
| `CDC_TARGET_NAMESPACE` | `cdc_namespace` | Iceberg namespace for CDC tables |
| `COMPACTION_TARGET_FILE_SIZE_MB` | `128` | Target file size for compaction |
| `SNAPSHOT_RETAIN_DAYS` | `7` | Default snapshot retention |
