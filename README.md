# Data Lakehouse Platform

A distributed data lakehouse platform with **REST ingestion**, **SQL querying**, **schema discovery**, **progressive result streaming**, and **real-time CDC (Change Data Capture)** — built on **Apache Spark**, **Apache Iceberg**, **Kafka**, **Debezium**, **RabbitMQ**, **MinIO**, and **Redis**.

> **Graduation Project** — demonstrates scalable data pipeline design with measurable performance optimizations.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Services Overview](#services-overview)
3. [Quick Start](#quick-start)
4. [Documentation](#documentation)
5. [API Reference](#api-reference)
6. [Workers](#workers)
7. [Storage Layer](#storage-layer)
8. [Performance Optimizations](#performance-optimizations)
9. [Environment Variables](#environment-variables)
10. [Troubleshooting](#troubleshooting)

---

## Architecture

```
  ┌────────────────────┐
  │  SOURCE POSTGRES   │   (wal_level = logical)
  │  :5432             │
  │  customers table   │
  └────────┬───────────┘
           │  WAL stream (pgoutput)
           ▼
  ┌────────────────────┐         ┌─────────────────────────────────────────┐
  │ DEBEZIUM CONNECT   │────────▶│              KAFKA (KRaft)              │
  │  :8083             │  CDC    │  :9092 / :29092                         │
  │  pgoutput plugin   │ events  │  topic: cdc.public.customers            │
  └────────────────────┘         └─────────────────┬───────────────────────┘
                                                   │
                                                   ▼
                                    ┌──────────────────────┐
                                    │    CDC WORKER        │
                                    │  Spark Structured    │
                                    │  Streaming + Iceberg │
                                    │                      │
                                    │  • Kafka consumer    │
                                    │  • Debezium parser   │
                                    │  • Deduplication     │
                                    │  • MERGE INTO (upsert│
                                    │    + delete)         │
                                    │  • Compaction        │
                                    │  • Snapshot tagging  │
                                    └──────────┬───────────┘
                                               │
┌──────────────────────────────────────────────────────────────────────────────┐
│                          API SERVICE  :8080                                  │
│                                                                              │
│  POST /api/v1/ingestion/upload          GET /api/v1/ingestion/status/{id}    │
│  POST /api/v1/query                     GET /api/v1/query/{id}               │
│  GET  /api/v1/schema/{source}           GET /api/v1/schema/status/{id}       │
│                                                                              │
│  GET  /api/v1/*/wait  ← long-poll endpoint (Redis Pub/Sub, no polling loop)  │
│  GET  /api/v1/query/{id}/stream  ← SSE endpoint (Redis Streams → client)     │
└──────────────┬──────────────────────────────┬────────────────────────────────┘
               │  ingestion.queue             │  query.queue (priority: 0-10)
               ▼                              ▼
        ┌─────────────────────────────────────────┐
        │              RabbitMQ  :5672            │
        │   ingestion.queue   │   query.queue     │
        │   (data ingestion)  │   (queries+schema)│
        └────────┬────────────┴────────┬──────────┘
                 │                     │
                 ▼                     ▼
  ┌──────────────────────┐  ┌───────────────────────┐
  │  INGESTION WORKER    │  │    QUERY WORKER       │
  │  Spark 3.5 + Iceberg │  │  Spark 3.5 + Iceberg  │
  │                      │  │                       │
  │  • Reads CSV/JSON/   │  │  • Executes SQL query │
  │    Parquet/Avro from │  │    on Iceberg table   │
  │    MinIO staging     │  │  • Schema discovery   │
  │  • Writes Iceberg    │  │  • Inline (≤ 1 000)   │
  │    table (append     │  │  • Streams via Redis  │
  │    with schema       │  │    Streams (> 1 000)  │
  │    evolution)        │  │  • Priority: schema=8 │
  │  • Invalidates Redis │  │    queries=1          │
  │    schema cache      │  │  • Caches schema in   │
  │                      │  │    Redis after fetch  │
  └──────┬───────────────┘  └──────┬────────────────┘
         │                         │
         │    status + results     │
         ▼                         ▼
  ┌─────────────────────────────────────────────────┐
  │                    Redis  :6379                 │
  │                                                 │
  │  job:{jobId}  → Hash { status, message,         │
  │                        rowCount, resultData,    │
  │                        streamed, createdAt }    │
  │                                                 │
  │  schema:iceberg.{project}.{table}               │
  │             → JSON column list (no TTL,         │
  │               event-driven invalidation)        │
  │                                                 │
  │  query-result-stream:{jobId}                    │
  │             → Stream entries (TTL: 1 h)         │
  │               type=metadata|batch|complete      │
  │                                                 │
  │  Pub/Sub channel: job-done:{jobId}              │
  │    Workers PUBLISH → API /wait resolves         │
  └─────────────────────────────────────────────────┘
         │ metadata
         ▼
  ┌──────────────────┐       ┌──────────────────────┐
  │  Iceberg REST    │       │        MinIO         │
  │  Catalog :8181   │       │   :9000 / :9001      │
  │  (PostgreSQL     │       │                      │
  │   backed)        │       │  s3://warehouse/     │
  └──────────────────┘       │    Iceberg Parquet   │
                             │  s3://staging-*/     │
                             │    raw uploads       │
                             └──────────────────────┘
```

---

## Services Overview

| Service                | Image                    | Port(s)     | Purpose                                   |
| ---------------------- | ------------------------ | ----------- | ----------------------------------------- |
| `api-service`          | custom (Spring Boot 3)   | 8080        | REST gateway — upload, query, schema      |
| `ingestion-worker`     | custom (Spark + Iceberg) | —           | Spark job: file → Iceberg table           |
| `query-worker`         | custom (Spark + Iceberg) | —           | Spark job: SQL queries + schema discovery |
| `cdc-worker`           | custom (Spark + Iceberg) | —           | CDC: Kafka → Iceberg (MERGE INTO)         |
| `source-postgres`      | postgres:15-alpine       | 5433        | Source database with logical replication   |
| `kafka`                | apache/kafka (KRaft)     | 9092        | Event streaming (Debezium CDC events)      |
| `debezium-connect`     | debezium/connect:2.5     | 8083        | PostgreSQL CDC connector (pgoutput)        |
| `minio`                | minio/minio              | 9000, 9001  | Object storage (uploads + Parquet files)  |
| `rabbitmq`             | rabbitmq:3-management    | 5672, 15672 | Priority message queue                    |
| `redis`                | redis:7-alpine           | 6379        | Job status, schema cache, Pub/Sub events  |
| `catalog-postgres`     | postgres:15-alpine       | 5432        | Iceberg catalog metadata (table registry) |
| `iceberg-rest-catalog` | tabulario/iceberg-rest   | 8181        | Iceberg REST Catalog API                  |

---

## Quick Start

### 1. Start all services

```bash
docker compose up -d
```

### 2. Verify all services are healthy

```bash
docker compose ps
```

Expected output — all services show `healthy`:

```
NAME                   STATUS
api-service            Up (healthy)
catalog-postgres       Up (healthy)
iceberg-rest-catalog   Up (healthy)
kafka                  Up (healthy)
source-postgres        Up (healthy)
debezium-connect       Up (healthy)
minio                  Up (healthy)
rabbitmq               Up (healthy)
redis                  Up (healthy)
ingestion-worker       Up
query-worker           Up
cdc-worker             Up
```

### 3. Quick health checks

```bash
# API service
curl http://localhost:8080/actuator/health

# Iceberg catalog
curl http://localhost:8181/v1/config

# RabbitMQ management UI → http://localhost:15672  (admin / changeme_in_production)
# MinIO console         → http://localhost:9001   (admin / changeme_in_production)
```

### 4. Run the full optimization test suite

```bash
bash test_system.sh
```

### Stop services

```bash
docker compose down        # stop, keep data volumes
docker compose down -v     # stop + delete all volumes  ⚠️ data loss
```

---

## Documentation

| Document                                                                     | Audience                         | Contents                                                                                     |
| ---------------------------------------------------------------------------- | -------------------------------- | -------------------------------------------------------------------------------------------- |
| [docs/API_REFERENCE.md](docs/API_REFERENCE.md)                               | **Frontend / client developers** | All endpoints, request/response shapes, Query DSL, TypeScript types, JS integration examples |
| [docs/QUERY_FLOW.md](docs/QUERY_FLOW.md)                                     | Backend, contributors            | End-to-end trace of a query from HTTP → RabbitMQ → Spark → Redis → SSE                       |
| [docs/PERFORMANCE.md](docs/PERFORMANCE.md)                                   | Backend, contributors            | Detailed description of the 5 performance optimizations with measured results                |
| [docs/ENHANCEMENT_GUIDE.md](docs/ENHANCEMENT_GUIDE.md)                       | Contributors                     | Conceptual guide for adding partitioning, compaction, and pagination                         |
| [docs/PARTITIONING_ALGORITHM.md](docs/PARTITIONING_ALGORITHM.md)             | Contributors                     | Auto-partitioning algorithm design and decision logic                                        |
| [query-worker/ENGINE_README.md](query-worker/ENGINE_README.md)               | Contributors                     | Spark engine layer internals — SparkService, QueryBuilder, ResultWriter                      |
| [ingestion-worker/DOCKER_DEBUGGING.md](ingestion-worker/DOCKER_DEBUGGING.md) | DevOps                           | Docker container setup issues and fixes for the ingestion worker                             |

---

## API Reference

Base URL: `http://localhost:8080`

> For the full frontend integration guide including request/response shapes, Query DSL, TypeScript types, and JavaScript examples, see **[docs/API_REFERENCE.md](docs/API_REFERENCE.md)**.

Job status lifecycle:

```
PENDING → QUEUED → PROCESSING → COMPLETED
                              ↘ FAILED
```

### Endpoint overview

| Method | Endpoint                                | Description                            |
| ------ | --------------------------------------- | -------------------------------------- |
| POST   | `/api/v1/ingestion/upload`              | Upload a file and start ingestion      |
| GET    | `/api/v1/ingestion/status/{jobId}`      | Poll ingestion job status              |
| GET    | `/api/v1/ingestion/status/{jobId}/wait` | Long-poll — blocks until terminal      |
| POST   | `/api/v1/query`                         | Submit a structured query              |
| GET    | `/api/v1/query/{jobId}`                 | Poll query job status + inline results |
| GET    | `/api/v1/query/{jobId}/wait`            | Long-poll — blocks until terminal      |
| GET    | `/api/v1/query/{jobId}/stream`          | SSE stream — progressive row delivery  |
| GET    | `/api/v1/schema/{source}`               | Request schema discovery for a table   |
| GET    | `/api/v1/schema/status/{jobId}`         | Poll schema job status + column list   |
| GET    | `/api/v1/schema/status/{jobId}/wait`    | Long-poll — blocks until terminal      |

> For full request/response details, the Query DSL, TypeScript types, and JavaScript examples, see **[docs/API_REFERENCE.md](docs/API_REFERENCE.md)**.

---

## Workers

### Ingestion Worker

Consumes `IngestionMessage` from `ingestion.queue`:

1. Reads the file from MinIO staging bucket using Spark (`s3a://`)
2. Sanitizes column names for Parquet compatibility
3. Detects whether the Iceberg table exists:
   - **New table** → `CREATE TABLE` + write
   - **Existing table** → align schemas bidirectionally (add missing columns as typed nulls), then `APPEND`
4. Writes data to `iceberg.{projectId}.{tableName}`
5. Invalidates Redis schema cache for that table
6. Publishes `job-done:{jobId}` event to Redis Pub/Sub
7. Updates job status: `PROCESSING → COMPLETED / FAILED`

**Column name sanitization:**

| Raw header      | Sanitized       |
| --------------- | --------------- |
| `First Name`    | `first_name`    |
| `Revenue (USD)` | `revenue_usd`   |
| `col.with.dots` | `col_with_dots` |

**Spark write tuning:**

| Setting                  | Value                                                              |
| ------------------------ | ------------------------------------------------------------------ |
| Dynamic partitions       | $\lceil\text{fileSize} \div 64\text{ MiB}\rceil$ clamped to [1, 8] |
| Target Iceberg file size | 128 MiB                                                            |
| Schema merge             | `mergeSchema = true`                                               |
| Catalog caching          | Disabled (`cache-enabled = false`)                                 |

### Query Worker

Consumes `QueryMessage` from `query.queue` (priority queue, 0–10):

1. Builds a Spark SQL statement from the structured query request
2. Executes it against the Iceberg table
3. If `rowCount ≤ 1 000` → serializes result to JSON inline (stored in Redis status hash under `resultData`)
4. If `rowCount > 1 000` → streams result in batches of 500 rows via `toLocalIterator()` to a Redis Stream (`query-result-stream:{jobId}`, TTL: 1 h) — no MinIO write
5. Publishes `job-done:{jobId}` event to Redis Pub/Sub
6. Updates job status with result metadata (`streamed: true` for large results)

Schema jobs run on the same worker but at **priority 8** (vs. query priority 1), so they are never blocked by a backlog of data queries.

### CDC Worker

Continuously syncs the source PostgreSQL database to the Iceberg lakehouse via Change Data Capture:

1. Consumes CDC events from Kafka topic `cdc.public.customers` (produced by Debezium)
2. Parses Debezium envelope (extracts `op`, `before`/`after`, `ts_ms`)
3. Deduplicates events within each micro-batch (keeps latest per primary key)
4. Executes `MERGE INTO` on the target Iceberg table:
   - `op = c` (create) → INSERT
   - `op = u` (update) → UPDATE SET
   - `op = r` (read/snapshot) → UPSERT
   - `op = d` (delete) → DELETE
5. Reports status to Redis

**Maintenance operations** (CLI):

| Command | Description |
|---------|-------------|
| `docker compose run cdc-worker --compact <table>` | Bin-pack compaction (merges small files) |
| `docker compose run cdc-worker --tag <table> <tagName> [snapshotId]` | Tag a snapshot for ML training |
| `docker compose run cdc-worker --expire-snapshots <table> [retainDays]` | Remove old snapshots (tagged survive) |

**CDC tuning:**

| Setting | Value |
|---------|-------|
| Trigger interval | 30 seconds (configurable via `CDC_TRIGGER_INTERVAL`) |
| Iceberg format version | 2 (row-level deletes) |
| Write mode | Merge-on-read |
| Compaction strategy | Bin-pack (min 2 input files, target 128 MiB) |

---

## Storage Layer

Three stores with three different roles:

| Store          | Content                                         | Why                                  | Retention                                    |
| -------------- | ----------------------------------------------- | ------------------------------------ | -------------------------------------------- |
| **PostgreSQL** | Iceberg table/snapshot metadata                 | ACID coordination, catalog registry  | Permanent                                    |
| **MinIO**      | Parquet data files (Iceberg only)               | Cheap S3-compatible object storage   | Permanent                                    |
| **Redis**      | Job status hashes, schema cache, result streams | Sub-millisecond lookups, auto-expiry | 1 h (jobs + streams) / event-driven (schema) |

**MinIO bucket layout:**

```
s3://warehouse/
  {projectId}/{tableName}/
    data/         ← Iceberg Parquet data files
    metadata/     ← snapshots, manifests, table metadata JSON

s3://staging-applicationarea/
  ingestion/{jobId}/file.csv    ← raw uploaded files
```

---

## Performance Optimizations

Five optimizations were implemented and validated end-to-end. See **[docs/PERFORMANCE.md](docs/PERFORMANCE.md)** for the full analysis including measured results for each optimization.

| Optimization          | Technique                                         | Key result                            |
| --------------------- | ------------------------------------------------- | ------------------------------------- |
| #1 Spark warm-up      | `SELECT 1` at startup                             | Cold-start: 10 s → 3.8 s              |
| #2 Schema Redis cache | Cache schema JSON, invalidate on ingest           | Schema hit: 185 ms → 10 ms (18×)      |
| #3 Priority queue     | Schema at priority 8, queries at priority 1       | Schema latency predictable under load |
| #4 Redis Pub/Sub      | `/wait` long-poll replaces client polling         | 5 000 ms polling wait → ~374 ms       |
| #5 Inline + SSE       | ≤1 000 rows inline, >1 000 rows via Redis Streams | No MinIO write for any query          |

### End-to-end results (all optimizations combined)

| Test                                  | Worker time | Notes                                      |
| ------------------------------------- | ----------- | ------------------------------------------ |
| T1 Initial ingestion (1 000 rows)     | 489 ms      | warm session — no cold-start penalty       |
| T2 Append ingestion (500 rows)        | 636 ms      | subsequent job on warm session             |
| T3 Schema MISS (→ Spark)              | 24 ms       | cache empty, fetches from Iceberg          |
| T4 Schema HIT (→ Redis)               | 13 ms       | served from Redis (~2× faster)             |
| T5 Ingestion invalidates cache        | 582 ms      | DEL called by ingestion-worker             |
| T6 Schema MISS (post-ingest)          | 37 ms       | re-fetches updated 6-column schema         |
| T7 Schema HIT (re-cached)             | 29 ms       | served from Redis again                    |
| T8 SELECT \* LIMIT 100                | 237 ms      | warm Spark + inline result                 |
| T9 WHERE + ORDER BY LIMIT 50          | 124 ms      | warm Spark + inline result                 |
| T10 SELECT \* all 1 600 rows (stream) | 227 ms      | 4 SSE batches × 500 rows, SSE read: 215 ms |
| T11 GROUP BY aggregation              | 396 ms      | warm Spark + inline result                 |

---

## Environment Variables

Configured in `docker-compose.yml` (override in `.env` at project root).

### MinIO

```env
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=changeme_in_production
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=changeme_in_production
MINIO_BUCKET_UPLOADS=staging-applicationarea
MINIO_BUCKET_WAREHOUSE=warehouse
```

### RabbitMQ

```env
RABBITMQ_DEFAULT_USER=admin
RABBITMQ_DEFAULT_PASS=changeme_in_production
RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672
```

### Redis

```env
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=changeme_in_production
```

### PostgreSQL (Iceberg Catalog)

```env
POSTGRES_USER=iceberg
POSTGRES_PASSWORD=iceberg_password
POSTGRES_DB=iceberg_catalog
```

### Source PostgreSQL (CDC)

```env
SOURCE_POSTGRES_HOST=source-postgres
SOURCE_POSTGRES_PORT=5432
SOURCE_POSTGRES_DB=sourcedb
SOURCE_POSTGRES_USER=debezium
SOURCE_POSTGRES_PASSWORD=debezium
```

### Kafka

```env
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```

### CDC Worker

```env
CDC_TOPIC_PREFIX=cdc
CDC_TABLE_INCLUDE_LIST=public.customers
CDC_TARGET_NAMESPACE=cdc_namespace
CDC_TRIGGER_INTERVAL=30 seconds
CDC_CHECKPOINT_DIR=s3a://warehouse/checkpoints/cdc
COMPACTION_TARGET_FILE_SIZE_MB=128
SNAPSHOT_RETAIN_DAYS=7
```

### Iceberg

```env
ICEBERG_CATALOG_URI=http://iceberg-rest-catalog:8181
ICEBERG_WAREHOUSE=s3://warehouse
AWS_REGION=us-east-1
```

---

## Troubleshooting

### Worker not consuming messages

```bash
docker compose logs ingestion-worker | grep -E "ERROR|RabbitMQ"
docker compose logs query-worker | grep -E "ERROR|RabbitMQ"
```

### Iceberg catalog errors

```bash
curl http://localhost:8181/v1/config
docker compose logs iceberg-rest-catalog
```

### Redis connection issues

```bash
docker exec redis redis-cli -a changeme_in_production ping
```

### Schema cache stuck (stale columns after ingestion)

```bash
# Manually flush the cache for a specific table
docker exec redis redis-cli -a changeme_in_production DEL "schema:iceberg.{projectId}.{tableName}"
```

### Restart a specific service

```bash
docker compose restart <service-name>
```

### Service startup order

Services start in dependency order — Docker waits for each health check:

```
minio + redis + rabbitmq + catalog-postgres + source-postgres + kafka
               ↓
          minio-setup  (creates buckets)
               ↓
       iceberg-rest-catalog + debezium-connect
               ↓
    api-service + ingestion-worker + query-worker + cdc-worker
               ↓
    register-connector (init container → registers Debezium connector)
```

### Force rebuild after source changes

Docker caches the `COPY src` layer by content hash. If your changes are not being picked up:

```bash
# Touch the changed file to bust the layer cache, then rebuild
touch ingestion-worker/src/.../ChangedFile.java
docker compose up -d --build ingestion-worker
```
