# Data Lakehouse Platform

A distributed data lakehouse platform with **REST ingestion**, **SQL querying**, and **schema discovery** — built on **Apache Spark**, **Apache Iceberg**, **RabbitMQ**, **MinIO**, and **Redis**.

> **Graduation Project** — demonstrates scalable data pipeline design with measurable performance optimizations.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Services Overview](#services-overview)
3. [Quick Start](#quick-start)
4. [API Reference](#api-reference)
   - [Ingestion API](#ingestion-api)
   - [Query API](#query-api)
   - [Schema API](#schema-api)
   - [Long-poll /wait Endpoints](#long-poll-wait-endpoints)
5. [Workers](#workers)
6. [Storage Layer](#storage-layer)
7. [Performance Optimizations](#performance-optimizations)
8. [Environment Variables](#environment-variables)
9. [Troubleshooting](#troubleshooting)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          API SERVICE  :8080                                   │
│                                                                               │
│  POST /api/v1/ingestion/upload          GET /api/v1/ingestion/status/{id}    │
│  POST /api/v1/query                     GET /api/v1/query/{id}               │
│  GET  /api/v1/schema/{source}           GET /api/v1/schema/status/{id}       │
│                                                                               │
│  GET  /api/v1/*/wait  ← long-poll endpoint (Redis Pub/Sub, no polling loop)  │
└──────────────┬──────────────────────────────┬───────────────────────────────┘
               │  ingestion.queue              │  query.queue (priority: 0-10)
               ▼                              ▼
        ┌─────────────────────────────────────────┐
        │              RabbitMQ  :5672             │
        │   ingestion.queue   │   query.queue      │
        │   (data ingestion)  │   (queries+schema) │
        └────────┬────────────┴────────┬───────────┘
                 │                     │
                 ▼                     ▼
  ┌──────────────────────┐  ┌──────────────────────┐
  │  INGESTION WORKER    │  │    QUERY WORKER       │
  │  Spark 3.5 + Iceberg │  │  Spark 3.5 + Iceberg  │
  │                      │  │                       │
  │  • Reads CSV/JSON/   │  │  • Executes SQL query │
  │    Parquet/Avro from │  │    on Iceberg table   │
  │    MinIO staging     │  │  • Schema discovery   │
  │  • Writes Iceberg    │  │  • Inline results     │
  │    table (append     │  │    for small datasets │
  │    with schema       │  │  • Priority: schema=8 │
  │    evolution)        │  │    queries=1          │
  │  • Invalidates Redis │  │  • Caches schema in   │
  │    schema cache      │  │    Redis after fetch  │
  └──────┬───────────────┘  └──────┬────────────────┘
         │                         │
         │    status + results      │
         ▼                         ▼
  ┌────────────────────────────────────────────────┐
  │                    Redis  :6379                 │
  │                                                 │
  │  job:{jobId}  → Hash { status, message,         │
  │                        rowCount, resultData,    │
  │                        createdAt, updatedAt }   │
  │                                                 │
  │  schema:iceberg.{project}.{table}               │
  │             → JSON column list (no TTL,         │
  │               event-driven invalidation)        │
  │                                                 │
  │  Pub/Sub channel: job-done:{jobId}              │
  │    Workers PUBLISH → API /wait resolves         │
  └────────────────────────────────────────────────┘
         │ metadata
         ▼
  ┌──────────────────┐       ┌──────────────────────┐
  │  Iceberg REST    │       │        MinIO          │
  │  Catalog :8181   │       │   :9000 / :9001       │
  │  (PostgreSQL     │       │                       │
  │   backed)        │       │  s3://warehouse/      │
  └──────────────────┘       │    Iceberg Parquet    │
                             │  s3://staging-*/      │
                             │    raw uploads        │
                             │    query results      │
                             └──────────────────────┘
```

---

## Services Overview

| Service                | Image                    | Port(s)     | Purpose                                   |
| ---------------------- | ------------------------ | ----------- | ----------------------------------------- |
| `api-service`          | custom (Spring Boot 3)   | 8080        | REST gateway — upload, query, schema      |
| `ingestion-worker`     | custom (Spark + Iceberg) | —           | Spark job: file → Iceberg table           |
| `query-worker`         | custom (Spark + Iceberg) | —           | Spark job: SQL queries + schema discovery |
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
minio                  Up (healthy)
rabbitmq               Up (healthy)
redis                  Up (healthy)
ingestion-worker       Up
query-worker           Up
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

## API Reference

Base URL: `http://localhost:8080`

All status responses share the same structure:

```json
{
  "status": "COMPLETED",
  "message": "...",
  "createdAt": "2026-02-26T12:00:00Z",
  "updatedAt": "2026-02-26T12:00:03Z"
}
```

Status lifecycle for every job type:

```
PENDING → QUEUED → PROCESSING → COMPLETED
                              → FAILED
```

---

### Ingestion API

#### Upload a file

**`POST /api/v1/ingestion/upload`** — `multipart/form-data`

| Parameter   | Type   | Required | Description                                           |
| ----------- | ------ | :------: | ----------------------------------------------------- |
| `file`      | file   |    ✅    | Data file (CSV, JSON, Parquet, Avro — max 500 MB)     |
| `userId`    | string |    ✅    | Your user identifier                                  |
| `projectId` | string |    ✅    | Project namespace — becomes the Iceberg namespace     |
| `tableName` | string |    ❌    | Target table (defaults to filename without extension) |

```bash
curl -X POST http://localhost:8080/api/v1/ingestion/upload \
  -F "file=@sales.csv" \
  -F "userId=user_001" \
  -F "projectId=my_project" \
  -F "tableName=sales"
```

Response:

```json
{
  "jobId": "9622371d-719c-4492-9e37-f6ba464ce63a",
  "status": "QUEUED",
  "checkStatusAt": "/api/v1/ingestion/status/9622371d-719c-4492-9e37-f6ba464ce63a"
}
```

#### Track ingestion status

**`GET /api/v1/ingestion/status/{jobId}`**

```bash
curl http://localhost:8080/api/v1/ingestion/status/9622371d-...
```

#### Wait for ingestion (no polling)

**`GET /api/v1/ingestion/status/{jobId}/wait?timeoutSec=30`**

Blocks until the job completes or the timeout expires. See [Long-poll /wait Endpoints](#long-poll-wait-endpoints).

---

### Query API

#### Submit a query

**`POST /api/v1/query`** — `application/json`

| Field     | Type   | Description                                 |
| --------- | ------ | ------------------------------------------- |
| `source`  | string | `"projectId.tableName"` — the Iceberg table |
| `select`  | array  | Columns to select (use `"*"` for all)       |
| `filters` | array  | WHERE conditions                            |
| `orderBy` | array  | ORDER BY columns with direction             |
| `groupBy` | array  | GROUP BY columns                            |
| `limit`   | int    | Max rows to return                          |

```bash
# SELECT * LIMIT 100
curl -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "source": "my_project.sales",
    "select": [{"column": "*"}],
    "limit": 100
  }'

# WHERE + ORDER BY
curl -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "source": "my_project.sales",
    "select": [{"column": "id"}, {"column": "price"}],
    "filters": [{"column": "price", "operator": ">", "value": 500}],
    "orderBy": [{"column": "price", "direction": "desc"}],
    "limit": 50
  }'

# GROUP BY with aggregations
curl -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "source": "my_project.sales",
    "select": [
      {"column": "product"},
      {"column": "price", "aggregation": "avg", "as": "avg_price"},
      {"column": "id",    "aggregation": "count", "as": "n"}
    ],
    "groupBy": ["product"]
  }'
```

Response:

```json
{
  "jobId": "query-abc123",
  "status": "QUEUED",
  "checkStatusAt": "/api/v1/query/query-abc123"
}
```

#### Get query result

**`GET /api/v1/query/{jobId}`**

Completed response (small result — inline):

```json
{
  "status": "COMPLETED",
  "rowCount": "5",
  "fileSizeBytes": "0",
  "resultData": "[{\"product\":\"Laptop\",\"avg_price\":517}]"
}
```

Completed response (large result — Parquet in MinIO):

```json
{
  "status": "COMPLETED",
  "rowCount": "50000",
  "fileSizeBytes": "2097152",
  "resultPath": "results/query-abc123/result.parquet"
}
```

> Results with ≤ 1 000 rows are returned **inline** in `resultData` (no MinIO file). Larger results are written to MinIO Parquet and the path is returned in `resultPath`.

#### Wait for query (no polling)

**`GET /api/v1/query/{jobId}/wait?timeoutSec=30`**

---

### Schema API

#### Request schema

**`GET /api/v1/schema/{source}`** — `source = "projectId.tableName"`

```bash
curl http://localhost:8080/api/v1/schema/my_project.sales
```

Response:

```json
{
  "jobId": "schema-xyz789",
  "status": "QUEUED",
  "checkStatusAt": "/api/v1/schema/status/schema-xyz789"
}
```

#### Get schema result

**`GET /api/v1/schema/status/{jobId}`**

```json
{
  "status": "COMPLETED",
  "rowCount": "5",
  "resultData": "[{\"name\":\"id\",\"type\":\"int\",\"nullable\":true},{\"name\":\"price\",\"type\":\"double\",\"nullable\":true}]"
}
```

#### Wait for schema (no polling)

**`GET /api/v1/schema/status/{jobId}/wait?timeoutSec=30`**

---

### Long-poll /wait Endpoints

All three job types have a `/wait` variant. Instead of polling every N seconds, a single request blocks until the job finishes:

```bash
# Submit
JOB=$(curl -s -X POST http://localhost:8080/api/v1/ingestion/upload \
  -F "file=@data.csv" -F "userId=u1" -F "projectId=demo" | jq -r .jobId)

# Wait for completion — returns the full result in one shot
curl -s --max-time 35 "http://localhost:8080/api/v1/ingestion/status/${JOB}/wait?timeoutSec=30" | jq .
```

**How it works:**

1. If the job is already `COMPLETED`/`FAILED`, returns immediately (fast-path, no subscription)
2. Otherwise, subscribes to Redis channel `job-done:{jobId}` and holds the HTTP connection open
3. When the worker publishes to that channel, the response resolves instantly
4. If the timeout expires, returns `null` — fall back to `GET /status/{jobId}`

| Parameter    | Default | Description                 |
| ------------ | ------- | --------------------------- |
| `timeoutSec` | 30      | Max seconds to wait (1–300) |

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
3. If `rowCount ≤ 1 000` → serializes result to JSON inline (skips MinIO write)
4. If `rowCount > 1 000` → writes Parquet to MinIO results bucket
5. Publishes `job-done:{jobId}` event to Redis Pub/Sub
6. Updates job status with result metadata

Schema jobs run on the same worker but at **priority 8** (vs. query priority 1), so they are never blocked by a backlog of data queries.

---

## Storage Layer

Three stores with three different roles:

| Store          | Content                           | Why                                  | Retention                          |
| -------------- | --------------------------------- | ------------------------------------ | ---------------------------------- |
| **PostgreSQL** | Iceberg table/snapshot metadata   | ACID coordination, catalog registry  | Permanent                          |
| **MinIO**      | Parquet data + query result files | Cheap S3-compatible object storage   | Permanent                          |
| **Redis**      | Job status hashes + schema cache  | Sub-millisecond lookups, auto-expiry | 1 h (jobs) / event-driven (schema) |

**MinIO bucket layout:**

```
s3://warehouse/
  {projectId}/{tableName}/
    data/         ← Iceberg Parquet data files
    metadata/     ← snapshots, manifests, table metadata JSON

s3://staging-applicationarea/
  ingestion/{jobId}/file.csv    ← raw uploaded files
  results/{jobId}/result.parquet ← large query results (>1 000 rows)
```

---

## Performance Optimizations

Five optimizations were implemented and validated end-to-end. All timings below are from a live test run on the full stack.

### Optimization #1 — Spark Session Warm-up

**Problem:** The first job after a worker restart pays a ~5 s cold-start penalty as Spark lazily initializes the Iceberg catalog, S3A filesystem, and executor pool.

**Fix:** Run `SELECT 1` immediately after `SparkSession.getOrCreate()` to force all lazy components to initialize before the first real job arrives.

```java
// SparkEngine.java
spark.sql("SELECT 1").count();  // forces catalog + executor init
```

**Measured results:**

| Job                          | Before | After  |
| ---------------------------- | ------ | ------ |
| First ingestion (1 000 rows) | ~10 s  | 3.8 s  |
| Second ingestion (500 rows)  | ~5 s   | 374 ms |

---

### Optimization #2 — Schema Redis Cache

**Problem:** Every schema request triggered a full Spark + Iceberg catalog round-trip (reads table metadata from PostgreSQL + MinIO), even though the schema only changes when new data is ingested.

**Fix:** Cache the schema JSON in Redis after the first retrieval with no TTL. Invalidate the cache key only when the ingestion worker writes new data.

```
First schema request:
  query-worker → Iceberg catalog → schema JSON → Redis (no TTL) → client

Subsequent requests:
  query-worker → Redis HIT → client  (~10 ms)

After ingestion:
  ingestion-worker → Redis DEL("schema:iceberg.{project}.{table}")
```

**Measured results:**

| Request          | Time   | Path            |
| ---------------- | ------ | --------------- |
| Schema MISS      | 185 ms | Spark + Iceberg |
| Schema HIT       | 10 ms  | Redis           |
| Post-ingest MISS | 31 ms  | Spark + Iceberg |
| Re-cached HIT    | 11 ms  | Redis           |

**~18× faster on cache hit.**

---

### Optimization #3 — RabbitMQ Priority Queue

**Problem:** Schema and data query messages shared the same queue without priority. A backlog of slow aggregation queries could block a fast schema request for many seconds.

**Fix:** Configure `x-max-priority=10` on `query.queue`. Schema messages are published at priority 8, data queries at priority 1 — so schema jobs always jump to the front of the queue.

```java
// Schema → delivered first
messageProperties.setPriority(8);

// Query → normal priority
messageProperties.setPriority(1);
```

**Effect:** Schema latency becomes **predictable** regardless of how many data queries are pending.

---

### Optimization #4 — Redis Pub/Sub (Eliminate Client Polling)

**Problem:** Clients polled `GET /status/{jobId}` every 1–5 seconds. This wastes time — a job completing in 400 ms still had to wait up to 5 seconds before the client found out. It also adds unnecessary Redis and API load.

**Fix:** Workers publish a `job-done:{jobId}` Pub/Sub event when a job reaches a terminal state. The API service exposes `/wait` long-poll endpoints backed by `DeferredResult` + a `RedisMessageListenerContainer` subscription. The HTTP response resolves the instant the event fires.

```
Worker finishes:
  Redis PUBLISH "job-done:{jobId}" "COMPLETED"
       ↓
API service (subscribed via Spring RedisMessageListenerContainer):
  reads full job Hash → resolves DeferredResult → HTTP response sent
```

**Fast-path:** If the job is already terminal when `/wait` is called, it returns immediately without subscribing.

**Measured results:**

| Scenario               | Old (polling loop) | New (/wait) |
| ---------------------- | ------------------ | ----------- |
| Ingestion (374 ms job) | up to 5 000 ms     | ~374 ms     |
| Schema HIT (10 ms job) | ~1 000 ms          | ~10 ms      |
| Query (670 ms job)     | up to 1 000 ms     | ~670 ms     |

---

### Optimization #5 — Inline Results for Small Queries

**Problem:** Every query wrote a Parquet file to MinIO, even for tiny result sets (5 rows, schema with 6 columns). This involved Spark serialization, an HTTP PUT to MinIO, and a second client request to fetch the file.

**Fix:** If `rowCount ≤ 1 000`, serialize the result as JSON and embed it directly in the Redis status hash (`resultData` field). Skip the MinIO write entirely.

```java
// QueryResultWriter.java
if (rowCount <= INLINE_THRESHOLD) {          // INLINE_THRESHOLD = 1 000
    return QueryResult.inline(json, rowCount); // resultPath = null, fileSizeBytes = 0
}
return QueryResult.external(minioPath, rowCount);
```

**Measured results:**

| Query                | Time   | Result delivery |
| -------------------- | ------ | --------------- |
| SELECT \* LIMIT 100  | 2.8 s  | inline JSON     |
| WHERE + ORDER BY     | 670 ms | inline JSON     |
| GROUP BY aggregation | 850 ms | inline JSON     |

All test queries return inline — no MinIO round-trip needed.

---

### Combined Results Summary

| Test                              | Worker time | Notes                                |
| --------------------------------- | ----------- | ------------------------------------ |
| T1 Initial ingestion (1 000 rows) | 3 806 ms    | warm session — no cold-start penalty |
| T2 Append ingestion (500 rows)    | 374 ms      | subsequent job on warm session       |
| T3 Schema MISS (→ Spark)          | 185 ms      | cache empty, fetches from Iceberg    |
| T4 Schema HIT (→ Redis)           | 10 ms       | served from Redis (~18× faster)      |
| T5 Ingestion invalidates cache    | 579 ms      | DEL called by ingestion-worker       |
| T6 Schema MISS (post-ingest)      | 31 ms       | re-fetches updated 6-column schema   |
| T7 Schema HIT (re-cached)         | 11 ms       | served from Redis again              |
| T8 SELECT \* LIMIT 100            | 2 819 ms    | warm Spark + inline result           |
| T9 WHERE + ORDER BY               | 670 ms      | warm Spark + inline result           |
| T10 GROUP BY aggregation          | 850 ms      | warm Spark + inline result           |

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
minio + redis + rabbitmq + catalog-postgres
               ↓
          minio-setup  (creates buckets)
               ↓
       iceberg-rest-catalog
               ↓
    api-service + ingestion-worker + query-worker
```

### Force rebuild after source changes

Docker caches the `COPY src` layer by content hash. If your changes are not being picked up:

```bash
# Touch the changed file to bust the layer cache, then rebuild
touch ingestion-worker/src/.../ChangedFile.java
docker compose up -d --build ingestion-worker
```
