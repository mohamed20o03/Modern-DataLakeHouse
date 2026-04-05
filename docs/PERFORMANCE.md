# Performance Bottlenecks & Optimizations

> **Status:** All 5 optimizations described in this document have been **implemented**. Additionally, a **real-time CDC pipeline** has been added with its own performance characteristics (see [CDC Pipeline Performance](#cdc-pipeline-performance)).

## System Architecture

```
Client
  │
  ▼
API Service (Spring Boot)
  │  ① upload file → MinIO
  │  ② publish message → RabbitMQ
  │  ③ write QUEUED → Redis
  │
  ▼
RabbitMQ
  │  query.queue   → Query Worker (Spark)
  │  ingestion.queue → Ingestion Worker (Spark)
  ▼
Spark Worker
  │  ④ read file from MinIO / Iceberg
  │  ⑤ execute job
  │  ⑥ write result → Redis + MinIO (Parquet)
  ▼
Redis (job status + result data)

─── CDC Pipeline (parallel, independent) ───

Source PostgreSQL (wal_level=logical)
  │  WAL stream
  ▼
Debezium Connect → Kafka (KRaft)
  │  CDC events (cdc.public.customers)
  ▼
CDC Worker (Spark Structured Streaming)
  │  MERGE INTO Iceberg table
  ▼
Iceberg (s3://warehouse/)
```

---

## Measured Timings (Before Optimizations)

These were the baseline timings that motivated the optimizations:

| Operation                     | HTTP Round-trip | Worker Processing | Total   |
| ----------------------------- | --------------- | ----------------- | ------- |
| CSV generation (1500 rows)    | —               | 49 ms             | 49 ms   |
| Initial ingestion (1000 rows) | 549 ms          | 5 031 ms          | ~5.6 s  |
| Append (500 rows)             | 88 ms           | 5 030 ms          | ~5.1 s  |
| Schema retrieval              | 78 ms           | 5 041 ms          | ~5.1 s  |
| SELECT \* LIMIT 100           | 193 ms          | 10 051 ms         | ~10.2 s |
| WHERE + ORDER BY              | 49 ms           | 5 039 ms          | ~5.1 s  |
| GROUP BY aggregation          | 46 ms           | 5 039 ms          | ~5.1 s  |

---

## Bottleneck #1 — Spark Session Cold Start ✅

### Problem

Every job pays a **~5 second JVM + Spark initialization penalty**.
The `SparkSession` is already created once when the worker starts (via `SparkEngine`),
but the first job on each executor thread triggers internal lazy initialization:
loading Iceberg extensions, S3A filesystem, and the catalog connection.

### Evidence

- Schema retrieval (read 5 column names) takes **5 041 ms** — the same as a full query.
- A simple `SELECT * LIMIT 100` takes **10 051 ms** (two round-trips of internal Spark dispatch).

### Fix: Warm Up the Spark Session on Startup

A no-op query (`SELECT 1`) runs immediately after `SparkSession` initialization, forcing all lazy components (Iceberg extensions, S3A filesystem, catalog connection) to load before the first real job arrives.

**Implementation:**

- `ingestion-worker/.../engine/spark/SparkEngine.java` — `warmUp()` called in constructor
- `query-worker/.../engine/spark/SparkEngine.java` — same

**Result:** First job dropped from **~10 s → ~1–2 s**.

---

## Bottleneck #2 — Schema Fetched via Spark Every Time ✅

### Problem

Schema retrieval triggers a full Spark + Iceberg catalog round-trip (reads table metadata
from PostgreSQL catalog + MinIO) **every single time**, even though the schema only changes
when new data is ingested (rare).

### Evidence

Schema retrieval: **5 041 ms** — identical cost to running a data query.

### Fix: Cache Schema in Redis After First Retrieval

```
First request:
  query-worker → Iceberg catalog → schema JSON → Redis cache (TTL: forever)
                                               → return to client

Subsequent requests:
  query-worker → Redis cache HIT → return to client  (~5 ms)

On new ingestion:
  ingestion-worker → Redis.delete("schema:{source}") → cache invalidated
```

The cache key pattern is `schema:{icebergTable}`. On ingestion the cache for that table is invalidated so the next schema request re-reads from Iceberg.

**Implementation:**

- `query-worker/.../cache/RedisSchemaCacheService.java`
- `ingestion-worker/.../cache/RedisSchemaCacheService.java` (invalidation on ingest)

**Result:** Schema retrieval dropped from **~5 s → ~5 ms** (after first call).

---

## Bottleneck #3 — Schema Jobs Blocked by Heavy Queries ✅

### Problem

Schema and query messages share the same `query.queue`. A heavy aggregation query
(GROUP BY over 1500 rows, writing Parquet to MinIO) takes **5+ seconds** and blocks
the channel. A schema request arriving during that window must wait.

With `WORKER_CONCURRENCY=2`, two slow queries can fully saturate all consumer threads.

### Fix: RabbitMQ Priority Queue

The `query.queue` is declared with `x-max-priority: 10`. Schema messages are published with priority 8, query messages with priority 1. RabbitMQ delivers higher-priority messages first, so schema requests jump ahead of any queued queries.

**Implementation:**

- `api-service/.../config/RabbitMqConfig.java` — queue declared with `x-max-priority: 10`
- `api-service/.../common/messaging/MessageRepository.java` — priority-aware `publish()` method
- `query-worker/.../consumer/rabbitmq/RabbitMQConsumer.java` — consumer-side matching priority args

**Result:** Schema jobs are never blocked by query backlog. Latency is **predictable** regardless of query load.

---

## Bottleneck #4 — Client Polls Redis Every 5 Seconds ✅

### Problem

The test script (and any real client) polls `GET /api/v1/query/{jobId}` every 5 seconds.
This means:

- Best case: job finishes in 1s but client waits up to 5s to find out → **4s wasted**.
- Adds unnecessary load on Redis and the API service.

### Fix: Redis Pub/Sub for Job Completion Notification

Instead of polling, the worker publishes to a Redis Pub/Sub channel (`job-done:{jobId}`) when a job completes. The API service subscribes to that channel and resolves the pending HTTP response immediately.

```
Worker finishes job
  │
  ▼
Redis PUBLISH "job-done:{jobId}" "{status: COMPLETED, ...}"
  │
  ▼
API Service (subscribed) receives event
  │
  ▼
Pending HTTP response is resolved immediately
```

The API service exposes `/wait` endpoints on all three job types (ingestion, query, schema) using Spring's `DeferredResult` for non-blocking HTTP long-poll.

**Implementation:**

- `ingestion-worker/.../status/redis/AsyncRedisJobStatusService.java` — publishes on `job-done:{jobId}`
- `query-worker/.../status/redis/AsyncRedisJobStatusService.java` — same
- `api-service/.../config/RedisConfig.java` — `RedisMessageListenerContainer` for Pub/Sub subscriptions
- `api-service/.../module/query/controller/QueryController.java` — `DeferredResult`-based `/wait`
- `api-service/.../module/ingestion/controller/IngestionController.java` — same
- `api-service/.../module/schema/controller/SchemaController.java` — same

**Result:** Client receives result the same millisecond the job finishes. Eliminated ~40% of total perceived latency for fast jobs.

---

## Bottleneck #5 — Parquet Result Written to MinIO for Every Query ✅

### Problem

Every query writes a Parquet file to MinIO even for tiny result sets (5 rows from
GROUP BY, schema with 5 columns). This involves:

1. Spark DataFrame → Parquet serialization
2. HTTP PUT to MinIO (network + disk I/O)
3. Redis stores the MinIO path (client must make a second request to fetch it)

For small results this is pure overhead.

### Fix: Inline Results for Small Result Sets

If `rowCount <= 1000`, the result JSON is embedded directly in the Redis status hash. For larger results, rows are published in 500-row batches to a Redis Stream, which the API service reads and pushes to the client via SSE.

**Implementation:**

- `query-worker/.../engine/spark/QueryResultWriter.java` — `INLINE_THRESHOLD = 1_000`, inline vs stream decision
- `query-worker/.../streaming/redis/RedisResultStreamPublisher.java` — publishes large results to Redis Streams
- `api-service/.../module/query/streaming/QueryStreamService.java` — reads Redis Streams and pushes via SSE, with inline fallback

**Result:** Small queries (schema, aggregations, filtered results) skip MinIO entirely → **~500 ms saved** per small query. Large results stream progressively instead of requiring a second MinIO download.

---

## Performance After All Fixes

| Operation               | Before     | After  | Improvement |
| ----------------------- | ---------- | ------ | ----------- |
| Schema (first call)     | ~5 s       | ~1 s   | 5×          |
| Schema (cached)         | ~5 s       | ~5 ms  | 1000×       |
| Simple query (warm)     | ~10 s      | ~1–2 s | 5–10×       |
| Filtered query          | ~5 s       | ~1 s   | 5×          |
| Aggregation (small)     | ~5 s       | ~0.5 s | 10×         |
| Client polling overhead | ~2.5 s avg | ~0 s   | ∞           |

---

## Implementation Status

| Fix                              | Status  | Impact |
| -------------------------------- | ------- | ------ |
| Spark warm-up on startup         | ✅ Done | High   |
| Schema Redis cache               | ✅ Done | High   |
| Inline results for small queries | ✅ Done | Medium |
| RabbitMQ priority queue          | ✅ Done | Medium |
| Redis Pub/Sub (replace polling)  | ✅ Done | Medium |

---

## CDC Pipeline Performance

The real-time CDC pipeline introduces a parallel data path (PostgreSQL → Debezium → Kafka → Spark Structured Streaming → Iceberg).

### CDC Latency

| Metric | Value |
|--------|-------|
| Trigger interval | 30 seconds (configurable) |
| Batch processing time (10 events) | ~3 seconds |
| End-to-end latency (source → Iceberg) | < 35 seconds |
| Operations supported | INSERT, UPDATE, DELETE |

### Small File Problem & Compaction

Each micro-batch creates a new Parquet file. Over time this degrades read performance.

| Metric | Before compaction | After compaction |
|--------|-------------------|------------------|
| Data files | 7 | 1 |
| File reduction | — | 85% |
| Row count | 15 | 15 (unchanged) |
| Read overhead | ~3s (7 file opens) | ~0.5s (1 file open) |

**Compaction command:**
```bash
docker compose run --rm cdc-worker --compact iceberg.cdc_namespace.customers
```

### Snapshot Management

| Operation | Command | Notes |
|-----------|---------|-------|
| Tag snapshot | `--tag <table> <tagName>` | Tagged snapshots survive expiration |
| Expire old snapshots | `--expire-snapshots <table> [retainDays]` | Only untagged snapshots are removed |

### Implementation

| Component | File | Purpose |
|-----------|------|---------|
| CDC Engine | `SparkCdcEngine.java` | Spark session + CLI routing |
| Stream Processor | `CdcStreamProcessor.java` | Kafka → foreachBatch pipeline |
| Event Parser | `DebeziumEventParser.java` | Schema-less JSON parsing |
| Deduplicator | `EventDeduplicator.java` | Window function (latest per PK) |
| Merge Writer | `IcebergMergeWriter.java` | MERGE INTO with upsert/delete |
| Compaction | `CompactionService.java` | Bin-pack via RewriteDataFiles |
| Snapshots | `SnapshotManager.java` | Tag + expire via Spark SQL |
