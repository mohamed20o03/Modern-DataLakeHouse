# Performance Bottlenecks & Optimizations

## Current System Architecture

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
```

---

## Measured Timings (Current)

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

## Bottleneck #1 — Spark Session Cold Start

### Problem

Every job pays a **~5 second JVM + Spark initialization penalty**.
The `SparkSession` is already created once when the worker starts (via `SparkEngine`),
but the first job on each executor thread triggers internal lazy initialization:
loading Iceberg extensions, S3A filesystem, and the catalog connection.

### Evidence

- Schema retrieval (read 5 column names) takes **5 041 ms** — the same as a full query.
- A simple `SELECT * LIMIT 100` takes **10 051 ms** (two round-trips of internal Spark dispatch).

### Fix: Warm Up the Spark Session on Startup

Run a cheap no-op query immediately after `SparkSession` initialization so all lazy
components are loaded before the first real job arrives.

```java
// SparkEngine.java — after SparkSession is built
private void warmUp(SparkSession spark) {
    log.info("Warming up Spark session...");
    long start = System.currentTimeMillis();
    spark.sql("SELECT 1").count(); // forces catalog + executor init
    log.info("Spark warm-up completed in {} ms", System.currentTimeMillis() - start);
}
```

**Expected gain:** First job drops from **~10 s → ~1–2 s**.

---

## Bottleneck #2 — Schema Fetched via Spark Every Time

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

**Implementation in `SparkService.java`:**

```java
// Cache key pattern: "schema:projectId.tableName"
String cacheKey = "schema:" + source;

String cached = redis.get(cacheKey);
if (cached != null) {
    log.info("Schema cache HIT: source={}", source);
    jobStatusService.writeResult(jobId, "COMPLETED", cached, 0, 0);
    return;
}

// Cache miss — fetch from Iceberg
QueryResult result = executeSchemaJob(icebergTable);
redis.set(cacheKey, result.getResultJson()); // no TTL — valid until next ingestion
```

**Invalidation in `SparkService.java` (ingestion path):**

```java
// After successful ingestion, invalidate the schema cache for that table
redis.delete("schema:" + source);
log.info("Schema cache invalidated for source={}", source);
```

**Expected gain:** Schema retrieval drops from **~5 s → ~5 ms** (after first call).

---

## Bottleneck #3 — Schema Jobs Blocked by Heavy Queries

### Problem

Schema and query messages share the same `query.queue`. A heavy aggregation query
(GROUP BY over 1500 rows, writing Parquet to MinIO) takes **5+ seconds** and blocks
the channel. A schema request arriving during that window must wait.

With `WORKER_CONCURRENCY=2`, two slow queries can fully saturate all consumer threads.

### Fix: RabbitMQ Priority Queue

Assign higher priority to schema messages so they are delivered before pending query
messages, even when they arrive later.

**`RabbitMqConfig.java`:**

```java
@Bean
public Queue queryQueue() {
    return QueueBuilder.durable(QUERY_QUEUE)
            .withArgument("x-max-priority", 10) // 0 = lowest, 10 = highest
            .build();
}
```

**`MessagePublisherService.java`:**

```java
// Schema → priority 8 (high)
public void publishSchemaMessage(SchemaMessage message) {
    MessagePostProcessor priority = m -> {
        m.getMessageProperties().setPriority(8);
        return m;
    };
    repository.publishWithOptions(QUERY_EXCHANGE, QUERY_ROUTING_KEY, message, priority);
}

// Query → priority 1 (normal)
public void publishQueryMessage(QueryMessage message) {
    MessagePostProcessor priority = m -> {
        m.getMessageProperties().setPriority(1);
        return m;
    };
    repository.publishWithOptions(QUERY_EXCHANGE, QUERY_ROUTING_KEY, message, priority);
}
```

**Expected gain:** Schema jobs are never blocked by query backlog. Latency becomes
**predictable** regardless of query load.

---

## Bottleneck #4 — Client Polls Redis Every 5 Seconds

### Problem

The test script (and any real client) polls `GET /api/v1/query/{jobId}` every 5 seconds.
This means:

- Best case: job finishes in 1s but client waits up to 5s to find out → **4s wasted**.
- Adds unnecessary load on Redis and the API service.

### Fix: Redis Pub/Sub for Job Completion Notification

Instead of polling, the worker publishes to a Redis channel when a job completes,
and the API service subscribes and delivers the result immediately.

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

**Worker side (`AsyncRedisJobStatusService.java`):**

```java
// After writing status to Redis hash, publish completion event
if ("COMPLETED".equals(status) || "FAILED".equals(status)) {
    redis.async().publish("job-done:" + jobId, statusJson);
}
```

**API side — Long-poll endpoint:**

```java
@GetMapping("/query/{jobId}/wait")
public DeferredResult<ResponseEntity<Map<Object,Object>>> waitForJob(
        @PathVariable String jobId) {

    DeferredResult<ResponseEntity<Map<Object,Object>>> result =
            new DeferredResult<>(30_000L); // 30s timeout

    redisSubscriber.subscribe("job-done:" + jobId, payload -> {
        result.setResult(ResponseEntity.ok(parse(payload)));
    });

    return result;
}
```

**Expected gain:** Client receives result as soon as job finishes instead of up to
5 seconds later. Eliminates ~40% of total perceived latency for fast jobs.

---

## Bottleneck #5 — Parquet Result Written to MinIO for Every Query

### Problem

Every query writes a Parquet file to MinIO even for tiny result sets (5 rows from
GROUP BY, schema with 5 columns). This involves:

1. Spark DataFrame → Parquet serialization
2. HTTP PUT to MinIO (network + disk I/O)
3. Redis stores the MinIO path (client must make a second request to fetch it)

For small results this is pure overhead.

### Fix: Inline Results for Small Result Sets

If `rowCount <= threshold` (e.g. 1000 rows), embed the result JSON directly in the
Redis status entry and skip the MinIO write entirely.

```java
// QueryResultWriter.java
private static final int INLINE_THRESHOLD = 1000;

public QueryResult write(Dataset<Row> df, String jobId, String projectId) {
    long rowCount = df.count();

    if (rowCount <= INLINE_THRESHOLD) {
        // Inline: serialize to JSON, store in Redis directly
        List<Row> rows = df.collectAsList();
        String json = toJson(rows, df.schema());
        return QueryResult.inline(jobId, json, rowCount);
    }

    // Large result: write Parquet to MinIO as before
    String path = writeParquet(df, jobId, projectId);
    return QueryResult.external(jobId, path, rowCount);
}
```

**Expected gain:** Small queries (schema, aggregations, filtered results) skip MinIO
entirely → **~500ms saved** per small query.

---

## Expected Performance After All Fixes

| Operation               | Current    | After Fixes | Improvement |
| ----------------------- | ---------- | ----------- | ----------- |
| Schema (first call)     | ~5 s       | ~1 s        | 5×          |
| Schema (cached)         | ~5 s       | ~5 ms       | 1000×       |
| Simple query (warm)     | ~10 s      | ~1–2 s      | 5–10×       |
| Filtered query          | ~5 s       | ~1 s        | 5×          |
| Aggregation (small)     | ~5 s       | ~0.5 s      | 10×         |
| Client polling overhead | ~2.5 s avg | ~0 s        | ∞           |

---

## Implementation Priority

| Priority | Fix                              | Effort | Impact |
| -------- | -------------------------------- | ------ | ------ |
| 1 🔴     | Spark warm-up on startup         | Low    | High   |
| 2 🔴     | Schema Redis cache               | Medium | High   |
| 3 🟡     | Inline results for small queries | Medium | Medium |
| 4 🟡     | RabbitMQ priority queue          | Low    | Medium |
| 5 🟢     | Redis pub/sub (replace polling)  | High   | Medium |
