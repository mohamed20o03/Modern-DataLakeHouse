# Query Flow — End to End

This document traces a single query request from the HTTP endpoint through
RabbitMQ, the Query Worker, Apache Spark, and back to the client.

---

## Bird's-eye view

```
Client
  │
  │  POST /api/v1/query
  ▼
QueryController          (api-service)
  │
  ▼
QueryService             (api-service)
  ├─ write PENDING → Redis
  ├─ publish QueryMessage → RabbitMQ
  └─ write QUEUED  → Redis
       │
       │  returns immediately — jobId + streamUrl
       ▼
     Client  ←── {"jobId":"query-xxx", "streamUrl":"/api/v1/query/xxx/stream"}

                    (async — happens in the query-worker process)
                         │
                         ▼
                   RabbitMQ queue
                         │
                         ▼
                   SparkService.query()      (query-worker)
                         │
                         ├─ write PROCESSING → Redis
                         │
                         ▼
                   SparkQueryBuilder.build()
                         │  builds a Spark DataFrame (no data read yet)
                         ▼
                   Spark executes the query   ← actual data scan here
                         │
                         ▼
                   QueryResultWriter.write()
                         │
                         ├─ ≤ 1 000 rows → embed JSON in Redis   (inline)
                         └─ > 1 000 rows → XADD batches to Redis Streams (streamed)
                                │
                                ▼
                         write COMPLETED → Redis
                         publish job-done → Redis Pub/Sub
```

---

## Phase 1 — Client submits the query

**File:** `api-service/.../query/controller/QueryController.java`  
**File:** `api-service/.../query/service/QueryService.java`

```
POST /api/v1/query
Content-Type: application/json

{
  "source": "my_project.sales",
  "select": [{"column": "*"}],
  "filters": [{"column": "price", "operator": ">", "value": 500}],
  "orderBy": [{"column": "price", "direction": "desc"}],
  "limit": 50
}
```

`QueryController` receives the request and calls `QueryService.submit()`.

Inside `QueryService.submit()`:

```
1.  jobId = "query-" + random UUID        → "query-abc123"

2.  Redis HSET job:query-abc123  status=PENDING

3.  Serialize request to queryJson string

4.  Build QueryMessage { jobId, source, queryJson }

5.  RabbitMQ publish → query.queue   (priority = 1)

6.  Redis HSET job:query-abc123  status=QUEUED

7.  Return QueryResponse immediately:
    {
      "jobId":       "query-abc123",
      "status":      "queued",
      "checkStatusAt": "/api/v1/query/query-abc123",
      "streamUrl":     "/api/v1/query/query-abc123/stream"
    }
```

The HTTP response goes back to the client **right away** — the query has not
run yet.

---

## Phase 2 — RabbitMQ delivers the message to the worker

**File:** `query-worker/.../consumer/RabbitMQMessageConsumer.java`

The query-worker is a long-running Java process (not Spring Boot — plain Java
`main`). At startup it connects to RabbitMQ and waits for messages on
`query.queue`.

When the message arrives:

```
RabbitMQ → onMessage(body)
               │
               ▼
           deserialize → QueryMessage { jobId, source, queryJson }
               │
               ▼
           SparkService.query(message)
```

---

## Phase 3 — SparkService orchestrates the job

**File:** `query-worker/.../engine/spark/SparkService.java`

```
SparkService.query(message)
  │
  ├─ 1.  Redis HSET job:query-abc123  status=PROCESSING
  │
  ├─ 2.  Resolve table name
  │         "my_project.sales"  →  "iceberg.my_project.sales"
  │
  ├─ 3.  Deserialize queryJson  →  QueryRequest object
  │
  ├─ 4.  SparkQueryBuilder.build("iceberg.my_project.sales", request)
  │         returns a Dataset<Row>  ← Spark plan built, NOT yet executed
  │
  ├─ 5.  QueryResultWriter.write(dataset, "query-abc123")
  │         Spark executes here ← data scan happens at this point
  │
  └─ 6.  Redis HSET job:query-abc123  status=COMPLETED  (+ result fields)
         Redis PUBLISH job-done:query-abc123  "COMPLETED"
```

---

## Phase 4 — SparkQueryBuilder builds the DataFrame

**File:** `query-worker/.../engine/spark/SparkQueryBuilder.java`

This class translates the structured `QueryRequest` JSON into a Spark
DataFrame pipeline. **No data is read from Iceberg at this point** — Spark
only builds a logical plan.

```
df = spark.read().format("iceberg").load("iceberg.my_project.sales")
          │
          ├─ 1. WHERE   applyFilters(df, filters)
          │               df.filter(col("price").gt(500))
          │
          ├─ 2. SELECT  applySelectAndGroupBy(df, request)
          │               df.select(col("*"))
          │               or df.agg(avg("price"), count("id")) if GROUP BY
          │
          ├─ 3. ORDER BY applyOrderBy(df, orderBy)
          │               df.orderBy(col("price").desc())
          │
          └─ 4. LIMIT   df.limit(50)
```

The method returns the `Dataset<Row>` object — a recipe for the query,
not the result yet.

---

## Phase 5 — Spark executes the query

**File:** `query-worker/.../engine/spark/QueryResultWriter.java`

`QueryResultWriter.write(dataset, jobId)` is where Spark actually runs:

```java
Iterator<Row> iterator = result.toLocalIterator();
```

This single call triggers the full Spark execution:

- Spark reads Parquet files from MinIO (the Iceberg table's data)
- Applies the WHERE filter, SELECT columns, ORDER BY, LIMIT across executors
- Returns results partition-by-partition to the driver as a Java `Iterator`

> `toLocalIterator()` is used instead of `collectAsList()` so that for very
> large results only one partition is held in driver memory at a time.
> **Spark still computes the full result before the first row arrives** —
> the difference is only in how the driver receives it.

---

## Phase 6 — QueryResultWriter decides: inline or stream

**File:** `query-worker/.../engine/spark/QueryResultWriter.java`

The writer buffers up to 1 001 rows to decide which path to take:

```
iterator → fill buffer (up to 1 001 rows)
                │
      ┌─────────┴─────────┐
  ≤ 1 000 rows          > 1 000 rows
      │                      │
  INLINE path           STREAM path
```

### Inline path (≤ 1 000 rows)

All rows are already in the buffer. They are serialized to a JSON string and
stored directly in the Redis job hash:

```
Redis HSET job:query-abc123
    status      = "COMPLETED"
    rowCount    = "50"
    resultData  = "[{\"price\":999,...},{\"price\":998,...}]"
    streamed    = "false"
```

The client polls `GET /api/v1/query/query-abc123` and gets the rows back
inside the status hash — no second HTTP request needed.

### Stream path (> 1 000 rows)

The buffer overflowed, meaning there are more than 1 000 rows. The writer
calls `RedisResultStreamPublisher` to write batches into a Redis Stream:

```
Step 1 — publish column names once:
    XADD query-result-stream:query-abc123 * type metadata columns ["id","price",...]

Step 2 — drain the 1 001-row buffer + continue the iterator, 500 rows at a time:
    XADD query-result-stream:query-abc123 * type batch batchIndex 0  rows [{...×500}]
    XADD query-result-stream:query-abc123 * type batch batchIndex 1  rows [{...×500}]
    XADD query-result-stream:query-abc123 * type batch batchIndex 2  rows [{...×500}]
    XADD query-result-stream:query-abc123 * type batch batchIndex 3  rows [{...×100}]

Step 3 — publish completion marker:
    XADD query-result-stream:query-abc123 * type complete totalBatches 4 totalRows 1600
```

Then the Redis job hash is updated:

```
Redis HSET job:query-abc123
    status      = "COMPLETED"
    rowCount    = "1600"
    resultData  = null          ← no inline data
    streamed    = "true"
```

The stream key is given a **1-hour TTL** after the completion marker —
matching the job hash TTL.

---

## Phase 7 — Client consumes the result

The client has two options depending on whether it got inline or streamed data.

### Option A — Inline (small result)

```
GET /api/v1/query/query-abc123

Response:
{
  "status":   "COMPLETED",
  "rowCount": "50",
  "resultData": "[{\"price\":999,...},...]"
}
```

All rows are in the response body directly.

### Option B — Stream via SSE (large result)

**File:** `api-service/.../query/streaming/QueryStreamService.java`  
**File:** `api-service/.../query/streaming/QueryStreamController.java`

```
GET /api/v1/query/query-abc123/stream
Accept: text/event-stream
```

`QueryStreamService.createStream()` starts a **background thread** that runs
a blocking `XREAD` loop on the Redis Stream:

```
lastId = "0"   ← start from the beginning of the stream

loop:
    records = XREAD COUNT 100 STREAMS query-result-stream:query-abc123 {lastId}

    for each record:
        type = record["type"]

        "metadata"  →  SSE event:metadata \n data:["id","price",...]\n\n
        "batch"     →  SSE event:batch    \n data:[{...},{...},...]\n\n
        "complete"  →  SSE event:complete \n data:{"totalBatches":4,"totalRows":1600}\n\n
                       → emitter.complete()  ← closes the SSE connection
```

The client receives events progressively as they are pushed:

```
event:metadata
data:["id","product","price","quantity","date","region"]

event:batch
data:[{"id":1,"product":"Laptop","price":999.0,...},{"id":2,...}, ...]

event:batch
data:[{"id":501,...},{"id":502,...}, ...]

event:batch
data:[{"id":1001,...}, ...]

event:batch
data:[{"id":1501,...}, ...]

event:complete
data:{"totalBatches":4,"totalRows":1600}
```

---

## Phase 8 — Client is notified without polling

While waiting for the job to finish, the client can use the long-poll endpoint
instead of polling every second:

```
GET /api/v1/query/query-abc123/wait?timeoutSec=60
```

**File:** `api-service/.../query/controller/QueryController.java`

Internally, `JobStatusService` subscribes to the Redis Pub/Sub channel
`job-done:query-abc123`. When the worker publishes to that channel after
writing `COMPLETED`, the long-poll HTTP connection is resolved instantly —
no polling loop, no wasted seconds waiting.

---

## Summary — which file does what

| Step | File                         | What it does                                                              |
| ---- | ---------------------------- | ------------------------------------------------------------------------- |
| 1    | `QueryController`            | Receives HTTP POST, validates request                                     |
| 2    | `QueryService`               | Generates jobId, writes Redis status, publishes to RabbitMQ               |
| 3    | `RabbitMQMessageConsumer`    | Pulls message from queue, calls SparkService                              |
| 4    | `SparkService`               | Orchestrates: PROCESSING → build → execute → COMPLETED                    |
| 5    | `SparkQueryBuilder`          | Translates `QueryRequest` → Spark `Dataset<Row>` (plan only)              |
| 6    | `QueryResultWriter`          | Calls `toLocalIterator()` (Spark executes here), decides inline vs stream |
| 7    | `ResultStreamPublisher`      | Interface: `publishMetadata` / `publishBatch` / `publishComplete`         |
| 8    | `RedisResultStreamPublisher` | Calls `XADD` to write each batch into a Redis Stream                      |
| 9    | `QueryStreamController`      | `GET /{jobId}/stream` — returns `SseEmitter`                              |
| 10   | `QueryStreamService`         | Background thread: `XREAD` loop → pushes SSE events to client             |

---

## Key design decisions

### Why RabbitMQ in the middle?

The API service returns immediately to the client without waiting for Spark.
RabbitMQ decouples the HTTP thread from the Spark execution time (which can be
seconds). Multiple queries queue up and are processed one at a time by the
worker.

### Why Redis Streams for large results (not Pub/Sub)?

Redis Pub/Sub is fire-and-forget — if the API service opens the stream
**after** the worker already published the batches, it misses them.
Redis Streams **persist entries** — the API service always reads from
offset `0` and receives every batch regardless of when it connects.

### Why `toLocalIterator()` instead of `collect()`?

`collect()` loads the **entire result set** into driver memory at once. For
50 million rows that would crash the JVM. `toLocalIterator()` streams one
Spark partition (e.g., 128 MB) at a time. The trade-off: Spark still
finishes computing the full result before the first row arrives — but driver
memory usage stays bounded.

### Why inline for ≤ 1 000 rows?

Setting up a Redis Stream and an SSE connection adds latency. For small
results it is faster and simpler to store the JSON directly in the job hash
and let the client read it in a single `GET /status` call.
