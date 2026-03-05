# API Reference — Frontend Integration Guide

> **Audience:** Frontend / client-side developers.  
> This document covers what you need to call the API and handle responses correctly. Worker internals, performance tuning, and architecture details live in the other docs.

**Base URL:** `http://localhost:8080`  
**Content-Type for JSON requests:** `application/json`  
**No authentication is required** in this version.

---

## Table of Contents

1. [Shared Concepts](#1-shared-concepts)
   - [Job Lifecycle](#job-lifecycle)
   - [Status Response Shape](#status-response-shape)
   - [Error Responses](#error-responses)
2. [Ingestion API](#2-ingestion-api)
   - [Upload a File](#post-apiv1ingestionupload)
   - [Check Ingestion Status](#get-apiv1ingestionstatusjobid)
   - [Wait for Ingestion](#get-apiv1ingestionstatusjobidwaittimeoutsec30)
3. [Query API](#3-query-api)
   - [Submit a Query](#post-apiv1query)
   - [Check Query Status](#get-apiv1queryjobid)
   - [Wait for Query](#get-apiv1queryjobidwaittimeoutsec30)
   - [Stream Query Results (SSE)](#get-apiv1queryjobidstream)
4. [Query DSL Reference](#4-query-dsl-reference)
   - [SelectColumn](#selectcolumn)
   - [FilterCondition](#filtercondition)
   - [OrderByClause](#orderbyclause)
   - [Full Query Examples](#full-query-examples)
5. [Schema API](#5-schema-api)
   - [Get Schema](#get-apiv1schemasource)
   - [Check Schema Status](#get-apiv1schemastatusjobid)
   - [Wait for Schema](#get-apiv1schemastatusjobidwaittimeoutsec30)
6. [How `/wait` Works](#6-how-wait-works)
7. [How the SSE Stream Works](#7-how-the-sse-stream-works)
   - [Event Sequence](#event-sequence)
   - [Event Payloads](#event-payloads)
   - [Small Results (Inline Fallback)](#small-results-inline-fallback)
8. [Integration Patterns — Retrieving Query Results](#8-integration-patterns--retrieving-query-results)
   - [Pattern 1 — Use `/wait`, then check the `streamed` flag](#pattern-1--use-wait-then-check-the-streamed-flag)
   - [Pattern 2 — Always use the SSE stream (simplest)](#pattern-2--always-use-the-sse-stream-simplest)
   - [Pattern 3 — Poll (discouraged)](#pattern-3--poll-discouraged)
   - [Which pattern should I use?](#which-pattern-should-i-use)
9. [Endpoint Summary](#endpoint-summary)

---

## 1. Shared Concepts

### Job Lifecycle

Every API operation (ingestion, query, schema) is **asynchronous**. You send a request, get a `jobId` back immediately, and then track that job until it finishes.

Every job progresses through these states:

```
PENDING → QUEUED → PROCESSING → COMPLETED
                              ↘ FAILED
```

| State        | Meaning                                                 |
| ------------ | ------------------------------------------------------- |
| `PENDING`    | Job created, being written to the queue                 |
| `QUEUED`     | Message published to RabbitMQ, waiting for a worker     |
| `PROCESSING` | A worker picked it up, Spark is executing               |
| `COMPLETED`  | Done — results available in the status hash or via SSE  |
| `FAILED`     | Worker encountered an error — check the `message` field |

`COMPLETED` and `FAILED` are **terminal states**. Once a job reaches one of them it will not change again.

### Status Response Shape

All status and `/wait` responses share this base shape:

```json
{
  "status": "COMPLETED",
  "message": "Query completed successfully",
  "createdAt": "2026-03-05T10:00:00Z",
  "updatedAt": "2026-03-05T10:00:03Z"
}
```

Additional fields appear depending on the job type — each API section below documents the full response.

### Error Responses

| HTTP Code | When it happens                                  |
| --------- | ------------------------------------------------ |
| `404`     | `jobId` does not exist in Redis                  |
| `400`     | Request body is missing required fields          |
| `500`     | Unexpected server error (check API service logs) |

Error body example:

```json
{ "error": "Job not found", "jobId": "query-abc123" }
```

---

## 2. Ingestion API

Upload a file and ingest it into an Iceberg table.

---

### `POST /api/v1/ingestion/upload`

**Content-Type:** `multipart/form-data`

| Parameter   | Type   | Required | Description                                                |
| ----------- | ------ | :------: | ---------------------------------------------------------- |
| `file`      | file   |    ✅    | Data file. Supported: `.csv`, `.json`, `.parquet`, `.avro` |
| `userId`    | string |    ✅    | Your user identifier                                       |
| `projectId` | string |    ✅    | Project namespace — becomes the Iceberg namespace          |
| `tableName` | string |    ❌    | Target table name. Defaults to filename without extension  |

**Response** — `200 OK`:

```json
{
  "jobId": "9622371d-719c-4492-9e37-f6ba464ce63a",
  "status": "QUEUED",
  "checkStatusAt": "/api/v1/ingestion/status/9622371d-719c-4492-9e37-f6ba464ce63a"
}
```

Save the `jobId` — you will use it to track or wait for the ingestion.

---

### `GET /api/v1/ingestion/status/{jobId}`

Returns the current state of an ingestion job. One-shot check — returns immediately.

**While running:**

```json
{
  "status": "PROCESSING",
  "message": "Ingesting data...",
  "createdAt": "2026-03-05T10:00:00Z",
  "updatedAt": "2026-03-05T10:00:01Z"
}
```

**On success:**

```json
{
  "status": "COMPLETED",
  "message": "Ingestion completed successfully",
  "createdAt": "2026-03-05T10:00:00Z",
  "updatedAt": "2026-03-05T10:00:03Z"
}
```

**On failure:**

```json
{
  "status": "FAILED",
  "message": "Column 'revenue (USD)' contains unsupported characters"
}
```

---

### `GET /api/v1/ingestion/status/{jobId}/wait?timeoutSec=30`

**Long-poll** — holds the connection open until the job reaches `COMPLETED` or `FAILED`, or until the timeout expires.

| Parameter    | Type | Default | Max |
| ------------ | ---- | ------- | --- |
| `timeoutSec` | int  | `30`    | 300 |

Response shape is the same as the status endpoint above.

> **Timeout behaviour:** if the job has not finished within `timeoutSec` seconds, the server returns the current (non-terminal) status rather than an error. Always check `status` in the response.

---

## 3. Query API

Submit a structured query against an Iceberg table and retrieve results.

---

### `POST /api/v1/query`

**Content-Type:** `application/json`

**Request body:**

```json
{
  "source":  "projectId.tableName",
  "select":  [ ... ],
  "filters": [ ... ],
  "groupBy": [ ... ],
  "orderBy": [ ... ],
  "limit":   100
}
```

| Field      | Type     | Required | Description                                               |
| ---------- | -------- | :------: | --------------------------------------------------------- |
| `source`   | string   |    ✅    | `"projectId.tableName"` — identifies the Iceberg table    |
| `select`   | array    |    ✅    | Columns to return. See [SelectColumn](#selectcolumn)      |
| `filters`  | array    |    ❌    | WHERE conditions. See [FilterCondition](#filtercondition) |
| `groupBy`  | string[] |    ❌    | Column names to group by                                  |
| `orderBy`  | array    |    ❌    | Sort clauses. See [OrderByClause](#orderbyclause)         |
| `limit`    | int      |    ❌    | Max rows. Hard-capped at **10 000** by the engine         |
| `encoding` | object   |    ❌    | Optional visualization hints `{x, y, color}`              |

See [Section 4 — Query DSL Reference](#4-query-dsl-reference) for the full format of each field.

**Response** — `200 OK`:

```json
{
  "jobId": "query-550e8400-e29b-41d4-a716-446655440000",
  "status": "queued",
  "message": "Query job queued for processing",
  "checkStatusAt": "/api/v1/query/query-550e8400-e29b-41d4-a716-446655440000",
  "streamUrl": "/api/v1/query/query-550e8400-e29b-41d4-a716-446655440000/stream"
}
```

> Save both `jobId` and `streamUrl`. You will need them for the next steps.

---

### `GET /api/v1/query/{jobId}`

Returns the current state of a query job. One-shot check.

**While running:**

```json
{ "status": "PROCESSING", "message": "..." }
```

**Completed — small result (≤ 1 000 rows, inline):**

```json
{
  "status": "COMPLETED",
  "rowCount": "42",
  "streamed": "false",
  "resultData": "[{\"product\":\"Laptop\",\"avg_price\":999.0},{\"product\":\"Mouse\",\"avg_price\":29.0}]",
  "fileSizeBytes": "0",
  "createdAt": "2026-03-05T10:00:00Z",
  "updatedAt": "2026-03-05T10:00:00Z"
}
```

**Completed — large result (> 1 000 rows, streamed):**

```json
{
  "status": "COMPLETED",
  "rowCount": "1600",
  "streamed": "true",
  "resultData": null,
  "fileSizeBytes": "0",
  "createdAt": "2026-03-05T10:00:00Z",
  "updatedAt": "2026-03-05T10:00:00Z"
}
```

The key field is **`streamed`**:

| `streamed` value | What it means                                                            |
| ---------------- | ------------------------------------------------------------------------ |
| `"false"`        | All rows are in `resultData` as a JSON string — parse it and you're done |
| `"true"`         | `resultData` is `null` — connect to the SSE stream to read the rows      |

---

### `GET /api/v1/query/{jobId}/wait?timeoutSec=30`

Long-poll — blocks until the query job is terminal. Response shape is the same as `GET /api/v1/query/{jobId}` above (including `streamed` and `resultData`), but it only resolves once the job finishes.

| Parameter    | Type | Default | Max |
| ------------ | ---- | ------- | --- |
| `timeoutSec` | int  | `30`    | 300 |

> **Best practice:** use `/wait` instead of polling `GET /query/{jobId}` in a loop. It responds the same millisecond the worker finishes — zero wasted round-trips.

---

### `GET /api/v1/query/{jobId}/stream`

**SSE endpoint** — returns `text/event-stream`.

Pushes query result rows to the client progressively as Server-Sent Events. **Works for both small and large results.** The server waits internally for the job to complete before it starts emitting events, so you can open this connection at any point after submitting the query — even before the worker has started.

See [Section 7 — How the SSE Stream Works](#7-how-the-sse-stream-works) for the full event format and behaviour.

---

## 4. Query DSL Reference

### `SelectColumn`

One entry in the `select` array.

| Field         | Type   | Required | Description                                                       |
| ------------- | ------ | :------: | ----------------------------------------------------------------- |
| `column`      | string |    ✅    | Column name, or `"*"` for all columns                             |
| `aggregation` | string |    ❌    | One of: `sum`, `avg`, `count`, `min`, `max`, `stddev`, `variance` |
| `as`          | string |    ❌    | Output alias for the column                                       |

**Examples:**

```json
{"column": "*"}
{"column": "product_name", "as": "product"}
{"column": "price", "aggregation": "avg", "as": "avg_price"}
{"column": "id", "aggregation": "count", "as": "n"}
```

> When using aggregations, **all non-aggregated columns in `select` must also appear in `groupBy`**.

---

### `FilterCondition`

One entry in the `filters` array — maps to a SQL WHERE clause.

| Field      | Type                    | Required | Description             |
| ---------- | ----------------------- | :------: | ----------------------- |
| `column`   | string                  |    ✅    | Column name             |
| `operator` | string                  |    ✅    | See supported operators |
| `value`    | string / number / array |    ❌    | Omit for null-check ops |

**Supported operators:**

| Operator      | Value type       | SQL equivalent           |
| ------------- | ---------------- | ------------------------ |
| `=`           | scalar           | `col = value`            |
| `!=`          | scalar           | `col != value`           |
| `<`           | number           | `col < value`            |
| `<=`          | number           | `col <= value`           |
| `>`           | number           | `col > value`            |
| `>=`          | number           | `col >= value`           |
| `LIKE`        | string with `%`  | `col LIKE 'value%'`      |
| `IN`          | array of scalars | `col IN ('a', 'b', 'c')` |
| `IS NULL`     | omit `value`     | `col IS NULL`            |
| `IS NOT NULL` | omit `value`     | `col IS NOT NULL`        |

**Examples:**

```json
{"column": "price", "operator": ">", "value": 500}
{"column": "region", "operator": "IN", "value": ["EMEA", "APAC"]}
{"column": "name", "operator": "LIKE", "value": "Laptop%"}
{"column": "discount", "operator": "IS NOT NULL"}
```

---

### `OrderByClause`

One entry in the `orderBy` array.

| Field       | Type   | Required | Description                   |
| ----------- | ------ | :------: | ----------------------------- |
| `column`    | string |    ✅    | Column name to sort by        |
| `direction` | string |    ❌    | `"asc"` (default) or `"desc"` |

**Examples:**

```json
{"column": "price", "direction": "desc"}
{"column": "product_name", "direction": "asc"}
```

---

### Full Query Examples

**SELECT \* with a filter and ordering:**

```json
{
  "source": "my_project.sales",
  "select": [{ "column": "*" }],
  "filters": [{ "column": "price", "operator": ">", "value": 500 }],
  "orderBy": [{ "column": "price", "direction": "desc" }],
  "limit": 50
}
```

**GROUP BY with multiple aggregations:**

```json
{
  "source": "my_project.sales",
  "select": [
    { "column": "region" },
    { "column": "price", "aggregation": "avg", "as": "avg_price" },
    { "column": "quantity", "aggregation": "sum", "as": "total_qty" },
    { "column": "id", "aggregation": "count", "as": "n" }
  ],
  "groupBy": ["region"],
  "orderBy": [{ "column": "total_qty", "direction": "desc" }]
}
```

**IN filter with limit:**

```json
{
  "source": "my_project.orders",
  "select": [{ "column": "*" }],
  "filters": [
    { "column": "status", "operator": "IN", "value": ["shipped", "delivered"] },
    { "column": "amount", "operator": ">=", "value": 100 }
  ],
  "limit": 200
}
```

**Global aggregation (no GROUP BY):**

```json
{
  "source": "my_project.sales",
  "select": [
    { "column": "price", "aggregation": "min", "as": "min_price" },
    { "column": "price", "aggregation": "max", "as": "max_price" },
    { "column": "price", "aggregation": "avg", "as": "avg_price" }
  ]
}
```

---

## 5. Schema API

Discover the column schema of an Iceberg table.

---

### `GET /api/v1/schema/{source}`

**Path parameter:** `source` = `"projectId.tableName"`

Triggers a schema discovery job. The first call after a table is created or updated hits Spark + Iceberg (~ 20–200 ms). Subsequent calls are served from Redis cache (~ 10 ms) until new data is ingested.

**Response** — `200 OK`:

```json
{
  "jobId": "schema-7f3d9a...",
  "status": "QUEUED",
  "checkStatusAt": "/api/v1/schema/status/schema-7f3d9a..."
}
```

---

### `GET /api/v1/schema/status/{jobId}`

Returns the current state of a schema job.

**Completed response:**

```json
{
  "status": "COMPLETED",
  "rowCount": "6",
  "resultData": "[{\"name\":\"id\",\"type\":\"int\",\"nullable\":true},{\"name\":\"product\",\"type\":\"string\",\"nullable\":true},{\"name\":\"price\",\"type\":\"double\",\"nullable\":true}]",
  "createdAt": "2026-03-05T10:00:00Z",
  "updatedAt": "2026-03-05T10:00:00Z"
}
```

The `resultData` field is a JSON string. When parsed, it is an array of column objects:

| Field      | Type    | Description                                                                  |
| ---------- | ------- | ---------------------------------------------------------------------------- |
| `name`     | string  | Column name                                                                  |
| `type`     | string  | Data type — e.g. `"int"`, `"string"`, `"double"`, `"boolean"`, `"timestamp"` |
| `nullable` | boolean | Whether the column allows null values                                        |

---

### `GET /api/v1/schema/status/{jobId}/wait?timeoutSec=30`

Long-poll — same behaviour as the other `/wait` endpoints. Usually resolves in under 200 ms for cached schemas.

---

## 6. How `/wait` Works

All three job types expose a `/wait` variant:

| Job type  | Wait endpoint                               |
| --------- | ------------------------------------------- |
| Ingestion | `GET /api/v1/ingestion/status/{jobId}/wait` |
| Query     | `GET /api/v1/query/{jobId}/wait`            |
| Schema    | `GET /api/v1/schema/status/{jobId}/wait`    |

**Query parameter:** `timeoutSec` — how long to wait (default 30, max 300).

### How it works

The server does **not** poll internally. Instead it subscribes to a Redis Pub/Sub channel (`job-done:{jobId}`). When the worker finishes the job, it publishes a message on that channel. The HTTP response resolves **instantly** — the same millisecond the worker writes `COMPLETED`.

There is also a **fast-path**: if the job is already terminal when you call `/wait`, it returns immediately without subscribing.

If the timeout expires before the job finishes, the server returns the current (non-terminal) status. It does **not** return an error — always check the `status` field in the response.

### Key takeaways

- `/wait` is **not polling**. One HTTP call replaces the entire "loop and check" pattern.
- It resolves with **zero delay** once the worker is done.
- You can call both `/wait` and the SSE stream at the same time — they don't interfere.
- If `/wait` comes back with a non-terminal status, the timeout expired. Just call `/wait` again.

---

## 7. How the SSE Stream Works

```
GET /api/v1/query/{jobId}/stream
Accept: text/event-stream
```

This endpoint opens a Server-Sent Events connection and pushes query result rows to the client as they become available.

**Important:** the server waits internally for the job to reach `COMPLETED` before emitting any events. This means you can open the stream connection **immediately** after submitting the query — even before the worker has started processing. The connection will simply stay open until results are ready, then events flow.

### Event Sequence

Every stream always produces events in this order:

```
event: metadata          ← first, exactly once — column names
event: batch             ← one or more — up to 500 rows per batch
  ...
event: batch             ← last batch (may be < 500 rows)
event: complete          ← last event — signals end of stream
```

### Event Payloads

#### `metadata`

A JSON array of column name strings — acts as the header row.

```
event: metadata
data: ["id","product","price","quantity","date","region"]
```

Use this to set up your table headers or know the column order before rows arrive.

#### `batch`

A JSON array of row objects. Each object maps column name to value. Up to 500 rows per batch.

```
event: batch
data: [{"id":1,"product":"Laptop","price":999.0,"quantity":5},{"id":2,"product":"Mouse","price":29.0,"quantity":120}]
```

You will receive multiple `batch` events for large results. Append each batch to your data as it arrives — this is what makes it "progressive". You can start rendering rows in your UI before the full result has arrived.

#### `complete`

A summary object, signals the stream is finished.

```
event: complete
data: {"totalBatches": 4, "totalRows": 1600}
```

After receiving `complete`, close the connection. No more events will follow.

#### `error` (rare)

Only emitted if something goes wrong during streaming itself (not for query failures, which are reported via the status endpoint).

```
event: error
data: {"error": "Stream timeout"}
```

### Small Results (Inline Fallback)

If the query produced ≤ 1 000 rows, the result is stored inline in the status hash (not in a Redis Stream). The SSE endpoint handles this transparently:

- It emits **one** `batch` event containing all rows.
- It emits **one** `complete` event.
- No `metadata` event is emitted for inline results (the column names are visible as the keys in each row object).

**This means you can always use the SSE endpoint regardless of result size.** The same event types arrive either way — the only difference is that small results come as a single batch instead of multiple.

---

## 8. Integration Patterns — Retrieving Query Results

After you `POST /api/v1/query`, you get back a `jobId` and a `streamUrl` immediately. The query hasn't finished yet — a worker will pick it up and execute it. **Your next question is: how do I get the results?**

There are two recommended patterns and one discouraged one.

---

### Pattern 1 — Use `/wait`, then check the `streamed` flag

The `/wait` response itself tells you which path to take via the `streamed` field.

**Steps:**

1. **Submit the query** — `POST /api/v1/query`. Save `jobId` and `streamUrl` from the response.
2. **Wait for completion** — `GET /api/v1/query/{jobId}/wait`. This single call blocks until the job is done.
3. **Check the `streamed` field** in the response:
   - If `streamed` is `"false"` → the result is small. All rows are right there in the `resultData` field as a JSON string. Parse it and you're done. No stream needed.
   - If `streamed` is `"true"` → the result is large. `resultData` is `null`. Open an `EventSource` connection to the `streamUrl` and read the `batch` events as they arrive.

**Flow:**

```
POST /api/v1/query
  → { jobId, streamUrl }

GET /api/v1/query/{jobId}/wait
  → { status, streamed, resultData, ... }
        │
        ├── streamed = "false"
        │       → resultData contains all rows as JSON string
        │       → parse it, done
        │
        └── streamed = "true"
                → resultData is null
                → open EventSource to streamUrl
                → listen for metadata → batch → ... → complete
```

**When to use this:** when you want to avoid the overhead of an SSE connection for tiny results (e.g. a single aggregation row, a COUNT query returning one number). For those cases, the data is already in the `/wait` response and opening a stream would be unnecessary.

---

### Pattern 2 — Always use the SSE stream (simplest)

The SSE endpoint handles both small and large results uniformly. For small results it still emits `batch` (all rows in one batch) → `complete`. You get the same event format either way, so you can just always connect to the stream and never worry about the result size.

**Steps:**

1. **Submit the query** — `POST /api/v1/query`. Save `streamUrl` from the response.
2. **Open the stream** — connect an `EventSource` to the `streamUrl`.
3. **Listen for events** — `metadata`, `batch`, `complete`. That's it.

You don't need to call `/wait` at all. The SSE endpoint waits internally for the job to finish before it starts emitting events. You can open the connection right after submitting.

**Flow:**

```
POST /api/v1/query
  → { jobId, streamUrl }

EventSource(streamUrl)
  ← event: metadata   (column names)
  ← event: batch      (rows — one batch for small results, many for large)
  ← ...
  ← event: complete   (done, close connection)
```

**When to use this:** when you want the simplest possible integration and one consistent code path. This is what the project's own `test_system.sh` does — it always reads the stream regardless of result size.

---

### Pattern 3 — Poll (discouraged)

You would call `GET /api/v1/query/{jobId}` in a loop, checking `status` each time.

**Don't do this.** A query that takes 400 ms could sit undiscovered for however long your polling interval is. It wastes network and Redis round-trips for no benefit. The `/wait` endpoint exists to replace this pattern entirely.

---

### Which pattern should I use?

| Situation                                                                                                        | Recommended pattern                                       |
| ---------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| You just want it to work — one code path for everything                                                          | **Pattern 2** — always stream                             |
| Most of your queries return small results (aggregations, lookups) and you want to skip stream overhead for those | **Pattern 1** — wait + check `streamed` flag              |
| You have a dashboard where results must appear progressively (table filling row by row)                          | **Pattern 2** — always stream                             |
| You need to know the exact row count before displaying anything                                                  | **Pattern 1** — wait gives you `rowCount` in the response |

Both Pattern 1 and Pattern 2 are correct. Pattern 2 is simpler. Pattern 1 gives you more control. Either way, never use Pattern 3.

---

## Endpoint Summary

Quick-reference table of every endpoint:

| Method | Endpoint                                | Purpose                      |
| ------ | --------------------------------------- | ---------------------------- |
| `POST` | `/api/v1/ingestion/upload`              | Upload and ingest a file     |
| `GET`  | `/api/v1/ingestion/status/{jobId}`      | Check ingestion status       |
| `GET`  | `/api/v1/ingestion/status/{jobId}/wait` | Wait for ingestion to finish |
| `POST` | `/api/v1/query`                         | Submit a query               |
| `GET`  | `/api/v1/query/{jobId}`                 | Check query status           |
| `GET`  | `/api/v1/query/{jobId}/wait`            | Wait for query to finish     |
| `GET`  | `/api/v1/query/{jobId}/stream`          | Stream query results (SSE)   |
| `GET`  | `/api/v1/schema/{source}`               | Get table schema             |
| `GET`  | `/api/v1/schema/status/{jobId}`         | Check schema status          |
| `GET`  | `/api/v1/schema/status/{jobId}/wait`    | Wait for schema to finish    |
