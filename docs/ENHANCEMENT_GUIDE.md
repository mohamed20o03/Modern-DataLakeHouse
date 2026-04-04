# Enhancement Guide: Production-Ready Features

A conceptual guide to implementing critical features for production-scale data platforms.

> **Implementation status:**
>
> - **Streaming / SSE** — ✅ Implemented
> - **File Compaction** — ✅ Implemented (via CDC worker bin-pack compaction)
> - **Partitioning** — not yet implemented (conceptual design below)
> - **Query Optimizer** — not yet implemented (conceptual design below)

---

## Table of Contents

1. [Partitioning Strategy](#partitioning-strategy) — not yet implemented
2. [File Compaction](#file-compaction) — ✅ implemented
3. [Pagination & Streaming](#pagination--streaming) — ✅ implemented

---

## Partitioning Strategy

> **Status:** Not yet implemented. The design below describes the intended approach.

### The Problem

**Current state:** All data for a table is stored in one flat directory structure:

```
s3://warehouse/my_project/sales/
  ├── data/
  │   ├── 00000-data.parquet  (rows from 2026-01-01 to 2026-02-28)
  │   ├── 00001-data.parquet  (rows from 2025-12-15 to 2026-03-10)
  │   ├── 00002-data.parquet  (rows from 2026-01-20 to 2026-02-10)
  │   └── 00003-data.parquet  (rows from 2025-11-01 to 2026-04-15)
```

**When a query runs:**

```sql
SELECT * FROM sales WHERE date >= '2026-02-01' AND date <= '2026-02-28'
```

Spark **must read all 4 Parquet files** to find matching rows, even though only 00000 and 00002 contain February data.

**Impact:**

- **Without partitioning:** Read 4 × 128 MB = 512 MB of data
- **With date partitioning:** Read only 2 × 128 MB = 256 MB (50% reduction)
- **At petabyte scale:** 500 TB → 250 TB (real example: Uber reduces query time 10–100×)

### How Partitioning Works in Iceberg

Iceberg stores partitioning metadata in the **manifest file** — a mini-index that maps partition values to data files:

```json
{
  "manifests": [
    {
      "partition_key": "date=2026-02-01",
      "data_files": ["00000-data.parquet", "00002-data.parquet"]
    },
    {
      "partition_key": "date=2026-03-01",
      "data_files": ["00001-data.parquet"]
    }
  ]
}
```

When you filter `WHERE date >= '2026-02-01'`, Iceberg reads the manifest and says:

> "I only need to touch partitions 2026-02-01 and 2026-03-01. Skip 2026-01-01 and 2026-04-01."

This is called **partition pruning** or **predicate pushdown**.

### Partitioning Strategies

#### 1. **Date-Based Partitioning (Most Common)**

Best for:

- Time-series data (events, transactions, logs)
- Daily/weekly/monthly reports
- Data retention policies (delete old partitions quickly)

```sql
CREATE TABLE sales (
  id INT,
  amount DECIMAL,
  customer_id INT,
  purchase_date DATE
)
USING iceberg
PARTITIONED BY (month(purchase_date))
```

Results in:

```
s3://warehouse/sales/
  ├── purchase_date_month=202601/
  │   ├── data-00000.parquet
  │   └── data-00001.parquet
  ├── purchase_date_month=202602/
  │   ├── data-00002.parquet
  │   └── data-00003.parquet
  ├── purchase_date_month=202603/
  │   └── data-00004.parquet
```

**Query benefit:**

```sql
-- Scans ONLY partition 202602
SELECT * FROM sales WHERE purchase_date >= '2026-02-01' AND purchase_date < '2026-03-01'
```

#### 2. **Category-Based Partitioning**

Best for:

- Multi-tenant systems (partition by customer_id or tenant_id)
- Geographic data (partition by region/country)
- Product data (partition by category)

```sql
CREATE TABLE products (
  id INT,
  name STRING,
  category STRING,
  price DECIMAL
)
USING iceberg
PARTITIONED BY (category)
```

Results in:

```
s3://warehouse/products/
  ├── category=Electronics/
  │   ├── data-00000.parquet (laptops, phones, etc.)
  └── category=Clothing/
      ├── data-00001.parquet (shirts, pants, etc.)
```

**Query benefit:**

```sql
-- Scans ONLY Electronics partition
SELECT AVG(price) FROM products WHERE category = 'Electronics'
```

#### 3. **Composite Partitioning (Advanced)**

Best for:

- Large datasets that need multiple filter dimensions
- Fast queries on both date AND category

```sql
CREATE TABLE events (
  id INT,
  user_id INT,
  event_type STRING,
  timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (year(timestamp), event_type)
```

Results in:

```
s3://warehouse/events/
  ├── timestamp_year=2025/event_type=click/
  │   └── data-00000.parquet
  ├── timestamp_year=2025/event_type=view/
  │   └── data-00001.parquet
  ├── timestamp_year=2026/event_type=click/
  │   └── data-00002.parquet
  ├── timestamp_year=2026/event_type=view/
  │   └── data-00003.parquet
```

**Query benefit:**

```sql
-- Scans ONLY 2 partitions: year=2026 AND event_type=click
SELECT COUNT(*) FROM events
WHERE YEAR(timestamp) = 2026 AND event_type = 'click'
```

### Implementation Strategy

**Key Concept:** Use a PartitionStrategyService that automatically detects and applies the best partitioning approach per table.

**Detection Priority:**

1. Look for temporal columns (DATE or TIMESTAMP types) → Use temporal partitioning with month() transformation
2. If no temporal columns found, look for categorical columns (low-cardinality strings) → Use categorical partitioning
3. If neither found → Store without partitioning (but still benefit from Iceberg ACID guarantees)

**Integration Points:**

- Add partition detection service that analyzes each table's schema
- Modify ingestion worker to analyze table before writing to Iceberg
- Cache partition strategy for future queries on the same table
- Log detected partition column and expected performance impact

### Performance Impact

**Before partitioning (current state):**

```
Query: SELECT * FROM sales WHERE date = '2026-02-15'
Files scanned: 4 (all files, regardless of date)
Data read: 512 MB
Time: 5.2 seconds
```

**After date partitioning:**

```
Query: SELECT * FROM sales WHERE date = '2026-02-15'
Files scanned: 1 (partition 202602 only, via manifest)
Data read: 128 MB
Time: 0.8 seconds
Speedup: 6.5×
```

**At scale (1000 daily partitions, 1 month query):**

```
Before: Read 30 × 128 MB = 3.8 GB  → 45 seconds
After:  Read 30 × 128 MB = 3.8 GB  → 2 seconds (manifest prunes 970 partitions instantly)
```

---

## File Compaction

> **Status: ✅ Implemented.** Bin-pack compaction is available as a maintenance CLI command in the CDC worker. It uses Iceberg's `RewriteDataFiles` action with the `bin-pack` strategy.
>
> **What was built:**
>
> - `cdc-worker/.../maintenance/CompactionService.java` — runs `SparkActions.get(spark).rewriteDataFiles(table)` with configurable target file size
> - CLI trigger: `docker compose run --rm cdc-worker --compact <tableName>`
> - Reports status to Redis (`PROCESSING` → `COMPLETED` with file stats)
>
> **Verified performance:** 7 small data files → 1 compacted file (85% reduction), row count unchanged.

### The Problem

**Current behavior:** Every ingestion creates a new Parquet file.

```
Day 1: Upload 100 rows → 1 file created
  sales/202602/data-00000.parquet (100 rows, 1 MB)

Day 2: Upload 50 rows → 1 file created
  sales/202602/data-00001.parquet (50 rows, 0.5 MB)

Day 3: Upload 75 rows → 1 file created
  sales/202602/data-00002.parquet (75 rows, 0.75 MB)

... (repeat 365 times per year)
```

After 1 year, partition `202602` has:

- **28 tiny Parquet files** (1 per day)
- **Total: 112 files** in one partition
- Each read operation must open **112 files** and merge results
- Iceberg keeps **112 entries in the manifest** → slower metadata reads

### The Cost

**Scenario: Query on February 2026 data with 28 daily uploads**

```
Without compaction:
├─ File open overhead: 28 × 10ms = 280 ms
├─ Metadata read: 28 entries = slow manifest parsing
├─ Network round-trips to S3: 28 calls (S3 timeout: 100ms per call)
└─ Total overhead: 280 ms + metadata + 2.8 seconds = 3+ seconds just for setup

With compaction (1 file per partition):
├─ File open overhead: 1 × 10ms = 10 ms
├─ Metadata read: 1 entry = fast
├─ Network round-trips to S3: 1 call
└─ Total overhead: < 100 ms setup
```

**For a query that should take 1 second, you're adding 3+ seconds of overhead.**

### How Iceberg Compaction Works

Iceberg **compaction** is the process of merging multiple small files into larger ones:

```
Before compaction:
  data-00000.parquet (100 rows)
  data-00001.parquet (50 rows)
  data-00002.parquet (75 rows)
  data-00003.parquet (40 rows)
  data-00004.parquet (60 rows)
  Total: 5 files, 325 rows

After compaction:
  data-00005.parquet (325 rows)  ← merged file
  Total: 1 file, 325 rows
```

The manifest is updated, and old files are marked for deletion (garbage collection).

### Compaction Strategies

#### 1. **Eager Compaction** (After Every Ingestion)

**When to use:** Small frequent uploads (100-1000 rows per day), query latency is critical

**How it works:** Immediately after writing new data to Iceberg, merge small files into larger ones

**Trade-offs:**

- ✅ Queries always fast (no pending small files)
- ✅ Minimal overhead during query time
- ❌ Compaction adds time to ingestion (2-5 seconds per upload)

**Best for:** Your system (small daily uploads, read-heavy workload)

#### 2. **Lazy Compaction** (Nightly Batch)

**When to use:** Many daily uploads (1000+), off-peak window available, ingestion latency critical

**How it works:** Uploads happen immediately without compaction, scheduled job runs at night (e.g., 2 AM) to merge all daily files

**Trade-offs:**

- ✅ Ingestion is fast (no compaction overhead)
- ❌ Queries slow until next compaction window
- ✅ Predictable compaction schedule (infrastructure planning easier)

**Best for:** Systems with predictable traffic patterns and off-peak windows

#### 3. **Heuristic Compaction** (Adaptive)

**When to use:** Mixed workloads, query performance important but not critical, variable ingestion patterns

**How it works:** Monitor file count per partition, trigger compaction only when count exceeds threshold (e.g., > 10 files)

**Decision logic:**

- Count distinct files in table
- If file_count > threshold → trigger compaction
- Otherwise → skip (save resources)

**Trade-offs:**

- ✅ Balanced approach (compact only when needed)
- ✅ Efficient resource usage
- ❌ Unpredictable compaction timing
- ❌ Need to tune threshold per table

**Best for:** Production systems with varying workloads

### Target File Size

**Recommendation:** 128 MB per Parquet file

**Why?**

- Iceberg distributed reads: Files smaller than 128 MB → limited parallelism
- Network efficiency: 128 MB files = good chunk for S3 transfers
- Memory efficiency: 128 MB fits in most executor memory allocations
- Compaction overhead: Too small (< 64 MB) → too many compactions; too large (> 512 MB) → slow reads

### When Compaction Becomes Critical

```
Scenario: Daily small uploads without compaction for 1 year

After 365 days:
  - 365 small Parquet files in a partition
  - Each file: 1-10 MB
  - Total data: 1-10 GB

Performance impact on read query:
  WITHOUT compaction: Open 365 files, merge metadata, slower reads
  WITH compaction: Open 1-2 files, fast reads

  Speedup: 4-10× faster on queries
```

### Performance Impact

**After 30 daily uploads to a partition:**

```
Before compaction:
  Files: 7
  Read time: ~3 seconds (file open overhead)

After compaction:
  Files: 1
  Read time: ~0.5 seconds
  Speedup: 6×
```

### How to Run Compaction

```bash
# Run compaction on a specific Iceberg table
docker compose run --rm cdc-worker --compact iceberg.cdc_namespace.customers

# Check file count before/after
docker exec cdc-worker /opt/spark/bin/spark-sql \
  --conf "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.iceberg.type=rest" \
  -e "SELECT count(*) FROM cdc_namespace.customers.files WHERE content = 0;"
```

---

## Pagination & Streaming

> **Status: ✅ Implemented.** SSE-based streaming is fully working. The system streams query results progressively via Server-Sent Events. See the [API Reference](API_REFERENCE.md#7-how-the-sse-stream-works) for frontend usage.
>
> **What was built:**
>
> - `api-service/.../module/query/streaming/QueryStreamController.java` — SSE endpoint at `/api/v1/query/{jobId}/stream`
> - `api-service/.../module/query/streaming/QueryStreamService.java` — reads Redis Streams, pushes via `SseEmitter`, with inline fallback
> - `query-worker/.../streaming/redis/RedisResultStreamPublisher.java` — publishes result batches (500 rows each) to Redis Streams
> - `query-worker/.../engine/spark/QueryResultWriter.java` — inline threshold (≤ 1 000 rows) vs Redis Stream decision
>
> The pagination alternative described below was **not** implemented — streaming was chosen instead.

### The Problem

**Current behavior:** Large query results are buffered completely before sending to client

**Flow:**

1. User queries 100,000 rows
2. Spark reads all 100,000 rows into memory
3. Serializes entire result to JSON
4. Uploads to MinIO as a file
5. Returns file path to client
6. Client makes **second** request to download the file

**Issues:**

- ❌ Blocks client for 5-30 seconds (waiting for full result collection)
- ❌ Requires second round-trip to MinIO (extra 1-2 second latency)
- ❌ Wastes storage (result file deleted after 1 hour)
- ❌ Memory spike if result is 1M+ rows
- ❌ Poor user experience (user sees nothing until complete)

### The Solution: Streaming Results

**New approach:** Server streams results chunk-by-chunk to client

**Flow:**

1. User queries 100,000 rows
2. Server begins streaming immediately (no waiting)
3. Client receives first rows in 100ms
4. Client processes rows as they arrive (streaming pattern)
5. No MinIO write, no file download needed
6. Single HTTP request

**Benefits:**

- ✅ Client sees first rows in 100ms (vs. 5+ seconds)
- ✅ Low memory usage (process 1 chunk at a time, not all rows)
- ✅ No MinIO overhead
- ✅ Single HTTP request (vs. two)
- ✅ Better user experience (progressive loading)

### Streaming Output Formats

#### 1. **JSONL** (JSON Lines)

**Format:** One JSON object per line

```
{"id":1,"product":"Laptop","price":1200}
{"id":2,"product":"Mouse","price":25}
{"id":3,"product":"Keyboard","price":85}
```

**Use case:** Web applications, data pipelines consuming line-by-line

**Benefits:**

- Human readable
- Efficient (1 object per line = easy to parse)
- Backward compatible with standard JSON parsers
- Works well with streaming processing

#### 2. **CSV** (Comma-Separated Values)

**Format:** Standard CSV with header row

```
id,product,price
1,Laptop,1200
2,Mouse,25
3,Keyboard,85
```

**Use case:** Excel, Google Sheets, business intelligence tools

**Benefits:**

- Universal compatibility
- No special parsing needed
- Works with all spreadsheet software
- Good for non-technical users

#### 3. **Parquet** (Binary Columnar Format)

**Format:** Binary Parquet file streamed in chunks

**Use case:** Data pipelines, downstream Spark/Iceberg processing, large datasets

**Benefits:**

- Highly compressed (50-90% compression)
- Columnar format (queries specific columns efficiently)
- Preserves data types (no need to parse strings as numbers)
- Best for large-scale processing

### Streaming vs Pagination

#### Streaming (Recommended)

**When user requests:** `GET /api/v1/query/job-xyz/stream`

**Behavior:**

- Server streams results continuously
- Client processes as data arrives
- Total latency: time for first chunk + processing time

**Best for:**

- Large result sets (10K+ rows)
- Real-time dashboards
- Data analysis workflows
- Bandwidth-constrained connections

#### Pagination (Alternative)

**When user requests:** `GET /api/v1/query/job-xyz/page?page=0&size=100`

**Behavior:**

- Server returns one page at a time (e.g., 100 rows)
- Client requests next page with `?page=1`
- User navigates page-by-page

**Best for:**

- Web UI with pagination controls
- Browsing data (not analyzing)
- Small result sets
- Users who want to see results one page at a time

### Implementation Considerations

**Streaming Response Body:**

- HTTP status 200 (OK)
- Content-Type: application/x-ndjson (JSONL) or text/csv or application/octet-stream (Parquet)
- Content-Encoding: gzip (optional, for compression)
- Chunked transfer encoding (server sends data as available, not all at once)

**Error Handling:**

- If query not COMPLETED: Return 400 error immediately
- If job not found: Return 404 error
- If streaming fails midway: Client sees partial data

**Batch Size:**

- Default: 1000 rows per chunk
- Tunable: Client can specify `?batchSize=500` for smaller chunks
- Trade-off: Smaller chunks = more responsive; larger chunks = fewer network calls

### Performance Impact

---

## Implementation Roadmap

### Phase 1: Partitioning (1-2 weeks) — not yet started

1. Add partition column detection
2. Update SparkEngine to create partitioned tables
3. Test with date-based partitions
4. Measure query speedup

### Phase 2: Compaction ✅ Done

1. ✅ Implement CompactionService with bin-pack strategy
2. ✅ Add CLI trigger (`--compact <table>`)
3. ✅ Report compaction stats to Redis
4. ✅ Verified: 7 files → 1 file (85% reduction)

Compaction is available as a maintenance operation:
```bash
docker compose run --rm cdc-worker --compact iceberg.cdc_namespace.customers
```

Additionally, snapshot tagging and expiration were implemented:
- `docker compose run --rm cdc-worker --tag <table> <tagName>` — tag a snapshot for ML training
- `docker compose run --rm cdc-worker --expire-snapshots <table> [retainDays]` — clean up old snapshots (tagged survive)

### Phase 3: Query Optimization (2 weeks) — not yet started

1. Implement QueryOptimizer
2. Add REST endpoints for EXPLAIN/ANALYZE
3. Test with various query patterns
4. Document performance insights

### Phase 4: Streaming ✅ Done

1. ✅ Implement QueryStreamController (SSE endpoint)
2. ✅ Implement Redis Streams result publishing from query worker
3. ✅ Handle both inline (≤ 1 000 rows) and streamed (> 1 000 rows) results
4. ✅ Test with large result sets via `test_system.sh`

---

## Testing

Integration testing via the existing end-to-end test suite:

```bash
bash test_system.sh
```

This exercises ingestion, schema retrieval, queries, and SSE streaming. When partitioning and compaction are implemented, add targeted tests for:

- Partition column detection (date → month transform, categorical fallback)
- Compaction triggering and file count reduction
- Query plan output for EXPLAIN/ANALYZE endpoints

```

---

## Summary

| Enhancement        | Effort    | Impact                   | Status              |
| ------------------ | --------- | ------------------------ | ------------------- |
| Partitioning       | 1-2 weeks | 10-100× query speedup    | Not yet implemented |
| Compaction         | 1 week    | 4-10× query speedup      | ✅ **Done**          |
| Query Optimization | 2 weeks   | visibility + 20% speedup | Not yet implemented |
| Streaming          | 2 weeks   | 100× memory reduction    | ✅ **Done**          |

**Remaining effort:** ~3–4 weeks for partitioning and query optimization.
```
