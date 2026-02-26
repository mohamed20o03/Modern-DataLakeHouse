# Query Worker — Engine Layer: Complete Guide

---

## Answer First: What Is the Best Way to Learn This Code?

**Do all three, in this exact order:**

1. **Read this README** — get the big picture, the vocabulary, and _why_ things exist before looking at a single line of code.
2. **Read the code** — now that you know what each class is supposed to do, the code will make sense instead of being a wall of text.
3. **Rewrite one small piece** — pick one method (e.g. `applyFilters`) and rewrite it from scratch without looking. This forces your brain to actually encode it. If you can rewrite it, you understand it.

> Reading alone is passive. You feel like you understand, but when you close the file the knowledge evaporates. Rewriting one function tells you exactly where your understanding has gaps.

---

## Table of Contents

1. [What is the Engine Layer?](#1-what-is-the-engine-layer)
2. [The Full Picture — One Diagram](#2-the-full-picture--one-diagram)
3. [The Interfaces — Contracts First](#3-the-interfaces--contracts-first)
4. [SparkEngine — Booting Spark](#4-sparkengine--booting-spark)
5. [SparkStorageConfigurer — Credentials Injection](#5-sparkstorageconfigurerscredentials-injection)
6. [SparkService — The Orchestrator](#6-sparkservice--the-orchestrator)
7. [SparkQueryBuilder — Building the DataFrame](#7-sparkquerybuilder--building-the-dataframe)
8. [QueryResultWriter — Persisting the Result](#8-queryresultwriter--persisting-the-result)
9. [QueryEngineFactory — The Entry Point](#9-queryenginefactory--the-entry-point)
10. [Data Flow: A Real Example End-to-End](#10-data-flow-a-real-example-end-to-end)
11. [Design Principles Used](#11-design-principles-used)
12. [Vocabulary Glossary](#12-vocabulary-glossary)

---

## 1. What is the Engine Layer?

The engine layer is the **brain** of the query worker. Its job is:

1. Receive a message from RabbitMQ that says _"run this query on this table"_
2. Start Apache Spark
3. Use Spark to actually read and filter data from Iceberg tables stored in MinIO
4. Write the result as a Parquet file back to MinIO
5. Report the job status (PROCESSING → COMPLETED or FAILED) to Redis

Everything in `engine/` and `engine/spark/` is responsible for this.

---

## 2. The Full Picture — One Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            QUERY WORKER STARTUP                              │
│                                                                              │
│  QueryWorkerMain                                                             │
│       │                                                                      │
│       ▼                                                                      │
│  QueryEngineFactory.create()      ← reads QUERY_ENGINE env var              │
│       │                                                                      │
│       ▼                                                                      │
│  SparkEngine(icebergCatalogUri, icebergWarehouse)   ← constructor, no Spark │
│       │                                                                      │
│       ▼ .initialize(jobStatusService, storageConfig)                        │
│  SparkStorageConfigurer.apply(builder, storageConfig)  ← inject credentials │
│       │                                                                      │
│       ▼ .getOrCreate()                                                       │
│  SparkSession (running)                                                      │
│       │                                                                      │
│       ▼                                                                      │
│  new SparkService(spark, jobStatusService, storageConfig)                    │
│       │                                                                      │
│       ▼ returned to QueryWorkerMain as QueryService                         │
│  consumer.start()   ← begins listening on RabbitMQ                          │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                        PER MESSAGE (one job)                                 │
│                                                                              │
│  RabbitMQConsumer receives message                                           │
│       │                                                                      │
│       ▼ queryService.query(message)                                          │
│  SparkService.query(message)                                                 │
│       │                                                                      │
│       ├─ Redis: PROCESSING                                                   │
│       │                                                                      │
│       ├─ resolveIcebergTable(source)  → "iceberg.projectId.tableName"       │
│       │                                                                      │
│       ├─ isSchemaJob?  (jobId starts with "schema-")                        │
│       │       │                                                              │
│       │    YES ▼                               NO ▼                         │
│       │  executeSchemaJob()           executeQueryJob()                      │
│       │       │                               │                             │
│       │  read StructType              parseQueryRequest(queryJson)           │
│       │  limit(0) — no data           → QueryRequest object                 │
│       │  return schema rows                   │                             │
│       │                               SparkQueryBuilder.build()             │
│       │                               → Dataset<Row>                        │
│       │                                       │                             │
│       │                               QueryResultWriter.write()             │
│       │                               → Parquet on MinIO                    │
│       │                               → QueryResult                         │
│       │                                                                      │
│       └─ Redis: COMPLETED (with resultPath, rowCount, resultData)           │
│           OR                                                                 │
│          Redis: FAILED (with error message)                                  │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. The Interfaces — Contracts First

### `QueryEngine.java`

```
QueryEngine
├── initialize(jobStatusService, storageConfig) → QueryService
└── close()
```

This is a **lifecycle contract**. It answers: _"How do I boot an engine and how do I shut it down?"_

- `initialize()` — starts the engine (creates the SparkSession), wires dependencies, and returns a ready-to-use `QueryService`. This is where the expensive startup cost happens (Spark takes a few seconds to start).
- `close()` — shuts down the engine cleanly. Called on JVM shutdown to release memory, threads, and network connections.

**Why it's an interface:** tomorrow you could add `DuckDBEngine` or `FlinkEngine` without changing anything else. The rest of the codebase only knows about `QueryEngine`, not about Spark.

---

### `QueryService.java`

```
QueryService
└── query(QueryMessage message)
```

This is the **per-job contract**. It answers: _"How do I run one job?"_

- `query()` — receives the message from RabbitMQ and does everything needed to execute one job end-to-end: read data, process it, write results, update Redis.

---

## 4. SparkEngine — Booting Spark

**File:** `engine/spark/SparkEngine.java`

**Single responsibility:** Create and manage the `SparkSession`. Nothing else.

### Constructor

```java
public SparkEngine(String icebergCatalogUri, String icebergWarehouse)
```

- Just saves two strings. Does NOT start Spark.
- Spark is expensive to start — we delay it until `initialize()` is explicitly called.

### `initialize(jobStatusService, storageConfig)`

This is where Spark actually boots. It does three things:

**Step 1 — Build the SparkSession configuration:**

```java
SparkSession.Builder builder = SparkSession.builder()
    .appName("query-worker")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions...")  // enable Iceberg SQL
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")  // register catalog
    .config("spark.sql.catalog.iceberg.type", "rest")                              // REST catalog type
    .config("spark.sql.catalog.iceberg.uri", icebergCatalogUri)                    // catalog server URL
    .config("spark.sql.catalog.iceberg.warehouse", icebergWarehouse)               // S3 warehouse root
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"); // file I/O
```

What each config does:
| Config key | What it does |
|---|---|
| `spark.sql.extensions` | Enables Iceberg SQL features (time travel, MERGE INTO, etc.) |
| `spark.sql.catalog.iceberg` | Registers a catalog named `iceberg` backed by Iceberg's SparkCatalog |
| `spark.sql.catalog.iceberg.type` | Says "use the REST catalog" (talks to the Iceberg REST server) |
| `spark.sql.catalog.iceberg.uri` | URL of the Iceberg REST catalog server |
| `spark.sql.catalog.iceberg.warehouse` | The S3 path prefix where all table data lives |
| `spark.sql.catalog.iceberg.io-impl` | Uses S3FileIO so Iceberg reads/writes table files via S3 API |

**Step 2 — Inject storage credentials:**

```java
spark = SparkStorageConfigurer.apply(builder, storageConfig).getOrCreate();
```

Delegates credential injection to `SparkStorageConfigurer` (see section 5), then calls `.getOrCreate()` which actually starts Spark.

**Step 3 — Create and return SparkService:**

```java
return new SparkService(spark, jobStatusService, storageConfig);
```

### `close()`

```java
spark.close();  // stops the SparkContext, releases executors, network, memory
```

---

## 5. SparkStorageConfigurer — Credentials Injection

**File:** `engine/spark/SparkStorageConfigurer.java`

**Single responsibility:** The only place that knows how to map a `StorageConfig` to Spark config keys. Nothing else.

### Why does this class exist?

Without it, `SparkEngine` would need to know S3A config key strings like `"spark.hadoop.fs.s3a.access.key"`. That's a different concern — it's storage knowledge, not engine lifecycle knowledge. By extracting it here, `SparkEngine` stays clean.

### `apply(builder, config)`

```java
public static SparkSession.Builder apply(SparkSession.Builder builder, StorageConfig config) {
    return switch (config.getType()) {
        case "minio" -> applyMinio(builder, config);
        case "s3"    -> applyS3(builder, config);
        default      -> applyMinio(builder, config);  // safe fallback
    };
}
```

Reads the `type` field from `StorageConfig` and routes to the right method.

### `applyMinio(builder, config)`

MinIO requires **two separate configuration layers** because two different libraries need to know the credentials:

```
┌─ Layer 1: Iceberg S3FileIO ─────────────────────────────────────┐
│  spark.sql.catalog.iceberg.s3.endpoint      = MinIO URL         │
│  spark.sql.catalog.iceberg.s3.path-style-access = true          │
│                                                                  │
│  Used by: Iceberg itself when reading/writing table metadata     │
│           and data files directly through Iceberg's S3FileIO    │
└──────────────────────────────────────────────────────────────────┘

┌─ Layer 2: Hadoop S3AFileSystem ─────────────────────────────────┐
│  spark.hadoop.fs.s3a.endpoint      = MinIO URL                  │
│  spark.hadoop.fs.s3a.access.key    = access key                 │
│  spark.hadoop.fs.s3a.secret.key    = secret key                 │
│  spark.hadoop.fs.s3a.path.style.access = true                   │
│  spark.hadoop.fs.s3a.impl          = S3AFileSystem              │
│  spark.hadoop.fs.s3a.aws.credentials.provider = SimpleAWSCred.. │
│                                                                  │
│  Used by: Spark's DataFrame reader/writer when streaming raw     │
│           file bytes (parquet write, parquet read)              │
└──────────────────────────────────────────────────────────────────┘
```

**Why `path-style-access = true`?**  
AWS S3 uses virtual-hosted style: `bucket.s3.amazonaws.com/key`  
MinIO uses path style: `minio.host/bucket/key`  
Without this, all MinIO requests 404.

---

## 6. SparkService — The Orchestrator

**File:** `engine/spark/SparkService.java`

**Single responsibility:** Control the flow of one job. Does NOT build DataFrames (that's `SparkQueryBuilder`). Does NOT write files (that's `QueryResultWriter`). Only coordinates.

### Constructor

```java
public SparkService(SparkSession spark,
                    JobStatusService jobStatusService,
                    StorageConfig storageConfig) {
    this.spark            = spark;
    this.jobStatusService = jobStatusService;
    this.queryBuilder     = new SparkQueryBuilder(spark);
    this.resultWriter     = new QueryResultWriter(spark, storageConfig);
    this.objectMapper     = new ObjectMapper().registerModule(new JavaTimeModule());
}
```

Creates its two workers (`SparkQueryBuilder` and `QueryResultWriter`) and an `ObjectMapper` for JSON parsing. Called once per worker startup.

---

### `query(message)` — The Main Method

This is the only public method. It runs one job end-to-end:

```java
public void query(QueryMessage message) {
    String jobId = message.getJobId();

    try {
        // Step 1: tell Redis this job has started
        jobStatusService.writeStatus(jobId, "PROCESSING", null);

        // Step 2: figure out the full Iceberg table name and project ID
        String icebergTable = resolveIcebergTable(message.getSource());
        String projectId    = extractProjectId(message.getSource());

        // Step 3: run the right type of job
        QueryResult result = isSchemaJob(jobId)
                ? executeSchemaJob(icebergTable)
                : executeQueryJob(message, icebergTable, projectId);

        // Step 4: tell Redis the job completed, with all result metadata
        jobStatusService.writeResult(jobId, "COMPLETED", ...).join();

    } catch (Exception e) {
        // If anything goes wrong, mark the job as FAILED in Redis
        jobStatusService.writeStatus(jobId, "FAILED", e.getMessage()).join();
        throw new RuntimeException(...);
    }
}
```

Notice `.join()` on `writeResult` and `writeStatus(FAILED)` — these are blocking calls because we must guarantee the final status is written to Redis before the RabbitMQ message is ACKed. The PROCESSING write at step 1 is fire-and-forget (no `.join()`).

---

### Private Methods of SparkService

#### `resolveIcebergTable(source)`

```java
// Input:  "myproject.sales"
// Output: "iceberg.myproject.sales"

// Input:  "iceberg.myproject.sales"  (already has prefix)
// Output: "iceberg.myproject.sales"  (unchanged)
```

The Iceberg catalog was registered in SparkEngine with the name `"iceberg"`. When you call `spark.read().load("iceberg.myproject.sales")`, Spark looks up the catalog named `iceberg` and finds the `myproject.sales` table inside it. This method ensures the prefix is always there.

---

#### `extractProjectId(source)`

```java
// Input:  "myproject.sales"
// Output: "myproject"

// Used to build the MinIO output path:
// "wh/myproject/queries/query_20260223_120000/result.parquet"
```

Takes everything before the first `.` in the source string.

---

#### `isSchemaJob(jobId)`

```java
return jobId != null && jobId.startsWith("schema-");
```

A simple prefix check. The API service sets the jobId to `"schema-{uuid}"` when requesting schema, and `"query-{uuid}"` for data queries.

---

#### `executeQueryJob(message, icebergTable, projectId)`

```java
private QueryResult executeQueryJob(QueryMessage message, String icebergTable, String projectId) {
    QueryRequest request = parseQueryRequest(message.getQueryJson());  // JSON → typed object
    Dataset<Row> result  = queryBuilder.build(icebergTable, request);  // build the DataFrame pipeline
    return resultWriter.write(result, projectId);                       // write Parquet, return metadata
}
```

Three lines. Each line delegates to a specialist.

---

#### `executeSchemaJob(icebergTable)`

```java
private QueryResult executeSchemaJob(String icebergTable) {
    StructField[] fields = spark.read()
            .format("iceberg")
            .load(icebergTable)
            .limit(0)          // ← KEY: reads zero data rows, only metadata
            .schema()
            .fields();

    // Convert each StructField to {"name": "id", "type": "int", "nullable": false}
    List<Map<String, Object>> schemaRows = Arrays.stream(fields)
            .map(f -> {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("name",     f.name());
                row.put("type",     f.dataType().simpleString());
                row.put("nullable", f.nullable());
                return row;
            })
            .collect(Collectors.toList());

    return QueryResult.builder()
            .resultPath(null)          // no Parquet file for schema jobs
            .rowCount(schemaRows.size())
            .fileSizeBytes(0L)
            .resultData(schemaRows)
            .build();
}
```

`limit(0)` is the trick here. Iceberg stores schema in the table metadata file — Spark can read it without scanning any S3 data files. This is instant even on a table with billions of rows.

---

#### `parseQueryRequest(queryJson)`

```java
return objectMapper.readValue(queryJson, QueryRequest.class);
```

The RabbitMQ message has a `queryJson` field that is a raw JSON string like:

```json
{
  "source": "myproject.sales",
  "select": [
    { "column": "region" },
    { "column": "amount", "aggregation": "sum", "as": "total" }
  ],
  "filters": [{ "column": "year", "operator": "=", "value": 2024 }],
  "groupBy": ["region"],
  "orderBy": [{ "column": "total", "direction": "desc" }]
}
```

Jackson deserializes this string into a typed `QueryRequest` Java object.

---

#### `buildCompletionMessage(result, icebergTable)`

Builds the human-readable message stored in Redis:

- Schema job: `"Schema retrieved: 5 columns from table iceberg.myproject.sales"`
- Data job: `"Query completed: 120 rows, result stored at wh/myproject/queries/query_20260223/result.parquet"`

---

#### `serializeResultData(resultData)`

```java
return objectMapper.writeValueAsString(resultData);
// List<Map<String,Object>> → "[{"region":"North","total":5000}, ...]"
```

Converts the in-memory result rows to a JSON string for storage in Redis. Redis stores strings, not Java objects.

---

## 7. SparkQueryBuilder — Building the DataFrame

**File:** `engine/spark/SparkQueryBuilder.java`

**Single responsibility:** Translate a `QueryRequest` into a Spark `Dataset<Row>` pipeline. Never writes to disk, never talks to Redis.

### What is a Dataset\<Row\>?

A `Dataset<Row>` is Spark's representation of a table-like result. Think of it like a lazy SQL query — it describes the transformation but hasn't executed it yet. Execution only happens when you call `.collect()` or `.write()`.

### `build(icebergTable, request)` — The Pipeline

```
Dataset<Row> df = spark.read().format("iceberg").load(icebergTable)
      │
      ▼  applyFilters()       →  WHERE conditions
      │
      ▼  applySelectAndGroupBy()  →  SELECT columns, GROUP BY, aggregations
      │
      ▼  applyOrderBy()       →  ORDER BY
      │
      ▼  df.limit()           →  LIMIT (only if caller specified one)
      │
      ▼  return df            (still lazy — not executed yet)
```

---

### `applyFilters(df, filters)` — WHERE Clause

Iterates over every `FilterCondition` and chains `.filter()` calls on the DataFrame:

```java
for (FilterCondition f : filters) {
    Column col = functions.col(f.getColumn());  // wraps column name in Spark Column object
    Object val = f.getValue();

    df = switch (f.getOperator()) {
        case "="           -> df.filter(col.equalTo(val));
        case "!="          -> df.filter(col.notEqual(val));
        case ">"           -> df.filter(col.gt(val));
        case ">="          -> df.filter(col.geq(val));
        case "<"           -> df.filter(col.lt(val));
        case "<="          -> df.filter(col.leq(val));
        case "LIKE"        -> df.filter(col.like(val.toString()));    // SQL LIKE pattern
        case "IN"          -> df.filter(col.isin(toObjectArray(val))); // value in list
        case "IS NULL"     -> df.filter(col.isNull());
        case "IS NOT NULL" -> df.filter(col.isNotNull());
        default -> df;  // unsupported operator — skip (logged as warning)
    };
}
```

Each `.filter()` call adds a WHERE condition to the lazy pipeline. Multiple filters are ANDed together.

**Example:**

```json
filters: [
  {"column": "year",   "operator": "=",  "value": 2024},
  {"column": "region", "operator": "IN", "value": ["North", "South"]}
]
```

Produces: `WHERE year = 2024 AND region IN ('North', 'South')`

---

### `applySelectAndGroupBy(df, request)` — SELECT + GROUP BY

This is the most complex method because it handles three different SQL patterns:

#### Case 1: GROUP BY + aggregation

```json
select: [
  {"column": "region"},
  {"column": "amount", "aggregation": "sum", "as": "total"}
],
groupBy: ["region"]
```

```java
df = df.groupBy(col("region"))
       .agg(functions.sum(col("amount")).alias("total"));
// SQL equivalent: SELECT region, SUM(amount) AS total FROM table GROUP BY region
```

How the code detects this: there are `aggCols` (columns with an `aggregation` field) AND `hasGroupBy` is true.

#### Case 2: Global aggregation (no GROUP BY)

```json
select: [
  {"column": "amount", "aggregation": "sum", "as": "total"},
  {"column": "id",     "aggregation": "count", "as": "count"}
]
```

```java
df = df.agg(
    functions.sum(col("amount")).alias("total"),
    functions.count(col("id")).alias("count")
);
// SQL equivalent: SELECT SUM(amount) AS total, COUNT(id) AS count FROM table
```

Returns exactly one row.

#### Case 3: Plain SELECT

```json
select: [{"column": "id"}, {"column": "name", "as": "fullName"}]
```

```java
df = df.select(
    col("id"),
    col("name").alias("fullName")
);
// SQL equivalent: SELECT id, name AS fullName FROM table
```

If select is `[{"column": "*"}]`, it's a no-op — all columns are returned unchanged.

---

### `buildAggExpressions(aggCols)` — Builds Aggregation Column Array

```java
private Column[] buildAggExpressions(List<SelectColumn> aggCols) {
    return aggCols.stream()
            .map(s -> {
                Column expr = applyAggregation(functions.col(s.getColumn()), s.getAggregation());
                return (s.getAs() != null) ? expr.alias(s.getAs()) : expr.alias(s.getColumn());
            })
            .toArray(Column[]::new);
}
```

For each aggregation column, calls `applyAggregation()` to wrap it in the right Spark function, then applies the alias. Returns an array because Spark's `.agg()` needs `(first, rest...)`.

---

### `applyAggregation(col, aggregation)` — Maps String → Spark Function

```java
return switch (aggregation.toLowerCase()) {
    case "sum"      -> functions.sum(col);
    case "avg"      -> functions.avg(col);
    case "count"    -> functions.count(col);
    case "min"      -> functions.min(col);
    case "max"      -> functions.max(col);
    case "stddev"   -> functions.stddev(col);
    case "variance" -> functions.variance(col);
    default         -> col;  // treat as plain column (logged as warning)
};
```

A pure string-to-function mapping. Nothing more.

---

### `applyOrderBy(df, orderBy)` — ORDER BY Clause

```java
Column[] cols = orderBy.stream()
        .map(o -> "desc".equalsIgnoreCase(o.getDirection())
                ? functions.col(o.getColumn()).desc()
                : functions.col(o.getColumn()).asc())
        .toArray(Column[]::new);
return df.orderBy(cols);
```

For each `OrderByClause`, wraps the column in `.asc()` or `.desc()` and passes all of them to `.orderBy()`.

**Example:**

```json
orderBy: [{"column": "total", "direction": "desc"}, {"column": "region", "direction": "asc"}]
```

Produces: `ORDER BY total DESC, region ASC`

---

### `toObjectArray(value)` — IN Operator Helper

```java
private Object[] toObjectArray(Object value) {
    if (value instanceof List<?> list) {
        return list.toArray();
    }
    return new Object[]{value};
}
```

When Jackson deserializes a JSON array like `["North", "South"]` in a filter value, it becomes a `List<Object>`. Spark's `.isin()` needs a plain `Object[]`. This converts between the two. If the value is already a scalar (e.g. `"North"` directly), it wraps it in a single-element array.

---

## 8. QueryResultWriter — Persisting the Result

**File:** `engine/spark/QueryResultWriter.java`

**Single responsibility:** Take a finished `Dataset<Row>`, write it to MinIO as Parquet, collect the rows into memory, and return metadata. Never builds DataFrames, never talks to Redis.

### `write(result, projectId)` — The Only Public Method

```java
public QueryResult write(Dataset<Row> result, String projectId) {
    // Step 1: Build output paths
    String timestamp    = LocalDateTime.now().format(TIMESTAMP_FMT);      // "20260223_120000"
    String relativePath = "wh/" + projectId + "/queries/query_" + timestamp + "/result.parquet";
    String s3aPath      = "s3a://" + storageConfig.getWarehouseBucket() + "/" + relativePath;

    // Step 2: Cache the Dataset
    result.cache();

    // Step 3: Collect all rows into memory (triggers Spark execution)
    List<Row> inlineRows = result.collectAsList();
    String[]  columns    = result.columns();
    long      rowCount   = inlineRows.size();

    // Step 4: Write Parquet to MinIO
    result.coalesce(1)
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(s3aPath);

    // Step 5: Release cache
    result.unpersist();

    // Step 6: Measure the written file size
    long fileSizeBytes = resolveFileSize(s3aPath);

    // Step 7: Build and return the metadata object
    return QueryResult.builder()
            .resultPath(relativePath)
            .rowCount(rowCount)
            .fileSizeBytes(fileSizeBytes)
            .resultData(toRowMaps(inlineRows, columns))
            .build();
}
```

#### Why `result.cache()`?

Without caching, calling both `collectAsList()` and `write()` would trigger **two separate Spark jobs** — each one re-reading from Iceberg, re-applying all filters, re-doing all aggregations. With `cache()`, the first action (collect) materializes the result into memory, and the second action (write) reuses that memory instead of recomputing from scratch.

#### Why `coalesce(1)`?

`coalesce(1)` merges all Spark partitions into one before writing. This means the output is a single Parquet file instead of 200 tiny files. For a query result (which is bounded and relatively small), one file is much easier for the API service to reference and download.

#### `SaveMode.Overwrite`

If the same path exists (e.g. two jobs with the same timestamp by coincidence), overwrite it. This avoids a `FileAlreadyExistsException`.

---

### `toRowMaps(rows, columns)` — Rows → JSON-Serializable Maps

```java
return rows.stream()
        .map(row -> {
            Map<String, Object> map = new LinkedHashMap<>();  // preserves column order
            for (int i = 0; i < columns.length; i++) {
                map.put(columns[i], row.get(i));  // "region" → "North"
            }
            return map;
        })
        .collect(Collectors.toList());
```

A Spark `Row` is positional (`row.get(0)`, `row.get(1)`). This converts it to a named map (`{"region": "North", "total": 5000}`) that Jackson can serialize to JSON for Redis storage.

`LinkedHashMap` is used instead of `HashMap` to preserve column order (so `{"id": 1, "name": "Alice"}` never becomes `{"name": "Alice", "id": 1}`).

---

### `resolveFileSize(s3aPath)` — File Size via Hadoop FS

```java
private long resolveFileSize(String s3aPath) {
    try {
        Path         hadoopPath = new Path(s3aPath);
        FileSystem   fs         = hadoopPath.getFileSystem(
                spark.sparkContext().hadoopConfiguration());  // uses the same S3A config
        ContentSummary summary  = fs.getContentSummary(hadoopPath);
        return summary.getLength();  // total bytes in the directory
    } catch (Exception e) {
        log.warn("Could not resolve file size: {}", e.getMessage());
        return 0L;  // non-fatal — job still succeeds
    }
}
```

After writing the Parquet file, uses the Hadoop `FileSystem` API (which already has the MinIO credentials from the Spark config) to stat the directory and get the total byte count. Returns `0` on failure — a missing file size is not critical enough to fail the whole job.

---

## 9. QueryEngineFactory — The Entry Point

**File:** `engine/QueryEngineFactory.java`

```java
public static QueryEngine create(String icebergCatalogUri, String icebergWarehouse) {
    String type = System.getenv().getOrDefault("QUERY_ENGINE", "spark").toLowerCase();

    return switch (type) {
        case "spark" -> new SparkEngine(icebergCatalogUri, icebergWarehouse);
        default -> {
            log.warn("Unknown QUERY_ENGINE '{}', falling back to spark", type);
            yield new SparkEngine(icebergCatalogUri, icebergWarehouse);
        }
    };
}
```

Reads the `QUERY_ENGINE` environment variable. If it's `"spark"` (or unset), returns a `SparkEngine`. In the future, `"duckdb"` could return a `DuckDBEngine` without any other change.

The private constructor `private QueryEngineFactory() {}` prevents anyone from accidentally creating an instance — this class is a namespace for a static factory method, not a real object.

---

## 10. Data Flow: A Real Example End-to-End

### Input Message (arrives from RabbitMQ)

```json
{
  "jobId": "query-abc123",
  "source": "myproject.orders",
  "queryJson": "{\"source\":\"myproject.orders\",\"select\":[{\"column\":\"region\"},{\"column\":\"amount\",\"aggregation\":\"sum\",\"as\":\"total\"}],\"filters\":[{\"column\":\"year\",\"operator\":\"=\",\"value\":2024}],\"groupBy\":[\"region\"],\"orderBy\":[{\"column\":\"total\",\"direction\":\"desc\"}]}"
}
```

### Step-by-Step Trace

```
1. RabbitMQConsumer.deliverCallback()
   → queryService.query(message)

2. SparkService.query(message)
   → Redis HSET job:query-abc123 status=PROCESSING

3. resolveIcebergTable("myproject.orders")
   → "iceberg.myproject.orders"

4. extractProjectId("myproject.orders")
   → "myproject"

5. isSchemaJob("query-abc123")  → false  → executeQueryJob()

6. parseQueryRequest(queryJson)
   → QueryRequest {
       select: [{column:"region"}, {column:"amount", aggregation:"sum", as:"total"}],
       filters: [{column:"year", operator:"=", value:2024}],
       groupBy: ["region"],
       orderBy: [{column:"total", direction:"desc"}]
     }

7. SparkQueryBuilder.build("iceberg.myproject.orders", request)

   7a. spark.read().format("iceberg").load("iceberg.myproject.orders")
       → full table Dataset (lazy)

   7b. applyFilters(): df.filter(col("year").equalTo(2024))
       → filtered Dataset (still lazy)

   7c. applySelectAndGroupBy(): hasAgg=true, hasGroupBy=true
       → df.groupBy(col("region")).agg(sum(col("amount")).alias("total"))
       → aggregated Dataset (still lazy)

   7d. applyOrderBy(): df.orderBy(col("total").desc())
       → ordered Dataset (still lazy)

   7e. no limit set → return df as-is

8. QueryResultWriter.write(df, "myproject")

   8a. relativePath = "wh/myproject/queries/query_20260223_120000/result.parquet"
   8b. s3aPath      = "s3a://warehouse/wh/myproject/queries/query_20260223_120000/result.parquet"

   8c. result.cache()

   8d. result.collectAsList()  ← SPARK EXECUTES HERE: reads Iceberg, filters, aggregates, sorts
       → [Row("North", 50000), Row("South", 30000), Row("East", 20000)]

   8e. result.coalesce(1).write().parquet(s3aPath)
       → writes the 3 rows as a single Parquet file to MinIO (reuses cache)

   8f. result.unpersist()

   8g. resolveFileSize(s3aPath) → 1842 bytes

   8h. toRowMaps(rows, columns)
       → [{"region":"North","total":50000}, {"region":"South","total":30000}, {"region":"East","total":20000}]

   8i. return QueryResult {
         resultPath: "wh/myproject/queries/query_20260223_120000/result.parquet",
         rowCount: 3,
         fileSizeBytes: 1842,
         resultData: [...]
       }

9. SparkService serializes resultData → JSON string
   jobStatusService.writeResult(
     "query-abc123", "COMPLETED",
     "Query completed: 3 rows, result stored at wh/myproject/...",
     "wh/myproject/queries/...",
     3, 1842,
     "[{\"region\":\"North\",\"total\":50000},...]"
   ).join()

10. Redis now contains:
    job:query-abc123 → {
      status:        "COMPLETED",
      message:       "Query completed: 3 rows, result stored at wh/...",
      updatedAt:     "2026-02-23T12:00:01Z",
      resultPath:    "wh/myproject/queries/query_20260223_120000/result.parquet",
      rowCount:      "3",
      fileSizeBytes: "1842",
      resultData:    "[{\"region\":\"North\",\"total\":50000},...]"
    }

11. RabbitMQConsumer sends channel.basicAck() → message removed from queue
```

---

## 11. Design Principles Used

### Single Responsibility Principle (SRP)

Each class does exactly one thing:

| Class                    | Responsibility                                               |
| ------------------------ | ------------------------------------------------------------ |
| `SparkEngine`            | Boot and shut down Spark                                     |
| `SparkStorageConfigurer` | Map storage credentials to Spark config keys                 |
| `SparkService`           | Orchestrate one job (detect type, delegate, report status)   |
| `SparkQueryBuilder`      | Translate `QueryRequest` → `Dataset<Row>` pipeline           |
| `QueryResultWriter`      | Persist `Dataset<Row>` → MinIO Parquet + collect inline rows |

If you need to change how filters work → only `SparkQueryBuilder` changes.  
If you need to change how results are stored → only `QueryResultWriter` changes.  
If you need to change the job flow → only `SparkService` changes.

### Open/Closed Principle (OCP)

`QueryEngine` and `QueryService` are interfaces. You can add `DuckDBEngine` and `DuckDBService` without modifying any existing class. `QueryEngineFactory` gets one new `case`, everything else is untouched.

### Lazy Execution (Spark-specific)

Spark DataFrames are lazy. Every `.filter()`, `.groupBy()`, `.orderBy()` call just adds a node to a logical plan. No actual data is read from S3 until you call a terminal action (`.collect()`, `.write()`, `.count()`). This means `SparkQueryBuilder` is essentially free — it just builds a description of what to do.

### Caching Strategy

`result.cache()` in `QueryResultWriter` ensures the expensive Spark execution (reading S3 → filtering → aggregating) happens only once even though we do two terminal actions (collect + write).

---

## 12. Vocabulary Glossary

| Term                      | Meaning                                                                                                                    |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| **Dataset\<Row\>**        | Spark's table-like object. Lazy — describes a query but doesn't run it until a terminal action.                            |
| **Terminal action**       | A method that forces Spark to actually execute (`.collect()`, `.write()`, `.count()`).                                     |
| **Iceberg**               | A table format for data lakes. Stores schema, partitioning, and file locations in metadata files separate from data files. |
| **REST catalog**          | A server that tracks Iceberg table metadata. Spark connects to it to find where table files are in S3.                     |
| **S3FileIO**              | Iceberg's implementation for reading/writing table data through S3 API.                                                    |
| **S3AFileSystem**         | Hadoop's implementation for reading/writing raw files through S3 API. Both S3FileIO and S3A are needed.                    |
| **Path-style access**     | `host/bucket/key` URL format — required for MinIO. Opposite: virtual-hosted `bucket.host/key` (AWS default).               |
| **coalesce(1)**           | Merge all Spark partitions into one. Makes the output a single file.                                                       |
| **cache() / unpersist()** | Store materialized partition data in memory. `unpersist()` releases it after you're done.                                  |
| **StructField**           | Iceberg/Spark's representation of one column's schema: name, type, nullable.                                               |
| **LinkedHashMap**         | A Java Map that preserves insertion order. Used so JSON column order is deterministic.                                     |
| **fire-and-forget**       | Calling an async method without `.join()` — the caller doesn't wait for the result.                                        |
| **SRP**                   | Single Responsibility Principle — each class has one reason to change.                                                     |
| **OCP**                   | Open/Closed Principle — open for extension, closed for modification.                                                       |
