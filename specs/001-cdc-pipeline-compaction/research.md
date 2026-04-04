# Research: CDC Pipeline & Iceberg Compaction

**Feature**: 001-cdc-pipeline-compaction  
**Date**: 2026-04-04

## R1: CDC Transport Layer — Kafka vs Alternatives

**Decision**: Use Apache Kafka (KRaft mode, no Zookeeper) as the CDC event transport between Debezium and Spark Structured Streaming.

**Rationale**:
- Debezium's primary and most battle-tested integration is Kafka Connect. Using Kafka provides a persistent, partitioned, replayable log with built-in offset management.
- Spark Structured Streaming has a first-class Kafka source connector with exactly-once semantics via checkpointing.
- Debezium Server (the Kafka-less alternative) requires custom offset management and lacks the built-in durability guarantees of Kafka.
- KRaft mode eliminates the need for Zookeeper, reducing infrastructure from 3 containers to 1 for the broker.

**Alternatives considered**:
- **Debezium Server → Redis Streams**: Would reuse existing Redis, but Redis Streams have no native Spark connector, limited retention, and no partitioning — unsuitable for reliable CDC.
- **Debezium Server → MinIO (file sink)**: File-based approach adds latency (batch-oriented) and loses real-time streaming capability.
- **RabbitMQ (existing)**: RabbitMQ doesn't support the log-based semantics (offset replay, partitioned ordering) that CDC requires.

## R2: Debezium Deployment Model — Kafka Connect vs Debezium Server

**Decision**: Deploy Debezium as a Kafka Connect connector running inside a Kafka Connect worker container (`debezium/connect`).

**Rationale**:
- Kafka Connect manages connector lifecycle, offset tracking, and fault tolerance automatically.
- Debezium's Docker image (`debezium/connect`) bundles all necessary connector JARs.
- REST API for connector registration makes it easy to configure programmatically via an init container.

**Alternatives considered**:
- **Debezium Server**: Standalone Quarkus app that sinks to various targets. However, since we're already adding Kafka (R1), using Kafka Connect is the natural fit.
- **Embedded Debezium Engine**: Embedding the CDC engine directly in the cdc-worker JVM. Too tightly coupled and loses the operational benefits of Kafka Connect's management plane.

## R3: PostgreSQL Logical Replication Configuration

**Decision**: Configure the source PostgreSQL with `wal_level=logical`, `max_replication_slots=4`, `max_wal_senders=4`, and use the native `pgoutput` logical decoding plugin.

**Rationale**:
- `wal_level=logical` is mandatory for Debezium CDC — `minimal` and `replica` levels do not capture row-level changes.
- `pgoutput` is PostgreSQL's built-in logical decoding plugin (Postgres 10+), requiring no extra extensions.
- 4 replication slots/senders is sufficient for a single Debezium connector with headroom for monitoring.
- Configured via `command:` args in docker-compose to avoid needing custom `postgresql.conf`.

## R4: Spark Structured Streaming → Iceberg MERGE INTO Pattern

**Decision**: Use `foreachBatch` with Spark SQL `MERGE INTO` to apply CDC events (upserts + deletes) to Iceberg tables using format-version 2 with merge-on-read (MoR).

**Rationale**:
- `MERGE INTO` is the standard Iceberg mechanism for row-level upserts and deletes.
- `foreachBatch` is required because `MERGE INTO` is a batch SQL operation — it cannot run inside a continuous streaming sink.
- Format-version 2 enables row-level deletes (equality delete files) which are essential for CDC delete events.
- Merge-on-Read (MoR) is preferred for high-frequency CDC because it writes small delete files instead of rewriting entire data files on every merge — much faster for streaming writes.
- Broadcast hint on the micro-batch (small) joined against the target table (large) optimizes the merge join.

**Alternatives considered**:
- **Copy-on-Write (CoW)**: Rewrites data files on every merge. Better read performance but too expensive for frequent streaming CDC micro-batches.
- **Append-only Bronze → MERGE Silver**: Two-stage pattern where raw events land in a Bronze table, then a separate job merges into Silver. Adds reliability/replay but doubles infrastructure. Deferred for v2.

## R5: CDC Event Deduplication Strategy

**Decision**: Within each `foreachBatch` micro-batch, deduplicate by primary key keeping the record with the highest Kafka offset (or latest `ts_ms` from Debezium).

**Rationale**:
- Kafka provides at-least-once delivery — duplicate events are possible on consumer rebalance or connector restart.
- Within a micro-batch, multiple events for the same primary key (rapid updates) should collapse to only the latest state.
- Using Kafka offset as the tiebreaker is deterministic and ordered.
- After deduplication, `MERGE INTO` handles the upsert logic naturally — each key appears exactly once in the source side.

## R6: Iceberg Compaction — RewriteDataFiles

**Decision**: Use `SparkActions.get(spark).rewriteDataFiles(table)` with the bin-pack strategy and a target file size of 128 MiB.

**Rationale**:
- Bin-pack is the fastest compaction strategy — it merges small files without re-sorting data.
- 128 MiB matches the platform's existing `TARGET_FILE_SIZE_BYTES` in the ingestion-worker for consistency.
- Iceberg's compaction is conflict-safe — it creates a new snapshot atomically, so concurrent CDC writes are not blocked.
- The compaction job should be a standalone operation (not embedded in cdc-worker) for separation of concerns.

**Alternatives considered**:
- **Sort strategy**: Re-sorts data during compaction for better query pruning. More expensive and not needed for the current workload.
- **Embedded in cdc-worker**: Running compaction inside the streaming job adds complexity and resource contention. Better as a separate, scheduled operation.

## R7: Snapshot Tagging and Expiration

**Decision**: Use Spark SQL `ALTER TABLE ... CREATE TAG` for tagging and `SparkActions.get(spark).expireSnapshots(table)` for expiration. Tags protect snapshots from expiration automatically.

**Rationale**:
- Iceberg's `CREATE TAG` creates a named reference to a snapshot ID. `ExpireSnapshots` explicitly skips tagged snapshots.
- Tags can optionally have a `RETAIN N DAYS` clause for auto-cleanup of old tags.
- Both operations are available via Spark SQL (tags) and the Iceberg Actions API (expiration), which are already dependencies in this project.

## R8: New Infrastructure Components Required

**Decision**: Add 3 new containers to docker-compose.yml: `source-postgres`, `kafka`, `debezium-connect`. Plus 1 new application service: `cdc-worker`.

**Rationale**:
- `source-postgres`: Separate from `catalog-postgres` (different purpose — application data vs. Iceberg catalog metadata).
- `kafka` (KRaft mode): Single-node broker, no Zookeeper required. Lightweight for dev/local.
- `debezium-connect`: Kafka Connect worker with Debezium PostgreSQL connector.
- `cdc-worker`: New Spark Structured Streaming application following the existing worker pattern (standalone Java, factory pattern, shutdown hooks).

## R9: cdc-worker Code Architecture (SOLID)

**Decision**: Structure the cdc-worker with the same patterns as existing workers: factory-driven engine/consumer creation, and separate classes for stream reading, event transformation, and Iceberg writing.

**Architecture**:
```
cdc-worker/
├── CdcWorkerMain.java              # Entry point — wires factories
├── consumer/
│   └── KafkaStreamReader.java      # Reads from Kafka, returns streaming DataFrame
├── engine/
│   ├── CdcEngine.java              # Interface (like IngestionEngine/QueryEngine)
│   ├── CdcEngineFactory.java       # Factory based on CDC_ENGINE env var
│   └── spark/
│       ├── SparkCdcEngine.java     # SparkSession lifecycle + warm-up
│       ├── CdcStreamProcessor.java # Orchestrator: read → transform → write
│       ├── DebeziumEventParser.java# Parses Debezium envelope, extracts op/before/after
│       ├── EventDeduplicator.java  # Deduplicates by PK within micro-batch
│       └── IcebergMergeWriter.java # Executes MERGE INTO via foreachBatch
├── maintenance/
│   ├── CompactionService.java      # RewriteDataFiles wrapper
│   └── SnapshotManager.java        # CREATE TAG + ExpireSnapshots
├── storage/                        # Reuse existing StorageConfig pattern
└── status/                         # Reuse existing JobStatusService pattern
```

Each class has a single responsibility. No monolithic Spark scripts.
