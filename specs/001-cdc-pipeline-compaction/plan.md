# Implementation Plan: CDC Pipeline & Iceberg Compaction

**Branch**: `001-cdc-pipeline-compaction` | **Date**: 2026-04-04 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-cdc-pipeline-compaction/spec.md`

## Summary

Add a Change Data Capture (CDC) pipeline to the Modern Data Lakehouse platform. A new source PostgreSQL database (with logical replication) feeds row-level changes through Debezium (Kafka Connect) into Kafka, where a new **cdc-worker** (Spark Structured Streaming) consumes CDC events, deduplicates by primary key, and applies UPSERT/DELETE operations via Iceberg `MERGE INTO` (format-version 2, merge-on-read). Additionally, implement Iceberg table maintenance: a **compaction** job using `RewriteDataFiles` to consolidate small files, and **snapshot tagging** via `CREATE TAG` to protect ML-training snapshots from `ExpireSnapshots`.

## Technical Context

**Language/Version**: Java 21 (matching existing workers)
**Primary Dependencies**: Apache Spark 3.5, Apache Iceberg (REST Catalog, format-version 2), Apache Kafka (KRaft), Debezium 2.5, Lettuce (Redis client)
**Storage**: MinIO (S3-compatible, Iceberg warehouse), PostgreSQL (source DB + catalog DB), Redis (job status, Pub/Sub)
**Testing**: Docker Compose integration tests, manual end-to-end verification via API
**Target Platform**: Linux containers (Docker Compose)
**Project Type**: Distributed microservices (Spark workers)
**Performance Goals**: CDC events visible in Iceberg within 30 seconds; compaction reduces file count by 50%+
**Constraints**: Must not disrupt existing ingestion/query workers; new code must follow SOLID principles with separated read/transform/write concerns
**Scale/Scope**: Single-node local development (1 Kafka broker, 1 Debezium connector, 1 cdc-worker)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Constitution is unfilled (template placeholders only). No gates defined — proceeding with standard engineering best practices:

- ✅ **SOLID principles**: Enforced by FR-021/FR-022 — cdc-worker has 7+ single-responsibility classes
- ✅ **Existing patterns**: Factory, strategy, repository patterns reused from ingestion-worker/query-worker
- ✅ **No monolithic scripts**: Each concern (reading, parsing, deduplicating, merging, maintenance) is a separate class
- ✅ **Graceful failure handling**: Kafka auto-reconnect, Spark checkpointing, NACK retry from existing patterns

## Project Structure

### Documentation (this feature)

```text
specs/001-cdc-pipeline-compaction/
├── spec.md              # Feature specification
├── plan.md              # This file
├── research.md          # Phase 0: technology decisions
├── data-model.md        # Phase 1: entity definitions
├── quickstart.md        # Phase 1: getting started guide
├── contracts/
│   ├── debezium-connector.md   # Debezium REST API contract
│   └── cdc-event-format.md     # Kafka CDC event schema
└── tasks.md             # Phase 2: implementation tasks (via /speckit.tasks)
```

### Source Code (repository root)

```text
# New service: cdc-worker
cdc-worker/
├── pom.xml                                      # Maven build (mirrors ingestion-worker)
├── Dockerfile                                   # Multi-stage Spark image
└── src/main/java/com/abdelwahab/cdc_worker/
    ├── CdcWorkerMain.java                       # Entry point — wires factories, starts streaming
    ├── config/
    │   └── CdcConfig.java                       # Centralized config from environment variables
    ├── consumer/
    │   ├── StreamReader.java                    # Interface
    │   └── kafka/
    │       └── KafkaStreamReader.java           # Reads from Kafka topic, returns streaming DataFrame
    ├── engine/
    │   ├── CdcEngine.java                       # Interface (lifecycle: init/start/stop)
    │   ├── CdcEngineFactory.java                # Factory: CDC_ENGINE=spark → SparkCdcEngine
    │   └── spark/
    │       ├── SparkCdcEngine.java              # SparkSession lifecycle + warm-up
    │       ├── CdcStreamProcessor.java          # Orchestrator: read → transform → write loop
    │       ├── DebeziumEventParser.java          # Extracts op/before/after from Debezium envelope
    │       ├── EventDeduplicator.java            # Deduplicates by PK per micro-batch
    │       └── IcebergMergeWriter.java           # foreachBatch + MERGE INTO
    ├── maintenance/
    │   ├── CompactionService.java               # SparkActions.rewriteDataFiles wrapper
    │   └── SnapshotManager.java                 # CREATE TAG + ExpireSnapshots
    ├── status/
    │   └── redis/
    │       └── AsyncRedisJobStatusService.java  # Copy/adapt from ingestion-worker
    └── storage/
        └── StorageConfigurer.java               # S3/MinIO config for SparkSession

# New infrastructure files
source-db/
├── init.sql                                     # DDL + seed data for source-postgres
└── postgresql.conf                              # (optional, may use command args instead)

# Modified files
docker-compose.yml                               # Add: source-postgres, kafka, debezium-connect, cdc-worker
.env                                             # Add: new environment variables
```

**Structure Decision**: The `cdc-worker` follows the same standalone Java application pattern as `ingestion-worker` and `query-worker`. No Spring Boot — just a `main()` method that wires factories and starts the streaming query. The `maintenance/` package groups compaction and snapshot management, which are batch operations (not streaming) but share the Spark session from the engine.

## Complexity Tracking

> No constitution violations — no entries needed.

| Component | Decision | Rationale |
|-----------|----------|-----------|
| Adding Kafka | Required for CDC transport | RabbitMQ (existing) lacks log-based offset replay semantics. Kafka is the standard Debezium transport. Using KRaft mode (no Zookeeper) minimizes overhead. |
| Adding source-postgres | Separate from catalog-postgres | Different purpose (app data vs. Iceberg metadata). Sharing would create coupling and risk disrupting the catalog. |
| 7 classes in cdc-worker/engine/spark/ | SOLID compliance | Each class has one responsibility. This is more classes than ingestion-worker (which has 2) but justified by the higher complexity of CDC (parse → dedup → merge vs. simple read → write). |
