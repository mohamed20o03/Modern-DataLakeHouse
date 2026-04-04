# Feature Specification: CDC Pipeline & Iceberg Compaction

**Feature Branch**: `001-cdc-pipeline-compaction`  
**Created**: 2026-04-04  
**Status**: Draft  
**Input**: User description: "Add CDC pipeline with Debezium and Spark Structured Streaming for Postgres-to-Iceberg sync, plus Iceberg compaction and snapshot tagging"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Real-Time Database-to-Lakehouse Sync (Priority: P1)

As a data engineer, I want changes made to application database tables (inserts, updates, deletes) to be automatically and continuously captured and reflected in the lakehouse so that analysts always query near-real-time data without manual ETL jobs.

**Why this priority**: This is the core value proposition — without reliable change data capture, the lakehouse holds only batch-loaded snapshots and cannot serve as a live analytical mirror of the operational database.

**Independent Test**: Can be fully tested by inserting, updating, and deleting rows in the source database table and then querying the target Iceberg table to confirm that every change is reflected accurately, including deletes. Delivers continuous, automated data synchronization.

**Acceptance Scenarios**:

1. **Given** a row is inserted into the source database table, **When** the CDC pipeline processes the event, **Then** the same row appears in the corresponding Iceberg table within the configured processing interval.
2. **Given** an existing row in the source database is updated, **When** the CDC pipeline processes the update event, **Then** the matching row in the Iceberg table reflects the new values (upsert behavior).
3. **Given** an existing row in the source database is deleted, **When** the CDC pipeline processes the delete event, **Then** the corresponding row is removed from the Iceberg table.
4. **Given** the CDC pipeline receives duplicate events for the same row (e.g., due to upstream retries), **When** it processes them, **Then** the Iceberg table contains only one copy of the row with the latest values — no duplicates are introduced.
5. **Given** the CDC pipeline is restarted after a failure, **When** it reconnects, **Then** it resumes from the last committed offset without losing or re-applying events that were already committed.

---

### User Story 2 - Source Database Bootstrapping (Priority: P1)

As a developer deploying the platform, I want the system to automatically provision a source database with sample data and the correct replication configuration so that the CDC pipeline works out of the box with zero manual database setup.

**Why this priority**: The CDC pipeline (Story 1) cannot function without a properly configured source database. This is a hard prerequisite.

**Independent Test**: Can be fully tested by starting the platform from a clean state and verifying the source database container is healthy, the sample table exists with seed data, and the replication configuration is active.

**Acceptance Scenarios**:

1. **Given** the platform is started for the first time, **When** all services finish initialization, **Then** the source database contains the sample schema and seed data without any manual intervention.
2. **Given** the source database is configured for change capture, **When** a row is modified, **Then** the modification is available to the CDC connector for capture.
3. **Given** the platform is stopped and restarted, **When** services come back up, **Then** the source database retains its data (persistent storage) and is ready for change capture.

---

### User Story 3 - Iceberg Table Compaction (Priority: P2)

As a data engineer, I want an automated mechanism to compact small data files in Iceberg tables into fewer, larger files so that query performance does not degrade over time as the CDC pipeline writes many small commits.

**Why this priority**: The CDC pipeline writes frequent micro-batches, each producing small Parquet files. Without compaction, downstream query performance degrades significantly. However, this can be added after the pipeline is working.

**Independent Test**: Can be fully tested by running the compaction process on an Iceberg table that has many small files, then verifying the file count is reduced and query performance is maintained or improved.

**Acceptance Scenarios**:

1. **Given** an Iceberg table has accumulated many small data files from CDC micro-batches, **When** the compaction process runs, **Then** the data files are rewritten into fewer, larger files at the configured target size.
2. **Given** the compaction process completes successfully, **When** a query is run against the compacted table, **Then** the query returns identical results as before compaction (data integrity is preserved).
3. **Given** the compaction process is triggered, **When** it completes, **Then** it reports how many files were rewritten and the resulting file statistics.

---

### User Story 4 - Snapshot Tagging for ML Training (Priority: P3)

As a data scientist, I want to tag specific Iceberg table snapshots as "ML training" datasets so that those snapshots are preserved even when old snapshots are expired for storage reclamation — enabling reproducible model training.

**Why this priority**: Snapshot tagging adds long-term reproducibility for ML workflows. It is valuable but depends on having data in the Iceberg tables first (Stories 1–3).

**Independent Test**: Can be fully tested by creating a tag on a snapshot, running the snapshot expiration process, and confirming the tagged snapshot still exists while untagged old snapshots are removed.

**Acceptance Scenarios**:

1. **Given** an Iceberg table with multiple snapshots, **When** a user tags a specific snapshot with a descriptive label (e.g., "training-2026-Q1"), **Then** the tag is recorded and the snapshot can be referenced by that label.
2. **Given** a tagged snapshot and several untagged old snapshots exist, **When** the snapshot expiration process runs, **Then** untagged snapshots older than the retention threshold are removed, but the tagged snapshot is preserved.
3. **Given** a tagged snapshot exists, **When** a user queries the table using the tag reference, **Then** the query returns data as it existed at the tagged snapshot.

---

### Edge Cases

- What happens when the source database is temporarily unreachable? The CDC connector must detect the outage and automatically retry/reconnect without data loss once the database is back.
- What happens when the CDC pipeline writes a batch but the Iceberg commit fails? The pipeline must not advance the offset, ensuring the batch is retried on the next cycle.
- What happens when the compaction process runs concurrently with active CDC writes? The system must handle concurrent operations safely — compaction must not corrupt in-flight writes.
- What happens when a user attempts to tag a snapshot that has already been expired? The system must return a clear error indicating the snapshot no longer exists.
- What happens when the source database schema changes (e.g., a column is added)? The pipeline should handle schema evolution gracefully — either propagating the change or logging a clear warning.
- What happens when a duplicate event has a different timestamp but the same primary key? The deduplication logic must use a deterministic tiebreaker (e.g., event timestamp/offset) to keep only the latest version.

## Requirements *(mandatory)*

### Functional Requirements

**Source Database Setup**

- **FR-001**: System MUST provision a source relational database container automatically as part of the platform's infrastructure startup.
- **FR-002**: System MUST execute an initialization script at first launch to create a sample database schema with at least one table containing representative columns (e.g., ID, name, email, timestamp).
- **FR-003**: System MUST seed the sample table with initial rows so the CDC pipeline can capture the existing state on first run.
- **FR-004**: System MUST configure the source database for logical replication so that row-level changes are available for capture by the CDC connector.
- **FR-005**: System MUST persist source database data across container restarts using a named volume.

**CDC Pipeline**

- **FR-006**: System MUST deploy a change data capture connector that monitors the source database and emits events for every INSERT, UPDATE, and DELETE on configured tables.
- **FR-007**: System MUST include a streaming application (cdc-worker) that continuously consumes CDC events and writes them to Iceberg tables in the lakehouse warehouse.
- **FR-008**: The cdc-worker MUST deduplicate events by primary key within each processing window to prevent duplicate rows from upstream retries or at-least-once delivery.
- **FR-009**: The cdc-worker MUST apply UPSERT semantics for insert and update events — inserting new rows and updating existing rows by matching on primary key.
- **FR-010**: The cdc-worker MUST apply DELETE semantics for delete events — removing the corresponding row from the target Iceberg table by primary key.
- **FR-011**: The cdc-worker MUST use Iceberg format-version 2 for the target table to support row-level deletes (equality deletes / merge-on-read).
- **FR-012**: The cdc-worker MUST maintain processing offsets/checkpoints so that pipeline restarts resume from the last committed position without data loss or duplication.
- **FR-013**: The cdc-worker MUST report its job status (RUNNING, FAILED) to the platform's job tracking system for observability.

**Compaction**

- **FR-014**: System MUST provide a compaction job that rewrites small data files in a target Iceberg table into fewer, larger files at a configurable target file size.
- **FR-015**: The compaction job MUST preserve data integrity — the table must contain exactly the same rows before and after compaction.
- **FR-016**: The compaction job MUST be safe to run concurrently with active Iceberg writes (i.e., it must use Iceberg's conflict resolution, not lock writers out).

**Snapshot Tagging & Expiration**

- **FR-017**: System MUST allow users to create named tags on specific Iceberg table snapshots (e.g., "training-2026-Q1").
- **FR-018**: System MUST provide a snapshot expiration process that removes snapshots older than a configurable retention period.
- **FR-019**: The snapshot expiration process MUST NOT remove snapshots that have been tagged — tagged snapshots survive expiration.
- **FR-020**: System MUST allow querying an Iceberg table at a tagged snapshot (time-travel by tag).

**Code Quality**

- **FR-021**: All new application code MUST follow single-responsibility principle — reading, transforming, and writing concerns must be separated into distinct components.
- **FR-022**: All new worker code MUST follow the platform's existing factory and strategy patterns for engine selection, storage configuration, and message consumption.

### Key Entities

- **Source Database Table**: The upstream relational table whose changes are captured. Key attributes: primary key (ID), data columns, row timestamps.
- **CDC Event**: A change event produced by the CDC connector. Attributes: operation type (INSERT/UPDATE/DELETE), before-image, after-image, source metadata (timestamp, offset, transaction ID).
- **Target Iceberg Table**: The lakehouse destination table. Must use format-version 2 for row-level operations. Identified by `iceberg.{namespace}.{tableName}`.
- **Snapshot Tag**: A named reference to a specific Iceberg table snapshot. Attributes: tag name, snapshot ID, creation timestamp.
- **Compaction Job**: A maintenance operation that rewrites small files. Attributes: target table, target file size, files rewritten count, job status.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Changes made in the source database are visible in the lakehouse Iceberg table within 30 seconds under normal operating conditions.
- **SC-002**: The CDC pipeline correctly handles all three operation types (INSERT, UPDATE, DELETE) — verified by applying a mixed workload of 100+ operations and confirming 100% accuracy in the target table.
- **SC-003**: No duplicate rows exist in the target Iceberg table after processing events that include upstream retries (at-least-once delivery from the CDC connector).
- **SC-004**: After pipeline restart (simulating failure recovery), all changes made during downtime are captured and applied — zero data loss.
- **SC-005**: After compaction, the number of data files in the target table is reduced by at least 50% (when starting from a table with many small files), and all rows remain intact.
- **SC-006**: Tagged snapshots survive the snapshot expiration process — verified by expiring old snapshots and confirming the tagged snapshot is still queryable.
- **SC-007**: The platform starts from a clean state (`docker compose up`) with zero manual database configuration steps — the source database is ready for change capture automatically.
- **SC-008**: All new worker code separates reading, transforming, and writing into distinct classes — no single class handles more than one of these concerns.

## Assumptions

- The source database for CDC is a new, dedicated container separate from the existing Iceberg catalog PostgreSQL (catalog-postgres). They serve different purposes and must not share a container.
- The CDC connector (Debezium) will publish events to a message transport (e.g., Kafka or a Kafka-compatible interface). If Kafka is not already in the stack, a lightweight Kafka broker will be added to the infrastructure.
- The cdc-worker is a new, standalone service alongside the existing ingestion-worker and query-worker — it does not replace or modify existing workers.
- Compaction and snapshot tagging are operational maintenance tasks that can be triggered manually or on a schedule — they do not need to be real-time.
- The sample schema is illustrative (e.g., a "customers" or "orders" table); the exact columns are not business-critical as long as they demonstrate the full CDC flow (INSERT/UPDATE/DELETE).
- The Iceberg REST catalog and MinIO storage already in the platform will be reused by the cdc-worker and compaction job — no new storage infrastructure is needed beyond the source database and CDC connector.
- Compaction target file size will default to the platform's existing 128 MiB standard unless otherwise configured.
- Snapshot retention period for expiration defaults to a reasonable development value (e.g., 7 days) and is configurable.
