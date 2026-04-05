# Research: Dynamic API-Driven CDC Connectivity

**Feature**: 002-dynamic-cdc-api  
**Date**: 2026-04-05

## R1: Coordination Mechanism — RabbitMQ vs Kafka Internal Topic

**Decision**: Use **RabbitMQ** to coordinate CDC connection requests between the api-service and cdc-worker.

**Rationale**:
- The api-service already integrates with RabbitMQ via `MessageRepository` for ingestion and query job dispatch.
- RabbitMQ provides task-queue semantics (exactly-once delivery per consumer) which is ideal for "register a new connection" commands.
- Using Kafka internal topics for coordination would require adding a Kafka producer to the Spring Boot api-service (currently not a dependency).
- The actual CDC event stream still uses Kafka — RabbitMQ is only for the control plane (connection create/delete commands).

**Alternatives considered**:
- **Kafka internal topic**: Would unify all messaging on one broker, but adds unnecessary coupling and complexity to the api-service (which is a Spring Boot app, not a Spark app).
- **Redis Pub/Sub**: Simpler, but fire-and-forget — if the cdc-worker is down when a message is published, it's lost. RabbitMQ persists messages until acknowledged.
- **Direct HTTP calls to cdc-worker**: Would require the cdc-worker to run an HTTP server. The worker is a Spark application with no web framework — adding one would be over-engineering.

## R2: Dynamic Topic Discovery — `subscribePattern` vs Explicit Subscribe

**Decision**: Switch the cdc-worker's `KafkaStreamReader` from `subscribe("cdc.public.customers")` to `subscribePattern("cdc\\..*")` to auto-discover new topics.

**Rationale**:
- When a new Debezium connector is registered, it creates a new Kafka topic automatically (e.g., `cdc.public.orders`).
- Spark Structured Streaming's `subscribePattern` option periodically checks for new topics matching the regex — new topics are picked up on the next micro-batch without restarting the streaming query.
- This eliminates the need for the cdc-worker to be "notified" of new topics — topic discovery is built into the Kafka consumer protocol.

**Key implication**: The `foreachBatch` callback now receives events from **multiple tables** mixed in one batch. The writer must group events by source table (using the `topic` column from Kafka) and execute a separate MERGE INTO per table.

**Alternatives considered**:
- **Restart streaming query on new topic**: Stopping and restarting the Spark streaming query causes a gap in processing and is disruptive.
- **One streaming query per connection**: Would provide isolation but multiplies Spark resource usage (each query has its own executor threads, checkpoint, etc.). Not scalable beyond ~5 connections on a single node.

## R3: Dynamic Schema Discovery — Debezium Event vs JDBC Metadata

**Decision**: **Hybrid approach** — use JDBC metadata at connection creation time (api-service validates and discovers PK/columns), and use Debezium event schema at processing time (cdc-worker creates Iceberg tables from actual data).

**Rationale**:
- **At API time (JDBC)**: The api-service already needs to validate the DB connection. At the same time, it can query `information_schema.columns` and `information_schema.table_constraints` to discover column types and the primary key. This info is stored in Redis as part of the connection metadata.
- **At processing time (Debezium event)**: The cdc-worker extracts column names and types from the `after` payload of the first event. It uses this to dynamically construct the `CREATE TABLE IF NOT EXISTS` DDL and the `MERGE INTO` SQL.
- This hybrid approach detects schema issues early (at API time) while keeping the cdc-worker self-sufficient (it doesn't need JDBC access to the source DB).

**Alternatives considered**:
- **JDBC-only (cdc-worker connects to source)**: Would require the cdc-worker to have JDBC connectivity to arbitrary external databases — this is a security concern and adds PostgreSQL driver as a dependency to a Spark worker.
- **Debezium event-only**: No early validation — errors would only surface after Debezium starts sending events, which could be minutes later.

## R4: Credential Security — Encryption at Rest

**Decision**: Encrypt source DB passwords using **AES-256-GCM** before storing in Redis. The encryption key is loaded from an environment variable (`CDC_ENCRYPTION_KEY`).

**Rationale**:
- Redis is an in-memory store that doesn't provide encryption at rest by default.
- Source DB passwords are sensitive credentials that must not be exposed in Redis `HGETALL` dumps or RDB backups.
- AES-256-GCM provides both confidentiality and integrity (authenticated encryption).
- The encryption key is a single environment variable, manageable via Docker secrets or `.env` file.

**What is NOT encrypted**: Connection metadata (hostname, port, database name, table name, schema) — these are not secrets and are needed for display in the status API.

**Alternatives considered**:
- **HashiCorp Vault**: Enterprise-grade secret management, but too heavyweight for a development/graduation project.
- **No encryption (plaintext in Redis)**: Unacceptable — violates FR-003.
- **One-way hashing**: Passwords need to be recovered (to pass to Debezium), so hashing is not suitable.

## R5: Multi-Table MERGE INTO — Generic SQL Generation

**Decision**: Replace the hardcoded `MERGE INTO` SQL in `IcebergMergeWriter` with a **dynamic SQL builder** that constructs the statement from the event's column names.

**Rationale**:
- The current `IcebergMergeWriter` has hardcoded column names: `t.name = s.name, t.email = s.email, ...`. This only works for the `customers` table.
- For dynamic tables, the writer must inspect the DataFrame's schema (column names from the parsed event) and generate the SET clause, INSERT columns, and VALUES dynamically.
- The primary key column name is stored in the connection metadata (Redis) and used for the `ON t.{pk} = s.{pk}` join condition.

**Implementation approach**:
1. Group the micro-batch by `topic` column (each topic = one source table)
2. For each group, look up the connection metadata from Redis (or cache) to get the PK column
3. Get column names from the DataFrame schema (excluding `_cdc_op`, `_cdc_ts`)
4. Build `MERGE INTO` SQL dynamically:
   ```sql
   MERGE INTO iceberg.{ns}.{table} t
   USING (SELECT * FROM {tempView}) s
   ON t.{pk} = s.{pk}
   WHEN MATCHED AND s._cdc_op = 'd' THEN DELETE
   WHEN MATCHED THEN UPDATE SET {col1} = s.{col1}, {col2} = s.{col2}, ...
   WHEN NOT MATCHED AND s._cdc_op != 'd' THEN INSERT ({cols}) VALUES ({s.cols})
   ```

## R6: API-Side Debezium Registration — WebClient vs RestTemplate

**Decision**: Use **Spring WebClient** (reactive HTTP client) for calling the Debezium Connect REST API from the api-service.

**Rationale**:
- `RestTemplate` is in maintenance mode in Spring Boot 3 — `WebClient` is the recommended replacement.
- The Debezium REST API calls are synchronous from the user's perspective, but the API endpoint returns immediately (async pattern with `connectionId`).
- `WebClient` provides built-in timeout configuration, error handling, and retry support.

**Calls to Debezium Connect REST API**:
1. `GET /connectors/{name}` — Check if connector exists (idempotency)
2. `POST /connectors` — Register new connector
3. `GET /connectors/{name}/status` — Monitor connector status
4. `DELETE /connectors/{name}` — Remove connector

## R7: Connection Lifecycle State Machine

**Decision**: Use the following state machine for CDC connections, tracked in Redis per connectionId:

```
PENDING → VALIDATING → REGISTERING → SNAPSHOTTING → STREAMING
                 ↘            ↘              ↘           ↘
              VALIDATION_FAILED  REGISTRATION_FAILED  SNAPSHOT_FAILED  STREAM_FAILED
                                                                           ↕
                                                                       STREAMING (recoverable)
    
Any state → DELETING → DELETED
```

**States**:
| State | Owner | Description |
|-------|-------|-------------|
| PENDING | api-service | Connection request received, queued |
| VALIDATING | api-service | Testing JDBC connection to source DB |
| REGISTERING | api-service | POSTing connector config to Debezium |
| SNAPSHOTTING | cdc-worker | Debezium initial snapshot in progress |
| STREAMING | cdc-worker | Continuous CDC streaming active |
| *_FAILED | either | Error occurred, includes error message |
| DELETING | api-service | Removing connector from Debezium |
| DELETED | api-service | Connector removed, connection inactive |
