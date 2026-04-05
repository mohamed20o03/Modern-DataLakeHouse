# Tasks: Dynamic API-Driven CDC Connectivity

**Feature**: 002-dynamic-cdc-api  
**Date**: 2026-04-05  
**Plan**: [plan.md](./plan.md) | **Spec**: [spec.md](./spec.md)

---

## Phase 1: API Service — CDC Module Foundation

**Purpose**: Create the new CDC connections module in the api-service with DTOs, controller, and basic service layer.

- [ ] T001 [US1] Create `CdcConnectionRequest` DTO with validation annotations (`@NotBlank`, `@Min`, `@Max`) for host, port, database, schema, table, username, password in `api-service/.../module/cdc/dto/CdcConnectionRequest.java`
- [ ] T002 [US1] Create `CdcConnectionResponse` DTO with connectionId, status, sourceTable, targetTable, checkStatusAt, createdAt in `api-service/.../module/cdc/dto/CdcConnectionResponse.java`
- [ ] T003 [US3] Create `CdcConnectionStatus` DTO with all status fields (excluding password/username) in `api-service/.../module/cdc/dto/CdcConnectionStatus.java`
- [ ] T004 [US1] Create `CdcConnectionController` with endpoints: `POST /api/v1/cdc/connections`, `GET /api/v1/cdc/connections`, `GET /api/v1/cdc/connections/{connectionId}`, `GET /api/v1/cdc/connections/{connectionId}/wait`, `DELETE /api/v1/cdc/connections/{connectionId}` in `api-service/.../module/cdc/controller/CdcConnectionController.java`
- [ ] T005 [US1] Create `CdcConnectionService` skeleton with `createConnection()`, `getStatus()`, `listConnections()`, `deleteConnection()`, `waitForCompletion()` methods in `api-service/.../module/cdc/service/CdcConnectionService.java`

**Checkpoint**: API endpoints compile and return stub responses. No actual DB validation or Debezium calls yet.

---

## Phase 2: Credential Security

**Purpose**: Implement secure credential handling for source database passwords.

- [ ] T006 [US1] Create `CredentialEncryptor` with `encrypt(plaintext)` → Base64-encoded ciphertext and `decrypt(ciphertext)` → plaintext using AES-256-GCM, keyed from `CDC_ENCRYPTION_KEY` env var in `api-service/.../module/cdc/security/CredentialEncryptor.java`
- [ ] T007 [US1] Add `CDC_ENCRYPTION_KEY` to `.env` with a default 32-byte Base64 key for development — document that this MUST be changed in production
- [ ] T008 [US1] Integrate `CredentialEncryptor` into `CdcConnectionService` — encrypt password before Redis storage, decrypt when building Debezium config
- [ ] T009 [US1] Add log masking: ensure `CdcConnectionService` and `CdcConnectionController` never log password fields — verify with grep across the module

**Checkpoint**: Passwords are encrypted in Redis. `HGETALL cdc-conn:*` shows ciphertext, not plaintext.

---

## Phase 3: Source Database Validation

**Purpose**: Validate source database connectivity and discover table metadata before registering a Debezium connector.

- [ ] T010 [US1] Add PostgreSQL JDBC driver dependency (`org.postgresql:postgresql`) to `api-service/pom.xml`
- [ ] T011 [US1] Create `SourceDatabaseValidator` with methods: `testConnection(host, port, db, username, password)` → boolean, `validateWalLevel(connection)` → boolean, `discoverPrimaryKey(connection, schema, table)` → String, `discoverColumns(connection, schema, table)` → List<ColumnInfo> in `api-service/.../module/cdc/service/SourceDatabaseValidator.java`
- [ ] T012 [US1] Integrate `SourceDatabaseValidator` into `CdcConnectionService.createConnection()` — validate before proceeding, return VALIDATION_FAILED with clear error messages on failure
- [ ] T013 [US1] Handle edge case: table has no primary key — return error "Table {table} has no primary key. CDC requires a primary key column for MERGE INTO."

**Checkpoint**: API rejects invalid connection requests with specific error messages. Valid requests pass validation and transition to REGISTERING state.

---

## Phase 4: Debezium Connector Registration

**Purpose**: Programmatically register and manage Debezium connectors via the Kafka Connect REST API.

- [ ] T014 [US1] Add `spring-boot-starter-webflux` dependency to `api-service/pom.xml` (for WebClient)
- [ ] T015 [US1] Create `DebeziumRegistrationService` with methods: `registerConnector(connectionId, config)`, `getConnectorStatus(connectorName)`, `deleteConnector(connectorName)`, `connectorExists(connectorName)` using WebClient to call `http://debezium-connect:8083` in `api-service/.../module/cdc/service/DebeziumRegistrationService.java`
- [ ] T016 [US1] Add `DEBEZIUM_CONNECT_URL` env var to `.env` and Spring Boot config (default: `http://debezium-connect:8083`)
- [ ] T017 [US1] Implement connector JSON config builder in `DebeziumRegistrationService` — generate unique connector name (`cdc-conn-{connectionId}`), unique slot name (`slot_{shortId}`), and set all required Debezium PostgreSQL Connector properties per data-model.md
- [ ] T018 [US1] Integrate `DebeziumRegistrationService` into `CdcConnectionService.createConnection()` — register connector after validation passes, update Redis status to REGISTERING → SNAPSHOTTING
- [ ] T019 [US1] Implement idempotency: check `connectorExists()` before registration. If exists, return existing connectionId from Redis lookup by connectorName
- [ ] T020 [US4] Implement `deleteConnection()` — call `deleteConnector()`, update Redis status to DELETING → DELETED

**Checkpoint**: Creating a connection registers a Debezium connector. The connector appears in `GET :8083/connectors`. Deleting removes it.

---

## Phase 5: RabbitMQ Coordination

**Purpose**: Establish RabbitMQ-based communication between api-service and cdc-worker for connection lifecycle events.

- [ ] T021 [US1] Declare `cdc.connections.queue` in `RabbitMqConfig.java` with `x-max-priority: 10`
- [ ] T022 [US1] Create `CdcConnectionMessage` class with fields: connectionId, action (CREATE/DELETE), sourceTable, topicName, targetTable, primaryKeyColumn in `api-service/.../module/cdc/dto/CdcConnectionMessage.java`
- [ ] T023 [US1] Publish `CdcConnectionMessage` to `cdc.connections.queue` after successful Debezium registration in `CdcConnectionService`
- [ ] T024 [US1] Add `amqp-client` dependency to `cdc-worker/pom.xml`
- [ ] T025 [US1] Create `CdcCommandConsumer` in cdc-worker that connects to RabbitMQ, listens on `cdc.connections.queue`, and processes CREATE/DELETE messages in `cdc-worker/.../consumer/rabbitmq/CdcCommandConsumer.java`
- [ ] T026 [US1] On CREATE message: store connection metadata in `ConnectionRegistry` (in-memory cache), update Redis status to SNAPSHOTTING
- [ ] T027 [US4] On DELETE message: mark connection as deleted in `ConnectionRegistry`, update Redis status to DELETED

**Checkpoint**: Creating a connection sends a RabbitMQ message that the cdc-worker receives and acknowledges.

---

## Phase 6: CDC Worker — Dynamic Multi-Table Processing

**Purpose**: Refactor the cdc-worker from single-table/hardcoded to multi-table/dynamic.

- [ ] T028 [US2] Modify `KafkaStreamReader` — change from `subscribe("cdc.public.customers")` to `subscribePattern("cdc\\..*")` in `cdc-worker/.../consumer/kafka/KafkaStreamReader.java`
- [ ] T029 [US2] Create `ConnectionRegistry` — thread-safe cache that loads connection metadata from Redis (PK column, column list, target table) keyed by Kafka topic name. Falls back to `CdcConfig` defaults for topics without a Redis entry (backward compat with `source-postgres.customers`) in `cdc-worker/.../registry/ConnectionRegistry.java`
- [ ] T030 [US2] Create `DynamicTableCreator` — given a DataFrame (first batch) and connection metadata, generates and executes `CREATE TABLE IF NOT EXISTS` DDL with columns inferred from the DataFrame schema + `_cdc_op` and `_cdc_ts` audit columns. Supports format-version 2 + MoR in `cdc-worker/.../engine/spark/DynamicTableCreator.java`
- [ ] T031 [US2] Modify `DebeziumEventParser.parse()` — make column extraction generic: iterate over all fields in the `after`/`before` JSON object instead of hardcoding `name`, `email`, `status`, etc. Preserve `_cdc_op` and `_cdc_ts` as before in `cdc-worker/.../engine/spark/DebeziumEventParser.java`
- [ ] T032 [US2] Rewrite `IcebergMergeWriter.call()` — group the micro-batch by `topic` column, then for each topic-group: (1) look up connection metadata from `ConnectionRegistry`, (2) call `DynamicTableCreator.ensureTable()` on first batch, (3) build dynamic MERGE INTO SQL from DataFrame column names and PK, (4) execute the MERGE in `cdc-worker/.../engine/spark/IcebergMergeWriter.java`
- [ ] T033 [US2] Implement dynamic MERGE INTO SQL builder in `IcebergMergeWriter` — iterate over non-CDC columns to build SET clause, INSERT column list, and VALUES. Use PK from `ConnectionRegistry` for the ON condition
- [ ] T034 [US1] Wire `ConnectionRegistry` and `CdcCommandConsumer` into `SparkCdcEngine.init()` — start the RabbitMQ consumer in a background thread alongside the streaming query

**Checkpoint**: The cdc-worker can process events from multiple Kafka topics, creating Iceberg tables dynamically for each.

---

## Phase 7: Connection Status & Monitoring

**Purpose**: Implement full connection status lifecycle in Redis, exposed via API.

- [ ] T035 [US3] Implement `CdcConnectionService.getStatus()` — read `cdc-conn:{connectionId}` hash from Redis, merge with live Debezium connector status from `DebeziumRegistrationService.getConnectorStatus()`, return `CdcConnectionStatus` DTO
- [ ] T036 [US3] Implement `CdcConnectionService.listConnections()` — scan Redis for `cdc-conn:*` keys, return list of `CdcConnectionStatus` DTOs
- [ ] T037 [US3] Implement `CdcConnectionService.waitForCompletion()` — use Redis Pub/Sub (existing `JobStatusService` pattern) to long-poll until connection reaches STREAMING or *_FAILED state
- [ ] T038 [US3] Implement status updates in cdc-worker — update Redis `cdc-conn:{connectionId}` status field as the worker progresses through SNAPSHOTTING → STREAMING or → *_FAILED states
- [ ] T039 [US3] Add `updatedAt` timestamp updates on every status transition (both api-service and cdc-worker sides)

**Checkpoint**: Status API returns real-time connection state. `/wait` endpoint resolves when streaming starts.

---

## Phase 8: Backward Compatibility & Integration

**Purpose**: Ensure the existing `source-postgres` → `customers` CDC pipeline continues to work alongside new dynamic connections.

- [ ] T040 [P] Implement `ConnectionRegistry` fallback — if a Kafka topic has no entry in Redis (e.g., `cdc.public.customers` from the existing hardcoded connector), use default config: PK=`id`, namespace from `CdcConfig.getCdcTargetNamespace()`, table name from topic
- [ ] T041 [P] Update `docker-compose.yml` — add `CDC_ENCRYPTION_KEY`, `DEBEZIUM_CONNECT_URL`, and RabbitMQ env vars to `cdc-worker` service; add `cdc.connections.queue` to RabbitMQ config
- [ ] T042 [P] Update `.env` — add `CDC_ENCRYPTION_KEY`, `DEBEZIUM_CONNECT_URL=http://debezium-connect:8083`
- [ ] T043 [P] Add PostgreSQL JDBC driver to api-service Docker image (already in pom.xml from T010)

**Checkpoint**: `docker compose up` from clean state — existing `customers` table syncs correctly AND new connections can be created via API.

---

## Phase 9: End-to-End Verification

**Purpose**: Full validation against all success criteria.

- [ ] T044 [US1] E2E test: Create a new table `orders (id SERIAL PK, product TEXT, quantity INT, price NUMERIC)` in `source-postgres`, then `POST /api/v1/cdc/connections` with source-postgres credentials and `orders` table. Verify: Debezium connector registered, Kafka topic created, Iceberg table created with correct schema, initial data visible
- [ ] T045 [US2] E2E test: INSERT/UPDATE/DELETE rows in `orders` table, verify reflected in Iceberg within 30s
- [ ] T046 [US3] E2E test: Verify status transitions PENDING → VALIDATING → REGISTERING → SNAPSHOTTING → STREAMING via GET /api/v1/cdc/connections/{connectionId}
- [ ] T047 [US1] Security test: Verify password is NOT in: API response body, application logs (`grep -r password`), Redis plaintext (`HGET cdc-conn:* sourcePassword` shows ciphertext)
- [ ] T048 [US1] Idempotency test: POST same connection twice, verify same connectionId returned
- [ ] T049 [US4] Deletion test: DELETE connection, verify Debezium connector removed, status=DELETED, Iceberg data preserved
- [ ] T050 [US1] Backward compat test: After all changes, verify `source-postgres.customers` CDC still works (from spec 001)
- [ ] T051 Run all 7 success criteria (SC-001 through SC-007) from spec.md

---

## Phase 10: Documentation

**Purpose**: Update project documentation to reflect the new dynamic CDC API.

- [ ] T052 [P] Update `README.md` — add CDC Connections API section with endpoint reference
- [ ] T053 [P] Update `docs/API_REFERENCE.md` — add CDC Connections API section (request/response format, status states, examples)
- [ ] T054 [P] Update `docs/ENHANCEMENT_GUIDE.md` — mark dynamic CDC connectivity as implemented
- [ ] T055 [P] Create `specs/002-dynamic-cdc-api/quickstart.md` — step-by-step guide to create and monitor a CDC connection

---

## Summary

| Phase | Tasks | Effort Estimate |
|-------|-------|-----------------|
| 1. API Module Foundation | T001–T005 | 1 day |
| 2. Credential Security | T006–T009 | 0.5 day |
| 3. Source DB Validation | T010–T013 | 1 day |
| 4. Debezium Registration | T014–T020 | 1.5 days |
| 5. RabbitMQ Coordination | T021–T027 | 1 day |
| 6. Dynamic Multi-Table | T028–T034 | 2 days |
| 7. Status & Monitoring | T035–T039 | 1 day |
| 8. Backward Compatibility | T040–T043 | 0.5 day |
| 9. E2E Verification | T044–T051 | 1 day |
| 10. Documentation | T052–T055 | 0.5 day |
| **Total** | **55 tasks** | **~10 days** |
