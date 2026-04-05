# Feature Specification: Dynamic API-Driven CDC Connectivity

**Feature Branch**: `002-dynamic-cdc-api`  
**Created**: 2026-04-05  
**Status**: Draft  
**Depends on**: `001-cdc-pipeline-compaction` (completed)  
**Input**: Transition CDC management from hardcoded CLI commands to a dynamic, API-driven architecture where any external PostgreSQL table can be synced to the lakehouse on demand.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Create a CDC Connection via API (Priority: P1)

As a data engineer, I want to send a single API request with source database credentials and a table name, and have the system automatically set up end-to-end CDC — from Debezium connector registration to Iceberg table creation and continuous streaming — so that I never need to manually configure connectors or edit docker-compose files.

**Why this priority**: This is the core value proposition — without the API endpoint, CDC remains a manual, CLI-driven process that requires direct Docker access.

**Independent Test**: Can be fully tested by sending a POST request with valid source DB credentials and table name, then verifying that a Debezium connector is registered, a Kafka topic is created, the Iceberg table is populated with the initial snapshot, and subsequent INSERT/UPDATE/DELETE operations are reflected.

**Acceptance Scenarios**:

1. **Given** a running platform with no CDC connections, **When** a user sends a POST request to `/api/v1/cdc/connections` with valid PostgreSQL credentials and a table name, **Then** the system returns a `connectionId` and status URL, registers a Debezium connector, and begins syncing data.
2. **Given** a CDC connection request with invalid database credentials, **When** the API processes the request, **Then** the system returns a clear error (e.g., "Cannot connect to source database") without exposing or logging the plaintext password.
3. **Given** a CDC connection already exists for the same database and table, **When** a duplicate request is submitted, **Then** the system returns the existing connection's ID and status (idempotent behavior).

---

### User Story 2 - Dynamic Iceberg Table Creation (Priority: P1)

As a data engineer, I want the CDC worker to automatically discover the schema of the source table and create the corresponding Iceberg table dynamically — so that I don't need to predefine table schemas.

**Why this priority**: Without dynamic schema discovery, adding a new table requires code changes to the cdc-worker (currently hardcoded to `customers` schema). This is a hard prerequisite for multi-table support.

**Independent Test**: Can be tested by creating a CDC connection for a table with an arbitrary schema, then verifying the Iceberg table is created with matching columns and types.

**Acceptance Scenarios**:

1. **Given** a new CDC connection is created for a table with columns `(id INT, title TEXT, price NUMERIC, created TIMESTAMP)`, **When** the CDC worker begins processing, **Then** it creates an Iceberg table with equivalent column types and CDC audit columns (`_cdc_op`, `_cdc_ts`).
2. **Given** the first CDC event for a new table arrives on Kafka, **When** the worker processes it, **Then** it infers the schema from the Debezium event's `after` payload and creates the target Iceberg table automatically.

---

### User Story 3 - Connection Status Monitoring (Priority: P1)

As a data engineer, I want to monitor the real-time status of each CDC connection via API — including whether it's registering, snapshotting, actively streaming, or has failed — so that I have full observability without checking Docker logs.

**Why this priority**: Status visibility is critical for a self-service API. Users need to know if their sync request succeeded, is in progress, or failed.

**Independent Test**: Can be tested by creating a connection and polling the status endpoint, verifying the status transitions from PENDING → REGISTERING → SNAPSHOTTING → STREAMING.

**Acceptance Scenarios**:

1. **Given** a CDC connection is created, **When** the user queries `GET /api/v1/cdc/connections/{connectionId}`, **Then** the response includes the current status, the source table, the target Iceberg table, and timestamp of last sync.
2. **Given** a CDC connection has failed (e.g., source DB went down), **When** the user queries the status endpoint, **Then** the status shows `FAILED` with an error message describing the failure.
3. **Given** a user wants to see all active CDC connections, **When** they query `GET /api/v1/cdc/connections`, **Then** the response lists all connections with their current statuses.

---

### User Story 4 - Stop/Delete a CDC Connection (Priority: P2)

As a data engineer, I want to stop or delete a CDC connection to discontinue syncing a table — so that I can free resources or remove tables I no longer need.

**Why this priority**: Lifecycle management is important but not required for initial launch. Users can manually manage connectors in the short term.

**Acceptance Scenarios**:

1. **Given** an active CDC connection, **When** a user sends `DELETE /api/v1/cdc/connections/{connectionId}`, **Then** the Debezium connector is unregistered, the streaming query for that topic stops, and the status is set to `DELETED`.
2. **Given** a deleted connection, **When** the user queries its status, **Then** the response shows `DELETED` and the Iceberg table data is preserved (not deleted).

---

### Edge Cases

- What happens when the Debezium Connect service is temporarily unreachable? The API should return a 503 error and the connection status should be `REGISTRATION_FAILED` with a retry mechanism.
- What happens when the source table has no primary key? The system should reject the connection with a clear error, as CDC requires a primary key for MERGE INTO.
- What happens when two concurrent requests try to create connections for the same table? The system uses Redis-based locking to ensure idempotent connector registration.
- What happens when the CDC worker restarts while multiple connections are active? The worker must rediscover all active connections from Redis and resume streaming from Kafka checkpoints.
- What happens when the source database password changes after a connection is established? The connection enters FAILED state; the user must create a new connection with updated credentials.

## Requirements *(mandatory)*

### Functional Requirements

**API Control Plane**

- **FR-001**: System MUST provide `POST /api/v1/cdc/connections` to create a new CDC connection with source DB host, port, database name, user, password, schema, and table name.
- **FR-002**: System MUST validate source DB connectivity by attempting a JDBC connection before registering the Debezium connector.
- **FR-003**: System MUST never log, return in API responses, or store in plaintext the source database password. Passwords MUST be stored encrypted in Redis using AES-256 or equivalent.
- **FR-004**: System MUST provide `GET /api/v1/cdc/connections` to list all CDC connections with their statuses.
- **FR-005**: System MUST provide `GET /api/v1/cdc/connections/{connectionId}` to get detailed status of a specific connection.
- **FR-006**: System MUST provide `GET /api/v1/cdc/connections/{connectionId}/wait` long-poll endpoint (matching the existing `/wait` pattern).
- **FR-007**: System MUST provide `DELETE /api/v1/cdc/connections/{connectionId}` to remove a connection and its Debezium connector.

**Debezium Registration**

- **FR-008**: The api-service MUST programmatically register Debezium connector configurations by POSTing JSON to the Debezium Connect REST API (`:8083/connectors`).
- **FR-009**: Each connector MUST have a unique name derived from the connection ID (e.g., `cdc-conn-{connectionId}`).
- **FR-010**: Each connector MUST use its own replication slot (named `debezium_slot_{connectionId}`) to avoid conflicts with other connectors.
- **FR-011**: The system MUST check if a connector already exists before registration (idempotent behavior).

**Dynamic Worker Processing**

- **FR-012**: The cdc-worker MUST use Kafka topic pattern subscription (e.g., `subscribePattern("cdc\\..*")`) to automatically discover new topics created by Debezium.
- **FR-013**: The cdc-worker MUST dynamically infer the Iceberg table schema from the first CDC event's payload — no hardcoded schemas.
- **FR-014**: The cdc-worker MUST derive the target Iceberg table name from the Kafka topic name (e.g., topic `cdc.public.orders` → table `iceberg.cdc_namespace.orders`).
- **FR-015**: The MERGE INTO logic MUST dynamically build SQL from the event's column names — not hardcode column references.

**Coordination & Status**

- **FR-016**: The api-service MUST publish a `CdcConnectionMessage` to RabbitMQ to notify the cdc-worker of new connection registrations.
- **FR-017**: The cdc-worker MUST report connection-level status to Redis (per connectionId) with states: `PENDING`, `REGISTERING`, `SNAPSHOTTING`, `STREAMING`, `FAILED`, `DELETED`.
- **FR-018**: The api-service MUST be able to read connection status from Redis and return it via the status API endpoints.

### Key Entities

- **CDC Connection**: A user-requested sync between one source table and one Iceberg table. Identified by `connectionId`. Attributes: source credentials, table name, connector name, status, target Iceberg table.
- **Debezium Connector Config**: The JSON configuration registered with Kafka Connect. Derived from the CDC Connection. Unique per source database + table combination.
- **Connection Status**: Real-time state tracked in Redis. Maps connectionId → {status, sourceTable, targetTable, connectorName, lastSyncTs, error}.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A new CDC connection can be created and begin streaming data within 60 seconds of the API request (including Debezium registration and initial snapshot for a 10-row table).
- **SC-002**: Source database passwords are never visible in API responses, application logs, Redis plaintext values, or Debezium connector status.
- **SC-003**: The cdc-worker handles 5+ concurrent CDC connections (different tables, same or different source DBs) without data loss or cross-contamination.
- **SC-004**: After cdc-worker restart, all active connections resume streaming from their last checkpoint — zero data loss.
- **SC-005**: The MERGE INTO logic works correctly for tables with any column schema — not just the hardcoded `customers` columns.
- **SC-006**: Deleting a connection removes the Debezium connector and stops processing, but preserves the Iceberg table data.
- **SC-007**: Creating a duplicate connection (same source DB + table) returns the existing connection instead of creating a conflict.

## Assumptions

- Source databases are PostgreSQL (version 10+) with `wal_level=logical` already configured. The system validates this and returns a clear error if not.
- The Debezium Connect and Kafka services are already running as part of the platform infrastructure (from spec 001).
- Each source table MUST have a primary key column. Tables without a PK are rejected at connection creation time.
- The primary key column name is discoverable from the Debezium event's key schema or from a JDBC metadata query.
- One CDC connection corresponds to exactly one source table and one target Iceberg table.
- The existing `source-postgres` container and its `customers` table (from spec 001) continues to work as-is alongside any new dynamic connections.
