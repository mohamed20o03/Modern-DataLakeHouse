# Implementation Plan: Dynamic API-Driven CDC Connectivity

**Branch**: `002-dynamic-cdc-api` | **Date**: 2026-04-05 | **Spec**: [spec.md](./spec.md)  
**Depends on**: `001-cdc-pipeline-compaction` (completed)

## Summary

Transition the CDC subsystem from hardcoded CLI-driven configuration to a dynamic, API-driven architecture. The api-service gains a new `/api/v1/cdc/connections` module that accepts source database credentials, validates connectivity, registers Debezium connectors programmatically, and coordinates with the cdc-worker via RabbitMQ. The cdc-worker is refactored from single-table/single-topic to multi-table/pattern-subscription, with dynamic schema discovery and generic MERGE INTO SQL generation. Connection status is tracked in Redis with a full lifecycle state machine.

## Technical Context

**Language/Version**: Java 21  
**API Framework**: Spring Boot 3 (api-service), Plain Java (cdc-worker)  
**Primary Dependencies**: Spring WebClient (Debezium API calls), Apache Spark 3.5, Apache Iceberg, Apache Kafka, Debezium 2.5, RabbitMQ, Lettuce (Redis), javax.crypto (AES-256-GCM)  
**Storage**: Redis (connection metadata + encrypted credentials), Kafka (CDC events), MinIO (Iceberg data)  
**Testing**: Docker Compose integration tests, manual API verification  
**Constraints**: Must not break existing `source-postgres` → `customers` CDC pipeline; existing ingestion/query workers unaffected

## Constitution Check

- ✅ **SOLID principles**: New CDC module follows existing controller → service → DTO pattern
- ✅ **Existing patterns**: Factory, repository, DeferredResult patterns reused
- ✅ **Separation of concerns**: API validation, Debezium registration, worker processing are distinct classes
- ✅ **Security**: Credentials encrypted at rest (AES-256-GCM), never logged, never returned in API responses

## Project Structure

### API Service — New CDC Module

```text
api-service/src/main/java/com/abdelwahab/api_service/
├── module/cdc/                                    # NEW MODULE
│   ├── controller/
│   │   └── CdcConnectionController.java           # REST endpoints
│   ├── service/
│   │   ├── CdcConnectionService.java              # Orchestration logic
│   │   ├── DebeziumRegistrationService.java        # HTTP calls to Debezium Connect REST API
│   │   └── SourceDatabaseValidator.java            # JDBC connectivity + schema validation
│   ├── dto/
│   │   ├── CdcConnectionRequest.java              # Input DTO
│   │   ├── CdcConnectionResponse.java             # Output DTO (create)
│   │   └── CdcConnectionStatus.java               # Output DTO (status)
│   └── security/
│       └── CredentialEncryptor.java                # AES-256-GCM encrypt/decrypt
├── config/
│   └── RabbitMqConfig.java                         # MODIFY — add cdc.queue declaration
```

### CDC Worker — Refactored for Dynamic Multi-Table

```text
cdc-worker/src/main/java/com/abdelwahab/cdc_worker/
├── consumer/kafka/
│   └── KafkaStreamReader.java                      # MODIFY — subscribePattern instead of subscribe
├── engine/spark/
│   ├── SparkCdcEngine.java                         # MODIFY — add RabbitMQ listener for connection commands
│   ├── CdcStreamProcessor.java                     # MODIFY — group events by topic
│   ├── IcebergMergeWriter.java                     # REWRITE — dynamic schema, per-topic MERGE INTO
│   ├── DynamicTableCreator.java                    # NEW — creates Iceberg tables from event schema
│   └── DebeziumEventParser.java                    # MODIFY — generic column extraction (not hardcoded)
├── registry/
│   └── ConnectionRegistry.java                     # NEW — reads/caches connection metadata from Redis
├── consumer/rabbitmq/
│   └── CdcCommandConsumer.java                     # NEW — listens for CREATE/DELETE connection commands
```

### Modified Files

```text
docker-compose.yml                                  # MODIFY — add cdc.queue env vars, PostgreSQL JDBC driver
.env                                                # MODIFY — add CDC_ENCRYPTION_KEY, DEBEZIUM_CONNECT_URL
api-service/pom.xml                                 # MODIFY — add spring-boot-starter-webflux (WebClient), postgresql JDBC driver
cdc-worker/pom.xml                                  # MODIFY — add RabbitMQ client dependency
```

## Architecture Flow

```
                         ┌─────────────────────┐
                         │   POST /api/v1/cdc/ │
                         │    connections       │
                         └────────┬────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │     api-service            │
                    │  1. Validate JDBC connect  │
                    │  2. Discover PK + columns  │
                    │  3. Encrypt password       │
                    │  4. Store in Redis         │
                    │  5. Register Debezium      │
                    │  6. Publish to RabbitMQ    │
                    │  7. Return connectionId    │
                    └───────┬───────┬───────────┘
                            │       │
                   ┌────────▼──┐  ┌─▼────────────────┐
                   │  Redis    │  │  RabbitMQ         │
                   │  (status) │  │  (cdc.connections │
                   │           │  │   .queue)         │
                   └────────┬──┘  └─┬────────────────┘
                            │       │
                    ┌───────▼───────▼───────────┐
                    │     cdc-worker             │
                    │  1. Receive connection msg │
                    │  2. Cache metadata         │
                    │  3. Kafka already discovers │
                    │     new topic via pattern  │
                    │  4. Create Iceberg table   │
                    │  5. Dynamic MERGE INTO     │
                    │  6. Update Redis status    │
                    └───────────────────────────┘
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| RabbitMQ for control plane | Already integrated in api-service; persistent delivery; simpler than adding Kafka producer to Spring Boot |
| `subscribePattern("cdc\\..*")` | Auto-discovers new Debezium topics; no streaming query restart needed |
| Dynamic MERGE INTO from DataFrame schema | Supports any table schema without code changes |
| JDBC validation at API time | Catches config errors early (wrong host, no replication, no PK) before committing to Debezium |
| AES-256-GCM for password encryption | Authenticated encryption; single env var for key; recoverable (Debezium needs the password) |
| Group foreachBatch by topic | One streaming query handles all connections; each topic = one table = one MERGE INTO |
| ConnectionRegistry (Redis cache) | cdc-worker needs PK column per table; read from Redis once, cache in memory |

## Complexity Tracking

| Component | Decision | Rationale |
|-----------|----------|-----------|
| JDBC driver in api-service | New dependency (postgresql) | Needed for source DB validation. Small JAR, no risk. |
| WebClient in api-service | New dependency (spring-webflux) | Needed for Debezium REST API calls. Standard Spring Boot practice. |
| RabbitMQ in cdc-worker | New dependency (amqp-client) | Needed for connection command messages. Lightweight client, no Spring required. |
| IcebergMergeWriter rewrite | Breaking change to existing class | The hardcoded MERGE INTO is fundamentally incompatible with dynamic schemas. Full rewrite required but preserves the same foreachBatch interface. |
| Backward compatibility | Keep existing `source-postgres` + `customers` working | Use the same `cdc` topic prefix. Pattern subscription picks up old and new topics. ConnectionRegistry falls back to default config if no Redis entry exists for a topic. |

## Verification Plan

### Automated Tests
1. `docker compose up` from clean state — verify existing `customers` CDC still works (backward compat)
2. `POST /api/v1/cdc/connections` with the existing `source-postgres` credentials — verify connection creation, Debezium registration, and status transitions
3. Create a second test table in `source-postgres`, create a connection for it, verify data flows to a new Iceberg table
4. Test with invalid credentials — verify VALIDATION_FAILED status and no password leak in response/logs
5. Test duplicate connection — verify idempotent behavior
6. DELETE a connection — verify connector removed and status is DELETED

### Manual Verification
- Check Redis for encrypted password (not plaintext)
- Check application logs for absence of passwords
- Verify `GET /api/v1/cdc/connections` lists all connections with correct statuses
