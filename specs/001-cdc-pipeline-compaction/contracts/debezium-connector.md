# Debezium Connector Contract

**Type**: Kafka Connect REST API (Debezium → Kafka)  
**Base URL**: `http://debezium-connect:8083`

## Register Connector

**Endpoint**: `POST /connectors`

**Request Body** (JSON):
```json
{
  "name": "source-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "source-postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "debezium",
    "database.dbname": "sourcedb",
    "topic.prefix": "cdc",
    "table.include.list": "public.customers",
    "plugin.name": "pgoutput",
    "publication.name": "dbz_publication",
    "slot.name": "debezium_slot",
    "publication.autocreate.mode": "filtered",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

**Response** (201 Created):
```json
{
  "name": "source-postgres-connector",
  "config": { ... },
  "tasks": [{ "connector": "source-postgres-connector", "task": 0 }]
}
```

## Kafka Topic Convention

| Topic Name                | Description                  |
|--------------------------|------------------------------|
| `cdc.public.customers`   | CDC events for customers table |

**Message format**: Debezium JSON envelope (no schema registry).

## Connector Status

**Endpoint**: `GET /connectors/source-postgres-connector/status`

**Response**:
```json
{
  "name": "source-postgres-connector",
  "connector": { "state": "RUNNING", "worker_id": "debezium-connect:8083" },
  "tasks": [{ "id": 0, "state": "RUNNING", "worker_id": "debezium-connect:8083" }]
}
```
