# CDC Kafka Event Contract

**Type**: Kafka Message (JSON, no schema registry)  
**Topic**: `cdc.public.customers`

## Message Key

```json
{
  "id": 42
}
```

The key is the row's primary key, enabling Kafka log compaction and partition-based ordering.

## Message Value — INSERT (`op: "c"`)

```json
{
  "payload": {
    "op": "c",
    "before": null,
    "after": {
      "id": 42,
      "name": "Mohamed Ahmed",
      "email": "mohamed@example.com",
      "status": "active",
      "created_at": 1743724800000000,
      "updated_at": 1743724800000000
    },
    "source": {
      "version": "2.5.0.Final",
      "connector": "postgresql",
      "name": "cdc",
      "ts_ms": 1743724800123,
      "db": "sourcedb",
      "schema": "public",
      "table": "customers",
      "lsn": 98765432
    },
    "ts_ms": 1743724800150
  }
}
```

## Message Value — UPDATE (`op: "u"`)

```json
{
  "payload": {
    "op": "u",
    "before": {
      "id": 42,
      "name": "Mohamed Ahmed",
      "email": "mohamed@example.com",
      "status": "active",
      "created_at": 1743724800000000,
      "updated_at": 1743724800000000
    },
    "after": {
      "id": 42,
      "name": "Mohamed Ahmed",
      "email": "mohamed.new@example.com",
      "status": "active",
      "created_at": 1743724800000000,
      "updated_at": 1743811200000000
    },
    "source": { ... },
    "ts_ms": 1743811200150
  }
}
```

## Message Value — DELETE (`op: "d"`)

```json
{
  "payload": {
    "op": "d",
    "before": {
      "id": 42,
      "name": "Mohamed Ahmed",
      "email": "mohamed.new@example.com",
      "status": "active",
      "created_at": 1743724800000000,
      "updated_at": 1743811200000000
    },
    "after": null,
    "source": { ... },
    "ts_ms": 1743897600150
  }
}
```

## Message Value — SNAPSHOT (`op: "r"`)

Same as INSERT but `op` is `"r"`. Used during Debezium's initial snapshot.

## Processing Rules

1. **`c` and `r`**: Use `after` image → UPSERT into Iceberg
2. **`u`**: Use `after` image → UPSERT into Iceberg
3. **`d`**: Use `before` image for PK → DELETE from Iceberg
4. **Dedup**: Within a micro-batch, keep only the latest event per PK (by `ts_ms` or Kafka offset)
