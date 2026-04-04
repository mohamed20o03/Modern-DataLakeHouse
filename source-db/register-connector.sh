#!/bin/bash
# ============================================================================
# Debezium Connector Registration Script
# Waits for Kafka Connect to be ready, then registers the PostgreSQL connector.
# ============================================================================
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://debezium-connect:8083}"
MAX_RETRIES=30
RETRY_INTERVAL=5

echo "Waiting for Kafka Connect at ${CONNECT_URL}..."

for i in $(seq 1 $MAX_RETRIES); do
    if curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
        echo "Kafka Connect is ready!"
        break
    fi
    echo "  Attempt $i/$MAX_RETRIES — not ready yet, sleeping ${RETRY_INTERVAL}s..."
    sleep $RETRY_INTERVAL
done

# Check if connector already exists
if curl -sf "${CONNECT_URL}/connectors/source-postgres-connector" > /dev/null 2>&1; then
    echo "Connector 'source-postgres-connector' already exists — skipping registration"
    exit 0
fi

echo "Registering Debezium PostgreSQL connector..."

curl -sf -X POST "${CONNECT_URL}/connectors" \
    -H "Content-Type: application/json" \
    -d '{
  "name": "source-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "source-postgres",
    "database.port": "5432",
    "database.user": "'"${SOURCE_POSTGRES_USER:-debezium}"'",
    "database.password": "'"${SOURCE_POSTGRES_PASSWORD:-debezium}"'",
    "database.dbname": "'"${SOURCE_POSTGRES_DB:-sourcedb}"'",
    "topic.prefix": "'"${CDC_TOPIC_PREFIX:-cdc}"'",
    "table.include.list": "'"${CDC_TABLE_INCLUDE_LIST:-public.customers}"'",
    "plugin.name": "pgoutput",
    "publication.name": "dbz_publication",
    "slot.name": "debezium_slot",
    "publication.autocreate.mode": "all_tables",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}'

echo ""
echo "Connector registered successfully!"

# Verify status
sleep 3
echo "Verifying connector status..."
curl -sf "${CONNECT_URL}/connectors/source-postgres-connector/status" | python3 -m json.tool 2>/dev/null || \
    curl -sf "${CONNECT_URL}/connectors/source-postgres-connector/status"

echo ""
echo "Done!"
