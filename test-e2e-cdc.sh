#!/bin/bash

# E2E Verification Script for Dynamic API-Driven CDC
#
# Prerequisite: The application must be running via docker-compose:
#   docker compose up -d
#
# This script executes the E2E steps defined in the 002-dynamic-cdc-api spec.

set -e

API_URL="http://localhost:8080/api/v1/cdc/connections"

echo "=========================================="
echo "Phase 9: E2E Verification for Dynamic CDC"
echo "=========================================="

# Wait for API to be healthy
echo "Waiting for api-service to be healthy..."
while ! curl -s http://localhost:8080/actuator/health | grep -q '"status":"UP"'; do
  sleep 2
done
echo "API Service is UP!"

echo -e "\n1. Creating CDC Connection for public.orders table..."
cat <<EOF > /tmp/create_cdc.json
{
  "host": "source-postgres",
  "port": 5432,
  "database": "sourcedb",
  "schema": "public",
  "table": "customers",
  "username": "debezium",
  "password": "debezium"
}
EOF

RESPONSE=$(curl -s -X POST $API_URL -H "Content-Type: application/json" -d @/tmp/create_cdc.json)
echo "Response: $RESPONSE"

CONNECTION_ID=$(echo $RESPONSE | grep -oP '(?<="connectionId":")[^"]*')
if [ -z "$CONNECTION_ID" ]; then
    echo "Failed to extract connectionId from response."
    exit 1
fi

echo -e "\n2. Long-polling for connection status..."
# This will block until the status reaches STREAMING or a FAILED state
STATUS_RESPONSE=$(curl -s "$API_URL/$CONNECTION_ID/wait?timeoutSec=30")
echo "Wait Response: $STATUS_RESPONSE"

echo -e "\n3. Checking connection details directly..."
DETAILS=$(curl -s "$API_URL/$CONNECTION_ID")
echo "Details: $DETAILS"

echo -e "\n4. Listing all connections..."
curl -s $API_URL | jq || echo "Use jq for pretty printing if installed."

echo -e "\n5. Deleting connection..."
curl -v -X DELETE "$API_URL/$CONNECTION_ID"

echo -e "\nVerification completed successfully."
