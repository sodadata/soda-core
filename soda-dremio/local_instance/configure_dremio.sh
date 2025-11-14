#!/bin/bash

echo "👉 Checking if Dremio is running..."
if ! curl -fs http://localhost:9047 >/dev/null 2>&1; then
  echo "❌ Dremio API not reachable at http://localhost:9047"
  echo "Make sure your container is running and port 9047 is exposed:"
  echo "    docker compose ps"
  exit 1
fi

echo "⏳ Waiting for Dremio API to become ready..."
until curl -fs http://localhost:9047/apiv2/server_status | grep -q 'OK'; do
  echo "  ... still waiting ..."
  sleep 5
done
echo "✅ Dremio API is ready!"

# --- Create admin user if not already present ---
# this will fail if you run the script twice, but oh well, don't know a good way to test if it exists
echo "Creating admin user (admin / admin1234)..."
curl -s -X PUT http://localhost:9047/apiv2/bootstrap/firstuser \
  -H "Content-Type: application/json" \
  -H "Authorization: _dremionull" \
  -d '{
    "userName": "admin",
    "firstName": "Soda",
    "lastName": "Admin",
    "email": "admin@soda.io",
    "password": "admin1234"
  }' >/dev/null
echo "✅ Admin user created."

# --- Log in to obtain auth token ---
echo "Logging in to obtain auth token..."
TOKEN=$(curl -s -X POST "http://localhost:9047/apiv2/login" \
  -H "Content-Type: application/json" \
  -d '{"userName": "admin", "password": "admin1234"}' | jq -r '.token')

if [[ -z "$TOKEN" || "$TOKEN" == "null" ]]; then
  echo "❌ Failed to log in and obtain token."
  exit 1
fi
echo "✅ Logged in. Token obtained."

# --- Create Postgres source if missing ---
SOURCE_NAME="postgres"
echo "Checking for existing Postgres source..."
SOURCE_RESPONSE=$(curl -s -X GET "http://localhost:9047/api/v3/source" \
  -H "Authorization: _dremionb7u6c3uibhjss72s8sb0dgbkr")

if echo "$SOURCE_RESPONSE" | grep -q '"postgres"'; then
  exit 1
fi

set -e  
echo "Creating new Postgres source '$SOURCE_NAME'..."
curl -s -X POST "http://localhost:9047/api/v3/source" \
  -H "Authorization: _dremionb7u6c3uibhjss72s8sb0dgbkr" \
  -H "Content-Type: application/json" \
  -d '{
    "entityType": "source",
    "config": {
      "type": "POSTGRES",
      "hostname": "localhost",
      "port": 5432,
      "database": "soda_test",
      "authenticationType": "PLAIN",
      "username": "soda_test",
      "password": "",
      "fetchSize": 200
    },
    "name": "postgres",
    "metadataPolicy": {
      "authTTLMs": 86400000,
      "namesRefreshMs": 3600000,
      "datasetRefreshAfterMs": 3600000,
      "datasetExpireAfterMs": 10800000,
      "deleteUnavailableDatasets": true,
      "autoPromoteDatasets": true
    }
  }' 
echo "✅ Postgres source created successfully."
