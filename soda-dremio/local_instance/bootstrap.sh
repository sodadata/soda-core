#!/bin/bash
set -e

echo "Starting Dremio in background..."
/opt/dremio/bin/dremio start &

LOG_FILE="/var/log/dremio/server.out"

sleep 5
# Stream the logs in the background so you can see startup progress
tail -n 0 -f "$LOG_FILE" &
TAIL_PID=$!

echo "Waiting for Dremio API to become available..."
until curl -s http://localhost:9047/apiv2/server_status | grep -q '"status":"RUNNING"'; do
  sleep 5
done
echo "✅ Dremio API is ready"

# Stop tail once server is ready
kill $TAIL_PID 2>/dev/null || true
wait $TAIL_PID 2>/dev/null || true

# Check if admin user already exists
EXISTING=$(curl -s -X GET http://localhost:9047/apiv2/bootstrap | jq -r '.isAdminCreated')
if [ "$EXISTING" = "true" ]; then
  echo "Admin already created, skipping bootstrap."
else
  echo "Creating initial admin user..."
  curl -s -X POST http://localhost:9047/apiv2/bootstrap \
    -H "Content-Type: application/json" \
    -d '{
      "userName": "admin",
      "firstName": "Soda",
      "lastName": "Admin",
      "email": "admin@soda.io",
      "password": "Soda123!"
    }'
  echo "✅ Admin user created (username: admin, password: Soda123!)"
fi

# Log in to get token
echo "Logging in to obtain auth token..."
TOKEN=$(curl -s -X POST "http://localhost:9047/apiv2/login" \
  -H "Content-Type: application/json" \
  -d '{"userName": "admin", "password": "Soda123!"}' | jq -r '.token')

if [ -z "$TOKEN" ] || [ "$TOKEN" == "null" ]; then
  echo "❌ Failed to log in and obtain token"
  exit 1
fi
echo "✅ Logged in, token obtained"

# Check if Postgres source already exists
echo "Checking for existing Postgres source..."
EXISTING_SOURCE=$(curl -s -X GET "http://localhost:9047/apiv2/source/postgres" -H "Authorization: _dremio$TOKEN")

if echo "$EXISTING_SOURCE" | grep -q '"errorMessage"'; then
  echo "No existing Postgres source, creating one..."
  curl -s -X POST "http://localhost:9047/apiv2/source" \
    -H "Authorization: _dremio$TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
      "entityType": "source",
      "config": {
        "type": "POSTGRES",
        "hostname": "postgres",
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
        "datasetRefreshAfterMs": 3600000,
        "datasetExpireAfterMs": 10800000,
        "namesRefreshMs": 3600000,
        "datasetUpdateMode": "PREFETCH_QUERIED",
        "deleteUnavailableDatasets": true,
        "autoPromoteDatasets": true
      }
    }'
  echo "✅ Postgres source created successfully"
else
  echo "Postgres source already exists, skipping creation."
fi

echo "Keeping Dremio process in foreground..."
wait
