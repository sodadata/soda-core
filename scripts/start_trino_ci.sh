#!/bin/bash
set -euo pipefail

# Start a local Trino instance for CI.
#
# Required environment variables:
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION  — build credentials
#   TRINO_CI_ASSUME_ROLE_ARN  — role with S3/Glue access for Iceberg
#   CI_MODULE                 — "trino-postgres" or "trino-s3"
#
# Outputs (appended to GITHUB_ENV):
#   TRINO_HOST, TRINO_PORT, TRINO_USERNAME, TRINO_PASSWORD, TRINO_VERIFY

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCAL_INSTANCE_DIR="$REPO_ROOT/soda-trino/local_instance"

# ── Write env vars for test runner ────────────────────────────────────
echo "TRINO_HOST=localhost"   >> "$GITHUB_ENV"
echo "TRINO_PORT=8443"        >> "$GITHUB_ENV"
echo "TRINO_USERNAME=soda-test" >> "$GITHUB_ENV"
echo "TRINO_PASSWORD=soda-test" >> "$GITHUB_ENV"
echo "TRINO_VERIFY=false"     >> "$GITHUB_ENV"

# ── Generate TLS keys ────────────────────────────────────────────────
cd "$LOCAL_INSTANCE_DIR"
./generate_keys.sh

# ── Validate secrets ─────────────────────────────────────────────────
if [ -z "${TRINO_CI_ASSUME_ROLE_ARN:-}" ]; then
  echo "::error::TRINO_CI_ASSUME_ROLE_ARN secret is not set — Trino Iceberg tests require AWS credentials for S3/Glue access"
  exit 1
fi
echo "TRINO_CI_ASSUME_ROLE_ARN is set (length=${#TRINO_CI_ASSUME_ROLE_ARN})"

if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
  echo "::error::AWS_BUILD credentials are not set — cannot assume role"
  exit 1
fi
echo "AWS build credentials present, assuming role..."

# ── Assume IAM role for S3/Glue (Iceberg) ────────────────────────────
CREDS=$(aws sts assume-role \
  --role-arn "${TRINO_CI_ASSUME_ROLE_ARN}" \
  --role-session-name github-actions-trino-ci \
  --output json)
export AWS_ACCESS_KEY_ID=$(echo "$CREDS" | jq -r '.Credentials.AccessKeyId')
export AWS_SECRET_ACCESS_KEY=$(echo "$CREDS" | jq -r '.Credentials.SecretAccessKey')
export AWS_SESSION_TOKEN=$(echo "$CREDS" | jq -r '.Credentials.SessionToken')

if [ -z "${AWS_SESSION_TOKEN}" ] || [ "${AWS_SESSION_TOKEN}" = "null" ]; then
  echo "::error::aws sts assume-role succeeded but returned empty/null session token"
  exit 1
fi
echo "Role assumed successfully (AccessKeyId starts with ${AWS_ACCESS_KEY_ID:0:4}...)"

# ── Verify S3 access from host (warning only) ────────────────────────
echo "Verifying S3 access from host..."
if ! aws s3 ls s3://soda-dev-trino/ci-warehouse/ --region eu-west-1 > /dev/null 2>&1; then
  echo "::warning::Assumed role cannot list s3://soda-dev-trino/ci-warehouse/ (s3:ListBucket may not be granted — this may be OK if Trino has the permissions it needs)"
  aws s3 ls s3://soda-dev-trino/ci-warehouse/ --region eu-west-1 2>&1 || true
else
  echo "S3 ListBucket access verified from host"
fi
aws sts get-caller-identity || true

# ── Create Glue database (idempotent) ────────────────────────────────
aws glue create-database \
  --database-input '{"Name":"soda_ci_trino","Description":"Trino Iceberg CI"}' 2>/dev/null || true

# ── Choose Docker network ────────────────────────────────────────────
# trino-postgres needs the GHA service network to reach the postgres container.
# trino-s3 needs the default bridge network for outbound internet (S3/Glue).
NETWORK_FLAG=""
if [ "${CI_MODULE}" = "trino-postgres" ]; then
  GH_NETWORK=$(docker network ls --format '{{.Name}}' | grep -E '^github_network_' | head -1)
  if [ -n "$GH_NETWORK" ]; then
    NETWORK_FLAG="--network $GH_NETWORK"
    echo "trino-postgres: joining GHA service network ($GH_NETWORK) for postgres access"
  else
    echo "::warning::trino-postgres: no GHA service network found — postgres container may not be reachable"
  fi
else
  echo "trino-s3: using default Docker network for outbound S3/Glue access"
fi

# ── Start Trino container ────────────────────────────────────────────
docker run -d --name trino-ci -p 8443:8443 \
  $NETWORK_FLAG \
  -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
  -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
  -e AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}" \
  -e AWS_REGION="${AWS_REGION:-eu-west-1}" \
  -v "$(pwd)/trino-config:/etc/trino" \
  -v "$(pwd)/trino-catalog:/etc/trino/catalog" \
  trinodb/trino:latest

# ── Wait for /v1/info ────────────────────────────────────────────────
echo "Waiting for Trino to be ready..."
for i in $(seq 1 24); do
  if curl -k -s https://localhost:8443/v1/info > /dev/null 2>&1; then
    echo "Trino is ready"
    break
  fi
  if [ "$i" -eq 24 ]; then
    echo "::error::Trino failed to start within 2 minutes"
    docker logs trino-ci
    exit 1
  fi
  sleep 5
done

# ── Verify S3 connectivity from inside the container ─────────────────
echo "Verifying S3 access from inside Trino container..."
docker exec trino-ci bash -c '
  if command -v curl > /dev/null 2>&1; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 https://s3.eu-west-1.amazonaws.com 2>/dev/null || echo "000")
    if [ "$HTTP_CODE" = "000" ]; then
      echo "::error::Trino container cannot reach S3 endpoint (https://s3.eu-west-1.amazonaws.com) — network issue"
      exit 1
    fi
    echo "S3 endpoint reachable from container (HTTP $HTTP_CODE)"
  else
    echo "WARNING: curl not available in container, skipping S3 connectivity check"
  fi
' || echo "WARNING: Could not verify S3 connectivity from container (non-fatal)"

# ── Wait for query engine ────────────────────────────────────────────
echo "Waiting for Trino query engine to accept queries..."
for i in $(seq 1 30); do
  if curl -k -s -f -u soda-test:soda-test \
    -X POST https://localhost:8443/v1/statement \
    -H 'X-Trino-User: soda-test' \
    -d 'SHOW CATALOGS' > /dev/null 2>&1; then
    echo "Trino query engine ready (attempt $i)"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "::error::Trino query engine not ready after 60s"
    docker logs trino-ci 2>&1 | tail -50
    exit 1
  fi
  sleep 2
done

# ── Verify Iceberg/Glue connectivity ─────────────────────────────────
echo "Verifying Trino can reach Glue via Iceberg catalog..."
RESULT=$(curl -k -s -f -u soda-test:soda-test \
  -X POST https://localhost:8443/v1/statement \
  -H 'X-Trino-User: soda-test' \
  -d 'SHOW SCHEMAS FROM iceberg' 2>&1) || true
echo "Iceberg schema query response: ${RESULT:0:500}"
