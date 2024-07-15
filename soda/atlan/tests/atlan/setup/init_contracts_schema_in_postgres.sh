#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ENV_FILE_PATH=$( cd -- "$( dirname -- "${SCRIPT_DIR}/../.." )" &> /dev/null && pwd )
echo Loading environment vars from: "$ENV_FILE_PATH"

. "$ENV_FILE_PATH/.env"

if [ -z $CONTRACTS_POSTGRES_HOST ] | [ -z $CONTRACTS_POSTGRES_USERNAME ] | [ -z $CONTRACTS_POSTGRES_PASSWORD ] | [ -z $CONTRACTS_POSTGRES_DATABASE ]; then
  echo CONTRACTS_POSTGRES_* variables not defined.  Copy .env.example to .env and fill in the variables
  exit 1
else
  echo DB host $CONTRACTS_POSTGRES_HOST
  echo DB user $CONTRACTS_POSTGRES_USERNAME
fi

INIT_SQL_FILE_PATH="$SCRIPT_DIR/init_contracts_schema_in_postgres.sql"

echo Initializing contracts DB with SQL file: "$INIT_SQL_FILE_PATH"

CONTRACTS_POSTGRES_URL="postgresql://$CONTRACTS_POSTGRES_USERNAME:$CONTRACTS_POSTGRES_PASSWORD@$CONTRACTS_POSTGRES_HOST/$CONTRACTS_POSTGRES_DATABASE"

psql "$CONTRACTS_POSTGRES_URL" -a -f "$INIT_SQL_FILE_PATH"
