#!/usr/bin/env bash

# Run this from the root project dir with scripts/start_test_postgres_db.sh

( cd test/postgres ; docker-compose up )
