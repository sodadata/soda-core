#!/usr/bin/env bash

# Run this from the root project dir with scripts/start_postgres_container.sh

( cd tests/postgres_container ; docker-compose up )
