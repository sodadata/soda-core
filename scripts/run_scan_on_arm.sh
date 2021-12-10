#!/usr/bin/env bash
docker compose -f docker-compose-arm.yml -p soda-sql run soda scan "$@"
