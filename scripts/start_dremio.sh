#!/usr/bin/env bash

mkdir -p soda-dremio/.dremio-data
sudo chown -R 999:999 .dremio-data
docker-compose -f soda-dremio/docker-compose.yml up --remove-orphans
