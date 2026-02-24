#!/bin/bash

# Generate JSON array of test modules from soda-* directories.
# Trino is split into two entries (trino-postgres, trino-s3) to test both catalogs.
MODULES=$(find . -maxdepth 1 -mindepth 1 -type d -name "soda*" -not -name "soda-core" -not -name "soda-tests" -printf '"%f",' | sed 's/,$//;s/soda-//g')
MODULES=$(echo "$MODULES" | sed 's/"trino"/"trino-postgres","trino-s3"/g')
printf "[%s]" "$MODULES"
