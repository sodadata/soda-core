#!/bin/bash

printf "[";find . -maxdepth 1 -mindepth 1 -type d -name "soda*" -not -name "soda-core" -not -name "soda-tests" -not -name "soda-databricks" -not -name "soda-fabric" -not -name "soda-synapse" -not -name "soda-sqlserver" -not -name "soda-duckdb" -printf '"%f",' | sed 's/,$//;s/soda-//g';printf "]"
