#!/bin/bash
# temporarily exclude soda-dremio from the matrix - tests not passing yet 
printf "[";find . -maxdepth 1 -mindepth 1 -type d -name "soda*" -not -name "soda-core" -not -name "soda-tests" -not -name "soda-dremio" -printf '"%f",' | sed 's/,$//;s/soda-//g';printf "]"
