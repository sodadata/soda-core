#!/bin/bash
printf ;find . -maxdepth 1 -mindepth 1 -type d -name "soda*" -not -name "soda-core" -not -name "soda-tests" -printf '"%f",' | sed 's/,$//;s/soda-//g';
