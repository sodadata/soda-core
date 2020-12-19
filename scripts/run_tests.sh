#!/usr/bin/env bash

# Run this from the root project dir with scripts/run_all_tests.sh

. .venv/bin/activate

python -m pytest tests
