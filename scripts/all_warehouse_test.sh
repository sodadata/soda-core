#!/usr/bin/env bash

# Run this from the root project dir with scripts/run_all_tests.sh

. .venv/bin/activate

WAREHOUSES=('redshift' 'snowflake' 'athena')
TEST=$1

for i in "${WAREHOUSES[@]}"
do
   :
   export SODA_TEST_TARGET=$i
   echo Running test on $SODA_TEST_TARGET
   python -m unittest $TEST
done

# python -m pytest tests/local
