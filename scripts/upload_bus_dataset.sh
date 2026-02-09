#!/usr/bin/env bash

# Run this from the root project dir with scripts/upload_bus_dataset.sh
# Please look at the test file below and adjust the CSV_FILE_LOCATION and SCHEMA_NAME if needed.
# Additionally, you will have to uncomment the @pytest.mark.skip decorator.

# Also make sure you have run `uv sync --all-packages --group dev` before running this script.

uv run pytest soda-tests/tests/features/test_sql_upload_bus_breakdown.py::test_insert_bus_breakdown_dataset -o log_cli=true -o log_cli_level=DEBUG
