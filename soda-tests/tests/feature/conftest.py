import os

import pytest

# Feature tests are only run against postgres in CI (see .github/workflows/main.workflow.yaml).
# Skip them for other data sources to avoid confusing failures when running locally.
_datasource = os.environ.get("TEST_DATASOURCE", "postgres")


def pytest_collection_modifyitems(config, items):
    if _datasource != "postgres":
        skip = pytest.mark.skip(reason=f"Feature tests only run against postgres (TEST_DATASOURCE={_datasource})")
        for item in items:
            item.add_marker(skip)
