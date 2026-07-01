"""Phase 5B — basic row_count contract verification under @pytest.mark.memory_container.

Adapted from soda-tests/tests/integration/test_row_count_check.py::test_row_count.
Smallest possible E2E DB-touching test: 3-row table on the host's postgres
(reached via host.docker.internal:5432), simple row_count check, asserts pass.

Goal: prove the full pipeline works end-to-end inside the memory-capped container —
plugin loads, conftest chain imports, DataSourceTestHelper connects to postgres,
contract verification runs, peak is measured. Memory cap is intentionally generous
(256 MB) since we're not stressing it; raise on regressions later.
"""

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification

# Module-level: builder is cheap, lets the conftest collect the test without
# requiring a connection.
test_table_specification_small = (
    TestTableSpecification.builder()
    .table_purpose("memory_row_count_small")
    .column_varchar("id")
    .rows(rows=[("1",), ("2",), ("3",)])
    .build()
)


@pytest.mark.memory_container(limit_mb=256)
def test_row_count_small(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification_small)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - row_count:
        """,
    )
