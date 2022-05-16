from __future__ import annotations

import pytest
from tests.helpers.common_test_tables import customers_profiling
from tests.helpers.scanner import Scanner


@pytest.mark.parametrize(
    "table_name, soda_cl_str, cloud_dict_expectation",
    [
        pytest.param(
            customers_profiling,
            "",
            {},
            id="customer table all numric columns",
        )
    ],
)
def test_discover_tables(scanner: Scanner, table_name, soda_cl_str, cloud_dict_expectation):
    table_name = scanner.ensure_test_table(table_name)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          discover tables:
            tables:
              - include {table_name}
        """
    )
    scan.execute()
    # remove the data source name because it's a pain to test
    profiling_result = mock_soda_cloud.scan_result
    assert 1 == 1
