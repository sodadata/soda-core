from __future__ import annotations

import pytest
from tests.helpers.common_test_tables import orders_test_table
from tests.helpers.scanner import Scanner


@pytest.mark.parametrize(
    "cloud_dict_expectation",
    [
        pytest.param(
            {
                "postgres": {
                    "table": "sodatest_orders_f7532be6",
                    "dataSource": "postgres",
                    "sampleFile": {
                        "columns": [
                            {"name": "id", "type": "varchar"},
                            {"name": "customer_id_nok", "type": "varchar"},
                            {"name": "customer_id_ok", "type": "varchar"},
                            {"name": "customer_country", "type": "varchar"},
                            {"name": "customer_zip", "type": "varchar"},
                            {"name": "text", "type": "varchar"},
                        ],
                        "totalRowCount": 7,
                        "storedRowCount": 7,
                        "reference": {"type": "sodaCloudStorage", "fileId": "file-0"},
                    },
                },
                "athena": {
                    "table": "sodatest_orders_f7532be6",
                    "dataSource": "athena",
                    "sampleFile": {
                        "columns": [
                            {"name": "id", "type": "varchar"},
                            {"name": "customer_id_nok", "type": "varchar"},
                            {"name": "customer_id_ok", "type": "varchar"},
                            {"name": "customer_country", "type": "varchar"},
                            {"name": "customer_zip", "type": "varchar"},
                            {"name": "text", "type": "varchar"},
                        ],
                        "totalRowCount": 7,
                        "storedRowCount": 7,
                        "reference": {"type": "sodaCloudStorage", "fileId": "file-0"},
                    },
                },
                "snowflake": {
                    "table": "SODATEST_ORDERS_F7532BE6",
                    "dataSource": "snowflake",
                    "sampleFile": {
                        "columns": [
                            {"name": "ID", "type": "2"},
                            {"name": "CUSTOMER_ID_NOK", "type": "2"},
                            {"name": "CUSTOMER_ID_OK", "type": "2"},
                            {"name": "CUSTOMER_COUNTRY", "type": "2"},
                            {"name": "CUSTOMER_ZIP", "type": "2"},
                            {"name": "TEXT", "type": "2"},
                        ],
                        "totalRowCount": 7,
                        "storedRowCount": 7,
                        "reference": {"type": "sodaCloudStorage", "fileId": "file-0"},
                    },
                },
            },
            id="orders table only",
        )
    ],
)
def test_sample_tables(cloud_dict_expectation: dict, scanner: Scanner):
    table_name = scanner.ensure_test_table(orders_test_table)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          sample datasets:
            tables:
                - include {table_name}
        """
    )
    scan.execute()
    # remove the data source name because it's a pain to test
    discover_tables_result = mock_soda_cloud.pop_scan_result()

    assert discover_tables_result is not None
    assert discover_tables_result["profiling"][0] == cloud_dict_expectation[scan._data_source_name]


@pytest.mark.parametrize(
    "soda_cl_str, expectation",
    [
        pytest.param(
            """
                - include sodatest_customers_%
            """,
            3,
            id="include all soda test customer tables",
        ),
        pytest.param(
            """
               - include sodatest_cust%
               - exclude sodatest_customersdist_%
            """,
            2,
            id="exclude customers dist",
        ),
    ],
)
def test_discover_tables_customer_wildcard(scanner: Scanner, soda_cl_str, expectation):
    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          sample datasets:
            tables:
                {soda_cl_str}
        """
    )
    scan.execute()
    discover_tables_result = mock_soda_cloud.pop_scan_result()
    assert discover_tables_result is not None
    assert len(discover_tables_result["profiling"]) == expectation
