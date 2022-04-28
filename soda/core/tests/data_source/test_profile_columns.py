import pytest
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


@pytest.mark.parametrize(
    "table_name, soda_cl_str, distance_column_exp, size_column_exp",
    [
        pytest.param(
            customers_test_table,
            "%.%",
            {
                "column_name": "distance",
                "mins": [-999, 0, 5, 10, 999],
                "maxes": [999, 10, 5, 0, -999],
                "min": -999,
                "max": 999,
                "frequent_values": [
                    {"value": "999", "frequency": 5},
                    {"value": "None", "frequency": 1},
                    {"value": "10", "frequency": 1},
                    {"value": "0", "frequency": 1},
                    {"value": "-999", "frequency": 1},
                ],
                "average": 445.6666666666667,
                "standard_deviation": 724.61731279345,
                "sum": 4011,
                "variance": 525070.25,
                "distinct_values": 5,
                "missing_values": 1,
            },
            {
                "column_name": "size",
                "mins": [-3.0, -1.2, -0.4, 0.5, 1.0],
                "maxes": [6.0, 5.0, 1.0, 0.5, -0.4],
                "min": -3.0,
                "max": 6.0,
                "frequent_values": [
                    {"value": "None", "frequency": 3},
                    {"value": "5.0", "frequency": 1},
                    {"value": "0.5", "frequency": 1},
                    {"value": "-3.0", "frequency": 1},
                    {"value": "-1.2", "frequency": 1},
                ],
                "average": 1.12857142857143,
                "standard_deviation": 3.26430915902803,
                "sum": 7.9,
                "variance": 10.6557142857143,
                "distinct_values": 7,
                "missing_values": 3,
            },
            id="customer table all numric columns",
        )
    ],
)
def test_profile_columns(scanner: Scanner, table_name, soda_cl_str, distance_column_exp, size_column_exp):
    table_name = scanner.ensure_test_table(table_name)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.activate_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          profile columns:
            columns: [{table_name}{soda_cl_str}]
        """
    )
    scan.execute()

    assert len(scan._profile_columns_result_tables[0].result_columns) == 2
    assert scan._profile_columns_result_tables[0].result_columns[1].__dict__ == distance_column_exp
    assert scan._profile_columns_result_tables[0].result_columns[0].__dict__ == size_column_exp

    profiling_result = mock_soda_cloud.scan_result_dicts
    # customer_column_profiles = profiling_result[0]["columnProfiles"]
    # distance_profile = customer_column_profiles[1]

    # assert distance_profile["columnName"] == "distance"
    # assert distance_profile["profile"]["mins"] == [-999, 0, 5, 10, 999, None]
