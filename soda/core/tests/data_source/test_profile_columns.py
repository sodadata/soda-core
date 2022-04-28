import pytest
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


@pytest.mark.parametrize(
    "table_name, soda_cl_str, cloud_dict_expectation",
    [
        pytest.param(
            customers_test_table,
            "%.%",
            [
                {
                    "definitionName": "test_profile_columns.py::test_profile_columns[customer table all numric columns]",
                    "profiling": [
                        {
                            "table": "sodatest_customers_b7580920",
                            "dataSource": "postgres",
                            "columnProfiles": [
                                {
                                    "columnName": "size",
                                    "profile": {
                                        "mins": [-3.0, -1.2, -0.4, 0.5, 1.0],
                                        "maxs": [6.0, 5.0, 1.0, 0.5, -0.4],
                                        "min": -3.0,
                                        "max": 6.0,
                                        "frequent_values": [
                                            {"value": "None", "frequency": 3},
                                            {"value": "5.0", "frequency": 1},
                                            {"value": "0.5", "frequency": 1},
                                            {"value": "-3.0", "frequency": 1},
                                            {"value": "-1.2", "frequency": 1},
                                        ],
                                        "avg": 1.12857142857143,
                                        "sum": 7.9,
                                        "stddev": 3.26430915902803,
                                        "variance": 10.6557142857143,
                                        "distinct": 7,
                                        "missing_count": 3,
                                        "histogram": {
                                            "boundaries": [
                                                -3.0,
                                                -2.55,
                                                -2.1,
                                                -1.65,
                                                -1.2,
                                                -0.75,
                                                -0.3,
                                                0.15,
                                                0.6,
                                                1.05,
                                                1.5,
                                                1.95,
                                                2.4,
                                                2.85,
                                                3.3,
                                                3.75,
                                                4.2,
                                                4.65,
                                                5.1,
                                                5.55,
                                                6.0,
                                            ],
                                            "frequencies": [1, 0, 0, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1],
                                        },
                                    },
                                },
                                {
                                    "columnName": "distance",
                                    "profile": {
                                        "mins": [-999, 0, 5, 10, 999],
                                        "maxs": [999, 10, 5, 0, -999],
                                        "min": -999,
                                        "max": 999,
                                        "frequent_values": [
                                            {"value": "999", "frequency": 5},
                                            {"value": "None", "frequency": 1},
                                            {"value": "10", "frequency": 1},
                                            {"value": "0", "frequency": 1},
                                            {"value": "-999", "frequency": 1},
                                        ],
                                        "avg": 445.6666666666667,
                                        "sum": 4011,
                                        "stddev": 724.61731279345,
                                        "variance": 525070.25,
                                        "distinct": 5,
                                        "missing_count": 1,
                                        "histogram": {
                                            "boundaries": [
                                                -999.0,
                                                -899.1,
                                                -799.2,
                                                -699.3,
                                                -599.4,
                                                -499.5,
                                                -399.6,
                                                -299.7,
                                                -199.8,
                                                -99.9,
                                                -0.0,
                                                99.9,
                                                199.8,
                                                299.7,
                                                399.6,
                                                499.5,
                                                599.4,
                                                699.3,
                                                799.2,
                                                899.1,
                                                999.0,
                                            ],
                                            "frequencies": [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 5],
                                        },
                                    },
                                },
                            ],
                        }
                    ],
                }
            ],
            id="customer table all numric columns",
        )
    ],
)
def test_profile_columns(scanner: Scanner, table_name, soda_cl_str, cloud_dict_expectation):
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
    profiling_result = mock_soda_cloud.scan_result

    assert profiling_result["definitionName"] == cloud_dict_expectation[0]["definitionName"]
    assert profiling_result["profiling"] == cloud_dict_expectation[0]["profiling"]
