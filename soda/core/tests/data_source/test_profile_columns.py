import pytest
from tests.helpers.common_test_tables import customers_profiling
from tests.helpers.scanner import Scanner


@pytest.mark.parametrize(
    "table_name, soda_cl_str, cloud_dict_expectation",
    [
        pytest.param(
            customers_profiling,
            "%.%",
            {
                "definitionName": "test_profile_columns.py::test_profile_columns[customer table all numric columns]",
                "profiling": [
                    {
                        "table": "sodatest_customers_profiling_579b10e8",
                        "dataSource": "postgres",
                        "columnProfiles": [
                            {
                                "columnName": "size",
                                "profile": {
                                    "mins": [-3.0, 0.5, 1.0, 6.0],
                                    "maxs": [6.0, 1.0, 0.5, -3.0],
                                    "min": -3.0,
                                    "max": 6.0,
                                    "frequent_values": [
                                        {"value": "None", "frequency": 4},
                                        {"value": "0.5", "frequency": 3},
                                        {"value": "6.0", "frequency": 2},
                                        {"value": "-3.0", "frequency": 1},
                                    ],
                                    "avg": 1.64285714285714,
                                    "sum": 11.5,
                                    "stddev": 3.26233921333407,
                                    "variance": 10.6428571428571,
                                    "distinct": 4,
                                    "missing_count": 4,
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
                                        "frequencies": [1, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
                                    },
                                },
                            },
                            {
                                "columnName": "distance",
                                "profile": {
                                    "mins": [-999, 1, 10, 999],
                                    "maxs": [999, 10, 1, -999],
                                    "min": -999,
                                    "max": 999,
                                    "frequent_values": [
                                        {"value": "999", "frequency": 4},
                                        {"value": "10", "frequency": 3},
                                        {"value": "None", "frequency": 2},
                                        {"value": "-999", "frequency": 1},
                                    ],
                                    "avg": 336.44444444444446,
                                    "sum": 3028,
                                    "stddev": 704.587842485078,
                                    "variance": 496444.02777777775,
                                    "distinct": 4,
                                    "missing_count": 2,
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
                                        "frequencies": [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 4],
                                    },
                                },
                            },
                        ],
                    }
                ],
            },
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

    assert profiling_result["profiling"] == cloud_dict_expectation["profiling"]
    # assert profiling_result["profiling"][0]["columnProfiles"][0]["columnName"] == "size"
    # assert (
    # profiling_result["profiling"][0]["columnProfiles"][0]["profile"]["frequent_values"]
    # == cloud_dict_expectation["profiling"][0]["columnProfiles"][0]["profile"]["frequent_values"]
    # )
    # assert (
    # profiling_result["profiling"][0]["columnProfiles"][0]["profile"]["histogram"]
    # == cloud_dict_expectation["profiling"][0]["columnProfiles"][0]["profile"]["histogram"]
    # )
