import pytest
from tests.helpers.common_test_tables import customers_profiling
from tests.helpers.scanner import Scanner


@pytest.mark.parametrize(
    "table_name, soda_cl_str, cloud_dict_expectation",
    [
        pytest.param(
            customers_profiling,
            ".%",
            {
                "definitionName": "test_profile_columns.py::test_profile_columns[customer table all numric columns]",
                "dataTimestamp": "2022-04-29T08:41:19.772904",
                "scanStartTimestamp": "2022-04-29T08:41:19.772904",
                "scanEndTimestamp": "2022-04-29T08:41:19.865563",
                "hasErrors": False,
                "hasWarnings": False,
                "hasFailures": False,
                "metrics": [],
                "checks": [],
                "profiling": [
                    {
                        "table": "sodatest_customers_profiling_f15d5223",
                        "dataSource": "postgres",
                        "columnProfiles": [
                            {
                                "columnName": "size",
                                "profile": {
                                    "mins": [0.5, 1.0, 6.0],
                                    "maxs": [6.0, 1.0, 0.5],
                                    "min": 0.5,
                                    "max": 6.0,
                                    "frequent_values": [
                                        {"value": "None", "frequency": 4},
                                        {"value": "0.5", "frequency": 3},
                                        {"value": "6.0", "frequency": 2},
                                    ],
                                    "avg": 2.41666666666667,
                                    "sum": 14.5,
                                    "stddev": 2.7823850680067,
                                    "variance": 7.74166666666667,
                                    "distinct": 3,
                                    "missing_count": 4,
                                    "histogram": {
                                        "boundaries": [
                                            0.5,
                                            0.775,
                                            1.05,
                                            1.325,
                                            1.6,
                                            1.875,
                                            2.15,
                                            2.425,
                                            2.7,
                                            2.975,
                                            3.25,
                                            3.525,
                                            3.8,
                                            4.075,
                                            4.35,
                                            4.625,
                                            4.9,
                                            5.175,
                                            5.45,
                                            5.725,
                                            6.0,
                                        ],
                                        "frequencies": [3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
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
                                        {"value": "10", "frequency": 3},
                                        {"value": "999", "frequency": 3},
                                        {"value": "None", "frequency": 2},
                                        {"value": "-999", "frequency": 1},
                                    ],
                                    "avg": 253.625,
                                    "sum": 2029,
                                    "stddev": 704.850528734386,
                                    "variance": 496814.26785714284,
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
                                        "frequencies": [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 3],
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
def test_profile_columns_numeric(scanner: Scanner, table_name, soda_cl_str, cloud_dict_expectation):
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
    assert profiling_result
    assert len(profiling_result["profiling"]) > 0
    assert len(profiling_result["profiling"][0]["columnProfiles"]) > 0
    assert profiling_result["profiling"][0]["columnProfiles"][0]["columnName"] == "size"
    assert (
        profiling_result["profiling"][0]["columnProfiles"][0]["profile"]["frequent_values"]
        == cloud_dict_expectation["profiling"][0]["columnProfiles"][0]["profile"]["frequent_values"]
    )
    assert (
        profiling_result["profiling"][0]["columnProfiles"][0]["profile"]["histogram"]
        == cloud_dict_expectation["profiling"][0]["columnProfiles"][0]["profile"]["histogram"]
    )


@pytest.mark.parametrize(
    "table_name, soda_cl_str, cloud_dict_expectation",
    [
        pytest.param(
            customers_profiling,
            ".country",
            {
                "definitionName": "test_profile_columns.py::test_profile_columns_text[table_name0-.country-cloud_dict_expectation0]",
                "dataTimestamp": "2022-04-29T14:32:57.111198",
                "scanStartTimestamp": "2022-04-29T14:32:57.111198",
                "scanEndTimestamp": "2022-04-29T14:32:57.197097",
                "hasErrors": False,
                "hasWarnings": False,
                "hasFailures": False,
                "metrics": [],
                "checks": [],
                "profiling": [
                    {
                        "table": "sodatest_customers_profiling_31fec4d4",
                        "dataSource": "postgres",
                        "columnProfiles": [
                            {
                                "columnName": "country",
                                "profile": {
                                    "mins": None,
                                    "maxs": None,
                                    "min": None,
                                    "max": None,
                                    "frequent_values": [
                                        {"value": "BE", "frequency": 6},
                                        {"value": "NL", "frequency": 4},
                                    ],
                                    "avg": None,
                                    "sum": None,
                                    "stddev": None,
                                    "variance": None,
                                    "distinct": 2,
                                    "missing_count": 0,
                                    "histogram": None,
                                },
                            }
                        ],
                    }
                ],
            },
            id="customer.country (text only)",
        )
    ],
)
def test_profile_columns_text(scanner: Scanner, table_name, soda_cl_str, cloud_dict_expectation):
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
