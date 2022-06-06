from __future__ import annotations

import logging
from numbers import Number

import pytest
from soda.common.yaml_helper import to_yaml_str
from tests.helpers.common_test_tables import customers_profiling
from tests.helpers.fixtures import test_data_source
from tests.helpers.scanner import Scanner


@pytest.mark.skipif(
    test_data_source == "athena",
    reason="TODO: fix for athena.",
)
def test_profile_columns_numeric(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_profiling)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          profile columns:
            columns: [{table_name}.%]
        """
    )
    scan.execute(allow_warnings_only=True)
    # remove the data source name because it's a pain to test
    scan_result = mock_soda_cloud.pop_scan_result()
    assert scan_result
    profiling = scan_result["profiling"]
    assert profiling
    first_profiling = profiling[0]
    column_profiles = first_profiling["columnProfiles"]
    column_profiles_by_name = {
        column_profile["columnName"].lower(): column_profile for column_profile in column_profiles
    }
    size_column = column_profiles_by_name["size"]
    size_profile = size_column["profile"]

    logging.debug(f"Size profile cloud dict: \n{to_yaml_str(size_profile)}")

    assert isinstance(size_profile["mins"], list)
    assert isinstance(size_profile["maxs"], list)
    for frequent_value in size_profile["frequent_values"]:
        assert isinstance(frequent_value, dict)
        assert "value" in frequent_value
        assert "frequency" in frequent_value
    for numeric_stat in ["avg", "min", "max", "sum", "stddev", "variance", "distinct", "missing_count"]:
        v = size_profile[numeric_stat]
        assert isinstance(
            v, Number
        ), f"{numeric_stat} in profile of column 'size' is not a number: {v} ({type(v).__name__})"
    assert isinstance(size_profile["histogram"], dict)


@pytest.mark.skipif(
    test_data_source == "athena",
    reason="TODO: fix for athena.",
)
def test_profile_columns_numeric2(scanner: Scanner, table_name, soda_cl_str, cloud_dict_expectation):
    table_name = scanner.ensure_test_table(table_name)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          profile columns:
            columns: [{table_name}{soda_cl_str}]
        """
    )
    scan.execute(allow_warnings_only=True)
    # remove the data source name because it's a pain to test
    profiling_result = mock_soda_cloud.pop_scan_result()
    assert profiling_result
    profiling_result = remove_datasource_and_table_name(profiling_result)
    assert len(profiling_result["profiling"]) > 0
    assert len(profiling_result["profiling"][0]["columnProfiles"]) > 0
    assert profiling_result["profiling"][0]["columnProfiles"][0]["columnName"].lower() == "size"
    assert (
        profiling_result["profiling"][0]["columnProfiles"][0]["profile"]["frequent_values"]
        == cloud_dict_expectation["profiling"][0]["columnProfiles"][0]["profile"]["frequent_values"]
    )
    assert (
        profiling_result["profiling"][0]["columnProfiles"][0]["profile"]["histogram"]
        == cloud_dict_expectation["profiling"][0]["columnProfiles"][0]["profile"]["histogram"]
    )


@pytest.mark.skipif(
    test_data_source == "athena",
    reason="TODO: fix for athena.",
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
                        "columnProfiles": [
                            {
                                "columnName": "country",
                                "profile": {
                                    "mins": None,
                                    "maxs": None,
                                    "min": None,
                                    "min_length": 2,
                                    "max": None,
                                    "max_length": 2,
                                    "frequent_values": [
                                        {"value": "BE", "frequency": 6},
                                        {"value": "NL", "frequency": 4},
                                    ],
                                    "avg": None,
                                    "avg_length": 2,
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
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          profile columns:
            columns: [{table_name}{soda_cl_str}]
        """
    )
    scan.execute(allow_warnings_only=True)
    profiling_result = mock_soda_cloud.pop_scan_result()
    # remove the data source name because it's a pain to test
    profiling_result = remove_datasource_and_table_name(profiling_result)

    assert profiling_result["profiling"] == cloud_dict_expectation["profiling"]


@pytest.mark.skipif(
    test_data_source == "athena",
    reason="TODO: fix for athena.",
)
def test_profile_columns_all_tables_all_columns(scanner: Scanner):
    _ = scanner.ensure_test_table(customers_profiling)
    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        """
            profile columns:
                columns: ["%.%"]
        """
    )
    scan.execute(allow_warnings_only=True)
    profiling_result = mock_soda_cloud.pop_scan_result()
    assert len(profiling_result["profiling"]) >= 5


def remove_datasource_and_table_name(results_dict: dict) -> dict:
    for i, _ in enumerate(results_dict["profiling"]):
        del results_dict["profiling"][i]["dataSource"]
        del results_dict["profiling"][i]["table"]
    return results_dict
