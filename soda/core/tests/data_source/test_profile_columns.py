from __future__ import annotations

import logging
import os
from numbers import Number

import pytest
from helpers.common_test_tables import (
    customers_profiling,
    customers_profiling_capitalized,
    orders_test_table,
)
from helpers.data_source_fixture import DataSourceFixture
from soda.common.yaml_helper import to_yaml_str
from soda.execution.check.profile_columns_run import ProfileColumnsRun


def test_profile_columns_numeric(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_profiling)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          profile columns:
            columns: [{table_name}.cst_size]
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
    size_column = column_profiles_by_name["cst_size"]
    size_profile = size_column["profile"]

    logging.debug(f"Size profile cloud dict: \n{to_yaml_str(size_profile)}")

    mins = size_profile["mins"]
    assert isinstance(mins, list)
    assert mins[0] == 0.5
    maxs = size_profile["maxs"]
    assert isinstance(maxs, list)
    assert maxs[0] == 6.1
    frequent_values = size_profile["frequent_values"]
    assert isinstance(frequent_values, list)
    for frequent_value in frequent_values:
        assert isinstance(frequent_value, dict)
        assert "value" in frequent_value
        assert "frequency" in frequent_value
    for numeric_stat in ["avg", "min", "max", "sum", "stddev", "variance", "distinct", "missing_count"]:
        v = size_profile[numeric_stat]
        assert isinstance(
            v, Number
        ), f"{numeric_stat} in profile of column 'cst_size' is not a number: {v} ({type(v).__name__})"
    histogram = size_profile["histogram"]
    boundaries = histogram["boundaries"]
    assert len(boundaries) > 0
    for b in boundaries:
        assert isinstance(b, float)
    frequencies = histogram["frequencies"]
    assert len(frequencies) > 0
    for f in frequencies:
        assert isinstance(f, int)


def test_profile_columns_text(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_profiling)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          profile columns:
            columns: [{table_name}.country]
        """
    )
    scan.execute(allow_warnings_only=True)
    scan_results = mock_soda_cloud.pop_scan_result()

    profilings = scan_results["profiling"]
    profiling = profilings[0]

    # remove the data source name because it's a pain to test
    profiling.pop("dataSource")
    profiling.pop("table")

    test_data_source = os.environ.get("test_data_source")

    uppercase_column_name_databases = ["snowflake", "db2", "oracle"]
    if test_data_source in uppercase_column_name_databases:
        expected_column_name = "COUNTRY"
    else:
        expected_column_name = "country"

    assert profiling == {
        "columnProfiles": [
            {
                "columnName": expected_column_name,
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


def test_profile_columns_all_tables_all_columns(data_source_fixture: DataSourceFixture):
    # Make sure that two tables are created in database
    customers_profiling_table_name = data_source_fixture.ensure_test_table(customers_profiling)
    customers_profiling_table_name = data_source_fixture.data_source.default_casify_table_name(
        customers_profiling_table_name
    )
    orders_test_table_name = data_source_fixture.ensure_test_table(orders_test_table)
    orders_test_table_name = data_source_fixture.data_source.default_casify_table_name(orders_test_table_name)

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        """
            profile columns:
                columns:
                  - "%.%"
        """
    )
    data_source_name = data_source_fixture.data_source_name
    data_source_scan = scan._get_or_create_data_source_scan(data_source_name)
    data_source = data_source_scan.data_source
    profiling_cfg = scan._sodacl_cfg.data_source_scan_cfgs[data_source_name].data_source_cfgs[0]
    include_columns = profiling_cfg.include_columns
    exclude_columns = profiling_cfg.exclude_columns

    profile_columns_run = ProfileColumnsRun(data_source_scan, profiling_cfg)

    table_names: list[str] = data_source.get_table_names(
        include_tables=profile_columns_run._get_table_expression(include_columns),
        exclude_tables=profile_columns_run._get_table_expression(exclude_columns, is_for_exclusion=True),
        query_name="profile-columns-get-table-names",
    )

    # Test only two tables
    assert customers_profiling_table_name in table_names
    assert orders_test_table_name in table_names

    parsed_included_tables_and_columns = profile_columns_run._build_column_expression_list(include_columns)
    assert parsed_included_tables_and_columns == {"%": ["%"]}

    parsed_excluded_tables_and_columns = profile_columns_run._build_column_expression_list(exclude_columns)
    assert parsed_excluded_tables_and_columns == {}

    customers_columns_metadata_result = data_source.get_table_columns(
        table_name=customers_profiling_table_name,
        query_name=f"profile-columns-get-column-metadata-for-{customers_profiling_table_name}",
        included_columns=parsed_included_tables_and_columns,
        excluded_columns=parsed_excluded_tables_and_columns,
    )
    assert len(customers_columns_metadata_result) == 12

    orders_columns_metadata_result = data_source.get_table_columns(
        table_name=orders_test_table_name,
        query_name=f"profile-columns-get-column-metadata-for-{orders_test_table_name}",
        included_columns=parsed_included_tables_and_columns,
        excluded_columns=parsed_excluded_tables_and_columns,
    )
    assert len(orders_columns_metadata_result) == 6


@pytest.mark.parametrize(
    "table_name, soda_cl_str, expectation",
    [
        pytest.param(
            customers_profiling,
            """
                profile columns:
                    columns:
                        - include {table_name}.%
                        - include %.cst_size
                        - exclude %.country
                        - exclude %.id
            """,
            "",
        ),
        pytest.param(
            customers_profiling,
            """
                profile columns:
                    columns:
                        - include %.%
                        - exclude %.id
            """,
            "all but id",
            id="all tables and cols except for id",
        ),
        pytest.param(
            customers_profiling,
            """
                profile columns:
                    columns:
                        - include %.%
                        - exclude %.%
            """,
            "all but id",
            id="all tables and columns included and then all excluded",
        ),
        pytest.param(
            customers_profiling,
            """
                profile columns:
                    columns:
                        - include {table_name}.%
                        - include %.si%
                        - exclude %.country
                        - exclude %.id
            """,
            "",
            id="all tables with 'si' like columns should have profile",
        ),
    ],
)
def test_profile_columns_inclusions_exclusions(
    data_source_fixture: DataSourceFixture, table_name, soda_cl_str, expectation
):
    _table_name = data_source_fixture.ensure_test_table(table_name)
    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(soda_cl_str.format(table_name=_table_name))
    # TODO: we should only allow warnings here, we'll have to look at what the errors were
    # it is most likely will be related to https://sodadata.atlassian.net/browse/CLOUD-155
    scan.execute(allow_error_warning=True)
    profiling_result = mock_soda_cloud.pop_scan_result()
    for table_result in profiling_result["profiling"]:
        if table_result["table"].lower().startswith("sodatest_customer"):
            column_names = [col_profile["columnName"] for col_profile in table_result["columnProfiles"]]
            if expectation == "all but id":
                assert "id" not in column_names
            elif expectation == "all but nothing":
                assert len(column_names) == 0
            else:
                assert "id" not in column_names
                assert "cst_size" in column_names
                assert "country" not in column_names


def test_profile_columns_quotes_error(data_source_fixture: DataSourceFixture):
    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
        profile columns:
            columns:
                - include "%.%"
                - exclude "something.else"
        """
    )
    scan.execute(allow_error_warning=True)
    scan_results = mock_soda_cloud.pop_scan_result()
    character_log_warnings = [
        x
        for x in scan_results["logs"]
        if "It looks like quote characters are present" in x["message"] and x["level"] == "error"
    ]
    assert len(character_log_warnings) == 2


def test_profile_columns_invalid_format(data_source_fixture: DataSourceFixture):
    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
        profile columns:
            columns:
                - "invalid%"
        """
    )
    scan.execute(allow_error_warning=True)
    scan_results = mock_soda_cloud.pop_scan_result()
    assert scan_results["hasErrors"]
    character_log_warnings = [
        x
        for x in scan_results["logs"]
        if "Invalid column expression: invalid% - must be in the form of table.column" in x["message"]
    ]
    assert len(character_log_warnings) == 1


def test_profile_columns_no_table_or_column(data_source_fixture: DataSourceFixture):
    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
        profile columns:
            columns:
        """
    )
    scan.execute(allow_error_warning=True)
    scan_results = mock_soda_cloud.pop_scan_result()
    assert scan_results["hasErrors"]
    character_log_warnings = [
        x for x in scan_results["logs"] if 'Configuration key "columns" is required in profile columns' in x["message"]
    ]
    assert len(character_log_warnings) == 1


def test_profile_columns_capitalized(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_profiling_capitalized)
    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          profile columns:
            columns: [{table_name}.%]
        """
    )
    scan.execute(allow_warnings_only=True)
    scan_results = mock_soda_cloud.pop_scan_result()

    profilings = scan_results["profiling"]
    profiling = profilings[0]

    column_profiles = profiling["columnProfiles"]

    test_data_source = os.environ.get("test_data_source")

    lowercase_column_name_databases = ["postgres", "redshift", "athena"]
    if test_data_source in lowercase_column_name_databases:
        expected_column_name = "items_sold"
    else:
        expected_column_name = "ITEMS_SOLD"

    assert len(column_profiles) == 1
    assert column_profiles[0]["columnName"] == expected_column_name
