from __future__ import annotations

import logging
import os
from collections import defaultdict
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
    for numeric_stat in [
        "avg",
        "min",
        "max",
        "sum",
        "stddev",
        "variance",
        "distinct",
        "missing_count",
    ]:
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

    tables_columns_metadata: defaultdict[str, dict[str, str]] = data_source.get_tables_columns_profiling(
        include_patterns=profile_columns_run.parse_profiling_expressions(include_columns),
        exclude_patterns=profile_columns_run.parse_profiling_expressions(exclude_columns),
        query_name="profile-columns-get-table-and-column-metadata",
    )

    # Test only two tables
    assert customers_profiling_table_name in tables_columns_metadata
    assert orders_test_table_name in tables_columns_metadata

    customers_columns_metadata_result = tables_columns_metadata.get(customers_profiling_table_name)
    orders_columns_metadata_result = tables_columns_metadata.get(orders_test_table_name)

    assert len(customers_columns_metadata_result) == 12
    assert len(orders_columns_metadata_result) == 6


@pytest.mark.parametrize(
    "soda_cl_str, expected_column_profiling_results",
    [
        pytest.param(
            """
                profile columns:
                    columns:
                        - include {table_name1}.%
                        - include {table_name2}.%
                        - exclude %.country
                        - exclude %.id
            """,
            {
                "table_name1": [
                    "cst_size",
                    "distance",
                    "cst_size_txt",
                    "pct",
                    "cat",
                    "zip",
                    "email",
                ],
                "table_name2": ["ITEMS_SOLD", "CST_SIZE"],
            },
            id="all tables and columns except for country and id",
        ),
        pytest.param(
            """
                profile columns:
                    columns:
                        - include %Profiling%.%
                        - exclude %.id
            """,
            {
                "table_name1": [
                    "cst_size",
                    "distance",
                    "cst_size_txt",
                    "pct",
                    "cat",
                    "country",
                    "zip",
                    "email",
                ],
                "table_name2": ["ITEMS_SOLD", "CST_SIZE"],
            },
            id="all tables with names that contain 'profiling' and columns except for id",
        ),
        pytest.param(
            """
                profile columns:
                    columns:
                        - include %.%
                        - exclude %.%
            """,
            {},
            id="no tables included",
        ),
        pytest.param(
            """
                profile columns:
                    columns:
                        - include %Profiling%.%si%
            """,
            {
                "column_case_insensitive": {
                    "table_name1": ["cst_size", "cst_size_txt"],
                    "table_name2": ["CST_SIZE"],
                },
                "column_case_sensitive": {
                    "table_name1": ["cst_size", "cst_size_txt"],
                },
            },
            id="'si' like columns in tables with names that contain 'profiling'",
        ),
        pytest.param(
            """
                profile columns:
                    columns:
                        - include {table_name1}.%si%
                        - exclude {table_name1}.%txt
            """,
            {"table_name1": ["cst_size"]},
            id="include 'si' like columns exclude 'txt' like columns",
        ),
        pytest.param(
            """
                profile columns:
                    columns:
                        - include %Capitalized%.%si%
            """,
            {
                "column_case_insensitive": {"table_name1": ["CST_SIZE"]},
                "column_case_sensitive": {},
            },
            id="use wildcard '%' in both table and column name",
        ),
        pytest.param(
            r"""
                profile columns:
                    columns:
                        - include %Profiling\_%.country
                        - include %Profiling\_%.%si%
            """,
            {
                "table_name1": [
                    "cst_size",
                    "cst_size_txt",
                    "country",
                ],
            },
            id="tables with 'profiling' and column_name 'country' or like '%si",
        ),
    ],
)
def test_profile_columns_inclusions_exclusions(
    data_source_fixture: DataSourceFixture, soda_cl_str, expected_column_profiling_results
):
    _table_name1 = data_source_fixture.ensure_test_table(customers_profiling)
    _table_name2 = data_source_fixture.ensure_test_table(customers_profiling_capitalized)
    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(soda_cl_str.format(table_name1=_table_name1, table_name2=_table_name2))
    # TODO: we should only allow warnings here, we'll have to look at what the errors were
    # it is most likely will be related to https://sodadata.atlassian.net/browse/CLOUD-155
    scan.execute(allow_error_warning=True)
    scan_results = mock_soda_cloud.pop_scan_result()

    profiled_tables = [profiled_table for profiled_table in scan_results["profiling"]]
    profiled_tables = sorted(profiled_tables, key=lambda x: x["table"].lower())

    column_profiling_results = {
        f"table_name{index}": list(map(lambda x: x["columnName"], profiled_table["columnProfiles"]))
        for index, profiled_table in enumerate(profiled_tables, 1)
    }

    test_data_source = os.environ.get("test_data_source")

    if "column_case_sensitive" in expected_column_profiling_results:
        if test_data_source == "duckdb":
            expected_column_profiling_results = expected_column_profiling_results["column_case_sensitive"]
        else:
            expected_column_profiling_results = expected_column_profiling_results["column_case_insensitive"]

    uppercase_column_name_databases = ["snowflake", "db2", "oracle"]
    lowercase_column_name_databases = ["postgres", "redshift", "athena"]

    test_data_source_uppercase = test_data_source in uppercase_column_name_databases
    test_data_source_lowercase = test_data_source in lowercase_column_name_databases

    if test_data_source_uppercase or test_data_source_lowercase:
        casify_function = lambda x: x.upper() if test_data_source_uppercase else x.lower()
        expected_column_profiling_results = {
            table_name: [casify_function(column_name) for column_name in column_names]
            for table_name, column_names in expected_column_profiling_results.items()
        }

    assert column_profiling_results == expected_column_profiling_results


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
        expected_column_name1 = "items_sold"
        expected_column_name2 = "cst_size"
    else:
        expected_column_name1 = "ITEMS_SOLD"
        expected_column_name2 = "CST_SIZE"

    assert len(column_profiles) == 2
    assert column_profiles[0]["columnName"] == expected_column_name1
    assert column_profiles[1]["columnName"] == expected_column_name2
