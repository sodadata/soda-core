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
# @pytest.mark.skipif(
#     test_data_source == "spark_df",
#     reason="TODO: fix for spark_df.",
# )
def test_profile_columns_numeric(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_profiling)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          profile columns:
            columns: [{table_name}.size]
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
        ), f"{numeric_stat} in profile of column 'size' is not a number: {v} ({type(v).__name__})"
    histogram = size_profile["histogram"]
    boundaries = histogram["boundaries"]
    assert len(boundaries) > 0
    for b in boundaries:
        assert isinstance(b, float)
    frequencies = histogram["frequencies"]
    assert len(frequencies) > 0
    for f in frequencies:
        assert isinstance(f, int)


@pytest.mark.skipif(
    test_data_source == "athena",
    reason="TODO: fix for athena.",
)
# @pytest.mark.skipif(
#     test_data_source == "spark_df",
#     reason="TODO: fix for spark_df.",
# )
def test_profile_columns_text(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_profiling)

    scan = scanner.create_test_scan()
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

    if scan._data_source_name == "spark_df":
        # TODO when we fix spark and add columns inclusions we should be able to make this work correctly.
        pass
    else:
        assert profiling == {
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


@pytest.mark.skipif(
    test_data_source == "athena",
    reason="TODO: fix for athena.",
)
# @pytest.mark.skipif(
#     test_data_source == "spark_df",
#     reason="TODO: fix for spark_df.",
# )
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


@pytest.mark.skipif(
    test_data_source == "athena",
    reason="TODO: fix for athena.",
)
@pytest.mark.parametrize(
    "table_name, soda_cl_str, expectation",
    [
        pytest.param(
            customers_profiling,
            """
                profile columns:
                    columns:
                        - include {table_name}.%
                        - include %.size
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
def test_profile_columns_inclusions_exclusions(scanner: Scanner, table_name, soda_cl_str, expectation):
    _table_name = scanner.ensure_test_table(table_name)
    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(soda_cl_str.format(table_name=_table_name))
    # TODO: we should only allow warnings here, we'll have to look at what the errors were
    # it is most likely will be related to https://sodadata.atlassian.net/browse/CLOUD-155
    scan.execute(allow_error_warning=True)
    profiling_result = mock_soda_cloud.pop_scan_result()
    if scan._data_source_name == "spark_df":
        # we deliberately have to not test this for spark as column inclusion and exclusion is unsupported
        pass
    else:
        for table_result in profiling_result["profiling"]:
            if table_result["table"].lower().startswith("sodatest_customer"):
                column_names = [col_profile["columnName"] for col_profile in table_result["columnProfiles"]]
                if expectation == "all but id":
                    assert "id" not in column_names
                elif expectation == "all but nothing":
                    assert len(column_names) == 0
                else:
                    assert "id" not in column_names
                    assert "size" in column_names
                    assert "country" not in column_names
