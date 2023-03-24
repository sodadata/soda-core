from textwrap import dedent

import pytest
from cli.run_cli import run_cli
from helpers.common_test_tables import (
    customers_test_table,
    dro_categorical_test_table,
    null_test_table,
)
from helpers.data_source_fixture import DataSourceFixture
from helpers.fixtures import test_data_source
from helpers.mock_file_system import MockFileSystem
from ruamel.yaml import YAML


def mock_file_system_and_run_cli(
    user_home_dir, mock_file_system, data_source_fixture, table_name, column_name, distribution_type
):
    mock_file_system.files = {
        f"{user_home_dir}/configuration.yml": data_source_fixture.create_test_configuration_yaml_str(),
        f"{user_home_dir}/customers_distribution_reference.yml": dedent(
            f"""
                table: {table_name}
                column: {column_name}
                distribution_type: {distribution_type}
            """
        ),
    }

    run_cli_output = run_cli(
        [
            "update-dro",
            "-c",
            "configuration.yml",
            "-d",
            data_source_fixture.data_source.data_source_name,
            f"{user_home_dir}/customers_distribution_reference.yml",
        ]
    )

    return run_cli_output


@pytest.mark.skipif(
    test_data_source not in ["postgres", "snowflake", "bigquery", "mysql"],
    reason="Support for other data sources is experimental and not tested",
)
@pytest.mark.parametrize(
    "column_name, distribution_type, expected_bins_and_weights",
    [
        pytest.param(
            "cst_size",
            "continuous",
            [
                (-3.0, 0.0),
                (-0.75, 0.2857142857142857),
                (1.5, 0.42857142857142855),
                (3.75, 0.0),
                (6.0, 0.2857142857142857),
            ],
            id="continuous column",
        ),
        pytest.param(
            "categorical_value",
            "categorical",
            [
                (0, 0.13636363636363635),
                (1, 0.13636363636363635),
                (2, 0.2727272727272727),
                (3, 0.18181818181818182),
                (6, 0.2727272727272727),
            ],
            id="categorical column",
        ),
    ],
)
def test_cli_update_distribution_file(
    data_source_fixture: DataSourceFixture,
    mock_file_system: MockFileSystem,
    column_name,
    distribution_type,
    expected_bins_and_weights,
):
    if distribution_type == "continuous":
        table_name = data_source_fixture.ensure_test_table(customers_test_table)
    else:
        table_name = data_source_fixture.ensure_test_table(dro_categorical_test_table)

    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system_and_run_cli(
        user_home_dir, mock_file_system, data_source_fixture, table_name, column_name, distribution_type
    )

    parsed_dro: dict = YAML().load(mock_file_system.files[f"{user_home_dir}/customers_distribution_reference.yml"])
    weights = parsed_dro["distribution_reference"]["weights"]
    bins = parsed_dro["distribution_reference"]["bins"]
    bins_and_weights_sorted = sorted(zip(bins, weights))
    assert bins_and_weights_sorted == expected_bins_and_weights


@pytest.mark.skipif(
    test_data_source not in ["postgres", "snowflake", "bigquery", "mysql"],
    reason="Support for other data sources is experimental and not tested",
)
@pytest.mark.parametrize(
    "distribution_type",
    [
        pytest.param("continuous", id="continuous distribution type"),
        pytest.param("categorical", id="categorical distribution type"),
    ],
)
def test_cli_update_distribution_errors(
    data_source_fixture: DataSourceFixture, mock_file_system: MockFileSystem, distribution_type
):
    table_name = data_source_fixture.ensure_test_table(null_test_table)

    user_home_dir = mock_file_system.user_home_dir()

    cli_output = mock_file_system_and_run_cli(
        user_home_dir, mock_file_system, data_source_fixture, table_name, "column_with_null_values", distribution_type
    ).output
    assert (
        "column_with_null_values column has only NULL values! To generate a distribution reference object (DRO) your column needs to have more than 0 not null values!"
        in cli_output
    )
