from pydoc import cli
from textwrap import dedent
from ruamel.yaml import YAML
import pytest
from cli.run_cli import run_cli
from helpers.common_test_tables import customers_test_table, dro_categorical_test_table, null_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.fixtures import test_data_source
from helpers.mock_file_system import MockFileSystem


@pytest.mark.skipif(
    test_data_source not in ["postgres", "snowflake", "bigquery", "mysql"],
    reason="Other data sources are not supported",
)
def test_cli_update_distribution_file(data_source_fixture: DataSourceFixture, mock_file_system: MockFileSystem):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system.files = {
        f"{user_home_dir}/configuration.yml": data_source_fixture.create_test_configuration_yaml_str(),
        f"{user_home_dir}/customers_distribution_reference.yml": dedent(
            f"""
                table: {table_name}
                column: cst_size
                distribution_type: continuous
            """
        ),
    }

    run_cli(
        [
            "update-dro",
            "-c",
            "configuration.yml",
            "-d",
            data_source_fixture.data_source.data_source_name,
            f"{user_home_dir}/customers_distribution_reference.yml",
        ]
    )
    parsed_dro: dict = YAML().load(mock_file_system.files[f"{user_home_dir}/customers_distribution_reference.yml"])
    weights = parsed_dro['distribution_reference']['weights']
    bins = parsed_dro['distribution_reference']['bins']
    bins_and_weights_sorted = sorted(zip(bins, weights))
    assert bins_and_weights_sorted == [(-3.0, 0.0), (-0.75, 0.2857142857142857), (1.5, 0.42857142857142855), (3.75, 0.0), (6.0, 0.2857142857142857)]


@pytest.mark.skipif(
    test_data_source not in ["postgres", "snowflake", "bigquery", "mysql"],
    reason="Other data sources are not supported",
)
def test_cli_update_distribution_file_categorical(data_source_fixture: DataSourceFixture, mock_file_system: MockFileSystem):
    table_name = data_source_fixture.ensure_test_table(dro_categorical_test_table)

    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system.files = {
        f"{user_home_dir}/configuration.yml": data_source_fixture.create_test_configuration_yaml_str(),
        f"{user_home_dir}/customers_distribution_reference.yml": dedent(
            f"""
                table: {table_name}
                column: categorical_value
                distribution_type: categorical
            """
        ),
    }

    run_cli(
        [
            "update-dro",
            "-c",
            "configuration.yml",
            "-d",
            data_source_fixture.data_source.data_source_name,
            f"{user_home_dir}/customers_distribution_reference.yml",
        ]
    )
    parsed_dro: dict = YAML().load(mock_file_system.files[f"{user_home_dir}/customers_distribution_reference.yml"])
    weights = parsed_dro['distribution_reference']['weights']
    bins = parsed_dro['distribution_reference']['bins']
    bins_and_weights_sorted = sorted(zip(bins, weights))
    assert bins_and_weights_sorted == [(0, 0.13636363636363635), (1, 0.13636363636363635), (2, 0.2727272727272727), (3, 0.18181818181818182), (6, 0.2727272727272727)]

@pytest.mark.skipif(
    test_data_source not in ["postgres", "snowflake", "bigquery", "mysql"],
    reason="Other data sources are not supported",
)
@pytest.mark.parametrize(
    "distribution_type", 
    [
        pytest.param("continuous", id="continuous distribution type"),
        pytest.param("categorical", id="categorical distribution type")
    ]
)
def test_cli_update_distribution_errors(data_source_fixture: DataSourceFixture, mock_file_system: MockFileSystem, distribution_type):
    table_name = data_source_fixture.ensure_test_table(null_test_table)

    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system.files = {
        f"{user_home_dir}/configuration.yml": data_source_fixture.create_test_configuration_yaml_str(),
        f"{user_home_dir}/customers_distribution_reference.yml": dedent(
            f"""
                table: {table_name}
                column: column_with_null_values
                distribution_type: {distribution_type}
            """
        ),
    }

    cli_output = run_cli(
        [
            "update-dro",
            "-c",
            "configuration.yml",
            "-d",
            data_source_fixture.data_source.data_source_name,
            f"{user_home_dir}/customers_distribution_reference.yml",
        ]
    ).output
    assert "column_with_null_values column has only NULL values! To generate a distribution reference object (DRO) your column needs to have more than 0 not null values!" in cli_output