from textwrap import dedent

import pytest
from cli.run_cli import run_cli
from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.fixtures import test_data_source
from helpers.mock_file_system import MockFileSystem


@pytest.mark.skipif(
    test_data_source != "postgres",
    reason="Run for postgres only as nothing data source specific is tested.",
)
@pytest.mark.parametrize(
    "dro_config",
    [
        pytest.param(
            dedent(
                """
            table: {table_name}
            column: cst_size
            distribution_type: continuous
            """
            ),
            id="no sample, no filter",
        ),
        pytest.param(
            dedent(
                """
                table: {table_name}
                column: cst_size
                distribution_type: continuous
                sample: TABLESAMPLE BERNOULLI(50) REPEATABLE(61)
                """
            ),
            id="with sample, no filter",
        ),
        pytest.param(
            dedent(
                """
                table: {table_name}
                column: cst_size
                distribution_type: continuous
                filter: cst_size > 0
                """
            ),
            id="no sample, with filter",
        ),
        pytest.param(
            dedent(
                """
                table: {table_name}
                column: cst_size
                distribution_type: continuous
                sample: TABLESAMPLE BERNOULLI(50) REPEATABLE(61)
                filter: cst_size > 0
                """
            ),
            id="with sample, with filter",
        ),
        pytest.param(
            dedent(
                """
                table: {table_name}
                column: cst_size
                distribution_type: continuous
                filter: random() > 0.5
                """
            ),
            id="hacky sample with filter",
        ),
    ],
)
def test_cli_update_distribution_file(
    data_source_fixture: DataSourceFixture, mock_file_system: MockFileSystem, dro_config: str
):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)
    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system.files = {
        f"{user_home_dir}/configuration.yml": data_source_fixture.create_test_configuration_yaml_str(),
        f"{user_home_dir}/customers_distribution_reference.yml": dro_config.format(table_name=table_name),
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
    assert ("weights" and "bins") in mock_file_system.files[f"{user_home_dir}/customers_distribution_reference.yml"]
