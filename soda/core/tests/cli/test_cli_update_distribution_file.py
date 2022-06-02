from textwrap import dedent

import pytest
from tests.cli.run_cli import run_cli
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.fixtures import test_data_source
from tests.helpers.mock_file_system import MockFileSystem
from tests.helpers.scanner import Scanner


@pytest.mark.skipif(
    test_data_source != "postgres",
    reason="Run for postgres only as nothing data source specific is tested.",
)
def test_cli_update_distribution_file(scanner: Scanner, mock_file_system: MockFileSystem, data_source_config_str: str):
    table_name = scanner.ensure_test_table(customers_test_table)

    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system.files = {
        f"{user_home_dir}/configuration.yml": data_source_config_str,
        f"{user_home_dir}/customers_distribution_reference.yml": dedent(
            f"""
                table: {table_name}
                column: size
                method: continuous
            """
        ),
    }

    run_cli(
        [
            "update",
            "-c",
            "configuration.yml",
            "-d",
            scanner.data_source.data_source_name,
            f"{user_home_dir}/customers_distribution_reference.yml",
        ]
    )

    print(mock_file_system.files[f"{user_home_dir}/customers_distribution_reference.yml"])
