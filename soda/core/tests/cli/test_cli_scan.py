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
def test_imports(data_source_fixture: DataSourceFixture, mock_file_system: MockFileSystem):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system.files = {
        f"{user_home_dir}/configuration1.yml": dedent(
            """
                data_source cli_ds:
                  type: postgres
                  connection:
                    host: localhost
                    username: sodasql
                  database: sodasql
                  schema: public
            """
        ).strip(),
        f"{user_home_dir}/configuration2.yml": "",
        f"{user_home_dir}/checks1.yml": dedent(
            f"""
                checks for {table_name}:
                  - row_count > 0
            """
        ).strip(),
        f"{user_home_dir}/checks2.yml": dedent(
            f"""
                checks for {table_name}:
                  - row_count > -1
            """
        ).strip(),
    }

    run_cli(
        [
            "scan",
            "-d",
            "cli_ds",
            "-c",
            "configuration1.yml",
            "-c",
            "configuration2.yml",
            "-v",
            "DAY=today",
            "-v",
            "MONTH=june",
            "checks1.yml",
            "checks2.yml",
        ]
    )


@pytest.mark.skipif(
    test_data_source != "postgres",
    reason="Run for postgres only as nothing data source specific is tested.",
)
def test_non_existing_configuration_file(data_source_fixture: DataSourceFixture, mock_file_system: MockFileSystem):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system.files = {
        f"{user_home_dir}/.yml": dedent(
            """
                data_source cli_ds:
                  type: postgres
                  connection:
                    host: localhost
                    username: sodasql
                  database: sodasql
                  schema: public
            """
        ).strip(),
        f"{user_home_dir}/configuration2.yml": "",
        f"{user_home_dir}/checks1.yml": dedent(
            f"""
                checks for {table_name}:
                  - row_count > 0
            """
        ).strip(),
        f"{user_home_dir}/checks2.yml": dedent(
            f"""
                checks for {table_name}:
                  - row_count > -1
            """
        ).strip(),
    }

    run_cli(
        [
            "scan",
            "-d",
            "cli_ds",
            "-c",
            "configuration1.yml",
            "-c",
            "configuration2.yml",
            "-v",
            "DAY=today",
            "-v",
            "MONTH=june",
            "checks1.yml",
            "checks2.yml",
        ]
    )
