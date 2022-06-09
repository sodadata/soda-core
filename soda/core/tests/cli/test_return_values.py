from textwrap import dedent

import pytest
from tests.cli.run_cli import run_cli
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.fixtures import test_data_source
from tests.helpers.mock_file_system import MockFileSystem
from tests.helpers.scanner import Scanner


def get_ds_definition():
    return dedent(
        """
        data_source cli_ds:
            type: postgres
            connection:
                host: localhost
                username: sodasql
            database: sodasql
            schema: public
        """
    ).strip()


@pytest.mark.skipif(
    test_data_source != "postgres",
    reason="Run for postgres only as nothing data source specific is tested.",
)
def test_non_existing_files(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    result = run_cli(
        [
            "scan",
            "-d",
            "cli_ds",
            "-c",
            "non-existing.yml",
            "checks.yml",
        ]
    )
    assert result.exit_code == 3


@pytest.mark.skipif(
    test_data_source != "postgres",
    reason="Run for postgres only as nothing data source specific is tested.",
)
def test_ok_with_variable(scanner: Scanner, mock_file_system: MockFileSystem):
    table_name = scanner.ensure_test_table(customers_test_table)

    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system.files = {
        f"{user_home_dir}/configuration.yml": get_ds_definition(),
        f"{user_home_dir}/checks.yml": dedent(
            f"""
                checks for {table_name}:
                  - freshness using ts with scan_execution_date < 1d
            """
        ).strip(),
    }

    result = run_cli(
        [
            "scan",
            "-d",
            "cli_ds",
            "-c",
            "configuration.yml",
            "-v",
            "scan_execution_date=2020-06-25 00:00:00",
            "checks.yml",
        ]
    )
    assert result.exit_code == 0


@pytest.mark.skipif(
    test_data_source != "postgres",
    reason="Run for postgres only as nothing data source specific is tested.",
)
def test_fail(scanner: Scanner, mock_file_system: MockFileSystem):
    table_name = scanner.ensure_test_table(customers_test_table)

    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system.files = {
        f"{user_home_dir}/configuration.yml": get_ds_definition(),
        f"{user_home_dir}/checks.yml": dedent(
            f"""
                checks for {table_name}:
                  - row_count > 1000
            """
        ).strip(),
    }

    result = run_cli(
        [
            "scan",
            "-d",
            "cli_ds",
            "-c",
            "configuration.yml",
            "checks.yml",
        ]
    )
    assert result.exit_code == 2


@pytest.mark.skipif(
    test_data_source != "postgres",
    reason="Run for postgres only as nothing data source specific is tested.",
)
def test_warn(scanner: Scanner, mock_file_system: MockFileSystem):
    table_name = scanner.ensure_test_table(customers_test_table)

    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system.files = {
        f"{user_home_dir}/configuration.yml": get_ds_definition(),
        f"{user_home_dir}/checks.yml": dedent(
            f"""
                checks for {table_name}:
                    - row_count:
                        warn: when < 1000
            """
        ).strip(),
    }

    result = run_cli(
        [
            "scan",
            "-d",
            "cli_ds",
            "-c",
            "configuration.yml",
            "checks.yml",
        ]
    )
    assert result.exit_code == 1
