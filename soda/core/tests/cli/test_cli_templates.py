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
def test_check_template(data_source_fixture: DataSourceFixture, mock_file_system: MockFileSystem):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system.files = {
        f"{user_home_dir}/configuration.yml": dedent(
            f"""
                data_source cli_ds:
                  type: postgres
                  host: localhost
                  username: sodasql
                  database: sodasql
                  schema: {data_source_fixture.schema_name}
            """
        ).strip(),
        f"{user_home_dir}/checks.yml": dedent(
            f"""
                checks for {table_name}:
                  - $test_template:
                      parameters:
                        - table: {table_name}
                      fail : when > 0
                      warn: when > 10
            """
        ).strip(),
        f"{user_home_dir}/templates/test_template.yaml": dedent(
            """
            templates:
              - name: test_template
                description: Test template
                author: Vijay Kiran
                parameters:
                  - table
                sql: |
                  SELECT count(*) FROM {{ table }}

            """).strip(),
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

    assert result.exit_code == 0


