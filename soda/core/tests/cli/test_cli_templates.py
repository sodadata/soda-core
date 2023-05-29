from textwrap import dedent

import pytest
from cli.run_cli import run_cli
from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.fixtures import test_data_source


@pytest.mark.skipif(
    test_data_source != "postgres",
    reason="Run for postgres only as nothing data source specific is tested.",
)
def test_check_template(data_source_fixture: DataSourceFixture, fs):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)
    fs.create_dir("/templates")
    fs.create_file(
        file_path="/templates/template.yaml",
        contents=dedent(
            """
        templates:
          - name: test_template
            description: Test template
            author: Vijay Kiran
            metric: cnt
            query: |
              SELECT count(*) as cnt FROM ${table_name}

        """
        ).strip(),
    )

    fs.create_file(
        file_path="/checks.yml",
        contents=dedent(
            f"""
                checks for {table_name}:
                  - $test_template:
                      parameters:
                        table_name: {table_name}
                      fail: when > 10
                  - $test_template > 0:
                      parameters:
                        table_name: {table_name}

            """
        ).strip(),
    )

    fs.create_file(
        file_path="/configuration.yml",
        contents=dedent(
            f"""
                data_source cli_ds:
                  type: postgres
                  host: localhost
                  username: sodasql
                  database: sodasql
                  schema: {data_source_fixture.schema_name}
            """
        ).strip(),
    )

    result = run_cli(["scan", "-d", "cli_ds", "-c", "configuration.yml", "checks.yml", "-T", "/templates"])

    assert result.exit_code == 0
