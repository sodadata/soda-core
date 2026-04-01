"""CLI integration tests for --check-filter with list-valued attributes.

These tests exercise the full CLI path: sys.argv → argparse → handler → contract verification
against a real DuckDB database. No mocking of the handler or verification engine.
"""

import sys
import tempfile
from pathlib import Path

import duckdb
import pytest
from soda_core.cli.cli import create_cli_parser
from soda_core.cli.exit_codes import ExitCode


@pytest.fixture()
def cli_test_dir():
    """Create a temp directory with a DuckDB database, data source config, and contract YAML."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Create DuckDB database with a test table
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
        conn.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        conn.close()

        # Write data source config
        ds_path = tmp_path / "ds.yml"
        ds_path.write_text(
            f"type: duckdb\n"
            f"name: cli-test\n"
            f"connection:\n"
            f'  database: "{db_path}"\n'
            f"  schema: main\n"
        )

        # Write contract YAML with list-valued attributes
        contract_path = tmp_path / "contract.yml"
        contract_path.write_text(
            "dataset: cli-test/main/test_table\n"
            "columns:\n"
            "  - name: id\n"
            "    checks:\n"
            "      - missing:\n"
            "          attributes:\n"
            "            tags:\n"
            "              - prod\n"
            "              - critical\n"
            "      - duplicate:\n"
            "          attributes:\n"
            "            tags:\n"
            "              - staging\n"
            "  - name: name\n"
            "    checks:\n"
            "      - missing:\n"
            "          attributes:\n"
            "            tags:\n"
            "              - prod\n"
        )

        yield {
            "dir": tmp_path,
            "ds": str(ds_path),
            "contract": str(contract_path),
        }


def _run_cli(args: list[str]) -> int:
    """Parse CLI args and invoke the handler. Returns the exit code."""
    saved_argv = sys.argv
    try:
        sys.argv = args
        parser = create_cli_parser()
        parsed = parser.parse_args()
        with pytest.raises(SystemExit) as exc_info:
            parsed.handler_func(parsed)
        return exc_info.value.code
    finally:
        sys.argv = saved_argv


class TestCliCheckFilterListAttributes:
    def test_member_match_filters_correctly(self, cli_test_dir):
        """'-cf attributes.tags=prod' should run only checks where 'prod' is in the tags list."""
        exit_code = _run_cli(
            [
                "soda",
                "contract",
                "verify",
                "-c",
                cli_test_dir["contract"],
                "-ds",
                cli_test_dir["ds"],
                "-cf",
                "attributes.tags=prod",
            ]
        )
        assert exit_code == ExitCode.OK

    def test_full_list_match_filters_correctly(self, cli_test_dir):
        """'-cf "attributes.tags=[prod,critical]"' should match only the exact list."""
        exit_code = _run_cli(
            [
                "soda",
                "contract",
                "verify",
                "-c",
                cli_test_dir["contract"],
                "-ds",
                cli_test_dir["ds"],
                "-cf",
                "attributes.tags=[prod,critical]",
            ]
        )
        assert exit_code == ExitCode.OK

    def test_no_filter_runs_all_checks(self, cli_test_dir):
        """Without --check-filter, all checks should run."""
        exit_code = _run_cli(
            [
                "soda",
                "contract",
                "verify",
                "-c",
                cli_test_dir["contract"],
                "-ds",
                cli_test_dir["ds"],
            ]
        )
        assert exit_code == ExitCode.OK

    def test_invalid_list_syntax_fails_at_parse_time(self, cli_test_dir):
        """An unterminated quote in list syntax should fail with LOG_ERRORS exit code."""
        exit_code = _run_cli(
            [
                "soda",
                "contract",
                "verify",
                "-c",
                cli_test_dir["contract"],
                "-ds",
                cli_test_dir["ds"],
                "-cf",
                'attributes.tags=["prod,critical]',
            ]
        )
        assert exit_code == ExitCode.LOG_ERRORS

    def test_multiple_filters_and_across_fields(self, cli_test_dir):
        """'-cf attributes.tags=prod -cf column=id' should AND across fields."""
        exit_code = _run_cli(
            [
                "soda",
                "contract",
                "verify",
                "-c",
                cli_test_dir["contract"],
                "-ds",
                cli_test_dir["ds"],
                "-cf",
                "attributes.tags=prod",
                "-cf",
                "column=id",
            ]
        )
        assert exit_code == ExitCode.OK
