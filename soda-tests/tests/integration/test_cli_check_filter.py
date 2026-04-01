"""CLI integration tests for --check-filter with list-valued attributes.

These tests exercise the full CLI path: sys.argv → argparse → handler → contract verification
against a real DuckDB database. No mocking of the handler or verification engine.

The test data is designed so that some checks FAIL (duplicate on id), proving
that filtering actually selects the intended checks — not just that all checks pass.
"""

import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest
from soda_core.cli.cli import create_cli_parser
from soda_core.cli.exit_codes import ExitCode
from soda_core.contracts.api import verify_contract as original_verify_contract


@pytest.fixture()
def cli_test_dir():
    """Create a temp directory with a DuckDB database, data source config, and contract YAML.

    Data has a duplicate id (2 appears twice) and no nulls, so:
      - missing checks PASS
      - duplicate check FAILS
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Create DuckDB database with a test table — duplicate id=2 to make duplicate check fail
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
        conn.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob'), (2, 'Charlie')")
        conn.close()

        # Write data source config
        ds_path = tmp_path / "ds.yml"
        ds_path.write_text(
            f"type: duckdb\n" f"name: cli-test\n" f"connection:\n" f'  database: "{db_path}"\n' f"  schema: main\n"
        )

        # Write contract YAML with list-valued attributes
        # Checks:
        #   id.missing   (tags: [prod, critical]) → PASS
        #   id.duplicate (tags: [staging])         → FAIL
        #   name.missing (tags: [prod])            → PASS
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


def _run_cli(args: list[str]) -> tuple[int, object]:
    """Parse CLI args and invoke the handler.

    Returns (exit_code, ContractVerificationSessionResult or None).
    The result is captured by patching verify_contract so we can assert on
    which checks were filtered/executed, not just the exit code.
    """
    captured = {}

    def capturing_verify(**kwargs):
        result = original_verify_contract(**kwargs)
        captured["result"] = result
        return result

    saved_argv = sys.argv
    try:
        sys.argv = args
        parser = create_cli_parser()
        parsed = parser.parse_args()
        with patch("soda_core.cli.handlers.contract.verify_contract", capturing_verify):
            with pytest.raises(SystemExit) as exc_info:
                parsed.handler_func(parsed)
        return exc_info.value.code, captured.get("result")
    finally:
        sys.argv = saved_argv


def _base_args(cli_test_dir) -> list[str]:
    return ["soda", "contract", "verify", "-c", cli_test_dir["contract"], "-ds", cli_test_dir["ds"]]


class TestCliCheckFilterListAttributes:
    def test_member_match_filters_correctly(self, cli_test_dir):
        """'-cf attributes.tags=prod' should run only checks where 'prod' is in the tags list.

        Matches: id.missing (tags=[prod,critical]) and name.missing (tags=[prod]) — both pass.
        Excludes: id.duplicate (tags=[staging]) — would fail.
        """
        exit_code, result = _run_cli(_base_args(cli_test_dir) + ["-cf", "attributes.tags=prod"])

        assert exit_code == ExitCode.OK
        cvr = result.contract_verification_results[0]
        non_excluded = [cr for cr in cvr.check_results if not cr.is_excluded()]
        assert len(non_excluded) == 2
        assert all(cr.check.type == "missing" for cr in non_excluded)
        assert cvr.number_of_checks_excluded == 1

    def test_full_list_match_filters_correctly(self, cli_test_dir):
        """'-cf "attributes.tags=[prod,critical]"' should match only the check with exactly those tags.

        Matches: id.missing (tags=[prod,critical]) — passes.
        Excludes: id.duplicate (tags=[staging]) and name.missing (tags=[prod]).
        """
        exit_code, result = _run_cli(_base_args(cli_test_dir) + ["-cf", "attributes.tags=[prod,critical]"])

        assert exit_code == ExitCode.OK
        cvr = result.contract_verification_results[0]
        non_excluded = [cr for cr in cvr.check_results if not cr.is_excluded()]
        assert len(non_excluded) == 1
        assert non_excluded[0].check.type == "missing"
        assert non_excluded[0].check.column_name == "id"
        assert cvr.number_of_checks_excluded == 2

    def test_no_filter_runs_all_checks(self, cli_test_dir):
        """Without --check-filter, all checks should run — including the failing duplicate."""
        exit_code, result = _run_cli(_base_args(cli_test_dir))

        assert exit_code == ExitCode.CHECK_FAILURES
        cvr = result.contract_verification_results[0]
        assert cvr.number_of_checks_excluded == 0
        assert len(cvr.check_results) == 3

    def test_invalid_list_syntax_fails_at_parse_time(self, cli_test_dir):
        """An unterminated quote in list syntax should fail with LOG_ERRORS exit code."""
        exit_code, result = _run_cli(_base_args(cli_test_dir) + ["-cf", 'attributes.tags=["prod,critical]'])

        assert exit_code == ExitCode.LOG_ERRORS
        assert result is None  # verify_contract was never called

    def test_multiple_filters_and_across_fields(self, cli_test_dir):
        """'-cf attributes.tags=prod -cf column=id' should AND across fields.

        Matches: id.missing (has tag 'prod' AND column 'id') — passes.
        Excludes: id.duplicate (no tag 'prod'), name.missing (column 'name' not 'id').
        """
        exit_code, result = _run_cli(_base_args(cli_test_dir) + ["-cf", "attributes.tags=prod", "-cf", "column=id"])

        assert exit_code == ExitCode.OK
        cvr = result.contract_verification_results[0]
        non_excluded = [cr for cr in cvr.check_results if not cr.is_excluded()]
        assert len(non_excluded) == 1
        assert non_excluded[0].check.type == "missing"
        assert non_excluded[0].check.column_name == "id"
        assert cvr.number_of_checks_excluded == 2

    def test_filter_selects_failing_check(self, cli_test_dir):
        """'-cf attributes.tags=staging' should run only the duplicate check, which fails.

        Matches: id.duplicate (tags=[staging]) — fails.
        Excludes: id.missing (tags=[prod,critical]), name.missing (tags=[prod]).
        """
        exit_code, result = _run_cli(_base_args(cli_test_dir) + ["-cf", "attributes.tags=staging"])

        assert exit_code == ExitCode.CHECK_FAILURES
        cvr = result.contract_verification_results[0]
        non_excluded = [cr for cr in cvr.check_results if not cr.is_excluded()]
        assert len(non_excluded) == 1
        assert non_excluded[0].check.type == "duplicate"
        assert non_excluded[0].check.column_name == "id"
        assert cvr.number_of_checks_excluded == 2
