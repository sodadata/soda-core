"""The CLI verify flow, run against a mock Soda Cloud.

For tests that need the real thing from end to end: the contract and the data source are read from files,
a real duckdb data source is opened, and the run ends the way the CLI ends it, with an exit code.
"""

from pathlib import Path
from typing import Optional
from unittest.mock import patch

from helpers.mock_soda_cloud import MockSodaCloud
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import handle_verify_contract
from soda_core.cli.handlers.dependencies import resolve_soda_cloud_for_failure_report
from soda_core.cli.handlers.scan import run_scan

# What the helper writes to disk when a test does not bring its own: a contract with one column, id, and an
# in-memory duckdb data source. A test that brings its own data source (for example a file with a table in it)
# must keep the name test_ds, because the contract points at test_ds/main/my_table.
DEFAULT_CONTRACT_YAML = """
dataset: test_ds/main/my_table
columns:
  - name: id
"""

DEFAULT_DATA_SOURCE_YAML = """
type: duckdb
name: test_ds
connection:
    database: ":memory:"
    schema: main
"""


def handle_verify_contract_with_files(
    tmp_path: Path, mock_cloud: MockSodaCloud, data_source_yaml: Optional[str] = None
) -> ExitCode:
    """Run the real CLI flow end-to-end (real session, real duckdb data source),
    with ``SodaCloud.from_config`` pinned to the given mock. Mirrors the cli.py
    verify wiring: channel resolution first, then the bare command wrapped in
    ``run_scan`` (the single Cloud-marking site).

    The contract and the data source are written to files under ``tmp_path``. The contract is always
    DEFAULT_CONTRACT_YAML. The data source is DEFAULT_DATA_SOURCE_YAML unless the test passes its own."""
    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text(DEFAULT_CONTRACT_YAML)
    data_source_path = tmp_path / "ds.yaml"
    data_source_path.write_text(data_source_yaml if data_source_yaml is not None else DEFAULT_DATA_SOURCE_YAML)

    with patch("soda_core.common.soda_cloud.SodaCloud.from_config", return_value=mock_cloud):
        soda_cloud = resolve_soda_cloud_for_failure_report("sc.yaml", {})
        return run_scan(
            soda_cloud,
            lambda logs: handle_verify_contract(
                contract_file_path=str(contract_path),
                dataset_identifier=None,
                data_source_file_paths=[str(data_source_path)],
                soda_cloud_file_path="sc.yaml",
                variables={},
                publish=True,
                verbose=False,
                use_runner=False,
                blocking_timeout_in_minutes=10,
                check_paths=None,
                check_selectors=[],
                diagnostics_warehouse_file_path=None,
                logs=logs,
            ),
        )
