"""A verification resolves its data sources by name, so two with one name are refused."""

import os

import duckdb
import pytest
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import handle_verify_contract
from soda_core.cli.handlers.scan import run_scan
from soda_core.common.exceptions import InvalidArgumentException
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

CONTRACT = "dataset: ds/main/orders\ncolumns: []\nchecks:\n  - row_count:\n"


def _duckdb_file(tmp_path, file_name: str) -> str:
    database_path = os.path.join(tmp_path, "shop.duckdb")
    with duckdb.connect(database_path) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS main.orders AS SELECT 1 AS id")
    path = os.path.join(tmp_path, file_name)
    with open(path, "w") as f:
        f.write(f"name: ds\ntype: duckdb\nconnection:\n  database: {database_path}\n")
    return path


def _cli_verify(tmp_path, data_source_file_paths: list[str]) -> ExitCode:
    contract_file_path = os.path.join(tmp_path, "contract.yml")
    with open(contract_file_path, "w") as f:
        f.write(CONTRACT)
    return run_scan(
        None,
        lambda logs: handle_verify_contract(
            contract_file_path=contract_file_path,
            dataset_identifier=None,
            data_source_file_paths=data_source_file_paths,
            soda_cloud_file_path=None,
            variables={},
            publish=False,
            verbose=False,
            use_runner=False,
            blocking_timeout_in_minutes=10,
            check_paths=None,
            check_selectors=[],
            diagnostics_warehouse_file_path=None,
            logs=logs,
        ),
    )


def test_the_cli_refuses_two_data_source_files_with_one_name(tmp_path):
    assert _cli_verify(tmp_path, [_duckdb_file(tmp_path, "ds.yml")]) == ExitCode.OK
    assert _cli_verify(tmp_path, [_duckdb_file(tmp_path, "ds.yml"), _duckdb_file(tmp_path, "ds_copy.yml")]) == (
        ExitCode.LOG_ERRORS
    )


def test_a_session_refuses_two_data_sources_with_one_name():
    def data_source() -> DuckDBDataSourceImpl:
        connection = duckdb.connect(":memory:")
        connection.execute("CREATE TABLE main.orders AS SELECT 1 AS id")
        return DuckDBDataSourceImpl.from_existing_cursor(connection, name="ds")

    def verify(data_sources):
        return ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_str(CONTRACT)], data_source_impls=data_sources
        )

    assert verify([data_source()]).is_passed
    with pytest.raises(InvalidArgumentException):
        verify([data_source(), data_source()])
