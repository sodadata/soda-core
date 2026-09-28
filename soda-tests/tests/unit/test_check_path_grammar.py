"""Pin the wire ``checkPath`` and the identity of each unscoped check form.

The rows live in ``fixtures/check_path_grammar.yml``, the list the backend mirrors (DET-206). Each row is
built as a one-check contract on the fixture's dataset and parsed without executing, so no table is
needed. The data source is named like the dataset's first segment, because its name is part of the
identity. The ``checkPath`` of a row is the grammar and is written by hand; the identity is recorded.
Recorded on origin/main before any scope code existed (DET-235 stage 0); C-2 adds the ``scope.eu:`` rows.

To re-record the identities after an intended change, run with ``SODA_TEST_RECORD_FIXTURES=1`` and review
the fixture diff before committing it.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import duckdb
import pytest
from ruamel.yaml import YAML
from soda_core.common.logs import Logs
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.impl.contract_verification_impl import CheckImpl, ContractImpl
from soda_core.contracts.impl.contract_yaml import ContractYaml
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

RECORD_FIXTURES_ENV_VAR = "SODA_TEST_RECORD_FIXTURES"
FIXTURE_PATH = Path(__file__).parent / "fixtures" / "check_path_grammar.yml"


def _round_trip_yaml() -> YAML:
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.width = 120
    return yaml


def _load_grammar() -> dict:
    return _round_trip_yaml().load(FIXTURE_PATH.read_text(encoding="utf-8"))


GRAMMAR: dict = _load_grammar()
ROW_IDS: list[str] = [f"{index}-{row['checkPath']}" for index, row in enumerate(GRAMMAR["rows"])]


def _build_check_impl(row: dict) -> CheckImpl:
    column_name = row.get("column")
    contract: dict = {"dataset": GRAMMAR["dataset"]}
    if column_name:
        contract["columns"] = [{"name": column_name, "checks": [row["check"]]}]
    else:
        contract["columns"] = [{"name": "id"}]
        contract["checks"] = [row["check"]]
    contract_yaml_text = StringIO()
    _round_trip_yaml().dump(contract, contract_yaml_text)

    data_source_name: str = GRAMMAR["dataset"].split("/")[0]
    data_source_impl = DuckDBDataSourceImpl.from_existing_cursor(duckdb.connect(":memory:"), name=data_source_name)
    logs = Logs()
    try:
        contract_yaml: ContractYaml = ContractYaml.parse(
            yaml_source=ContractYamlSource.from_str(contract_yaml_text.getvalue()), provided_variable_values={}
        )
        now = datetime.now(tz=timezone.utc)
        contract_impl = ContractImpl(
            logs=logs,
            yaml=contract_yaml,
            only_validate_without_execute=True,
            data_source_impl=data_source_impl,
            all_data_source_impls={data_source_name: data_source_impl},
            data_timestamp=now,
            execution_timestamp=now,
            soda_cloud_impl=None,
            publish_results=False,
        )
        assert not logs.has_errors, logs.get_errors_str()
    finally:
        logs.close()
    assert len(contract_impl.all_check_impls) == 1
    return contract_impl.all_check_impls[0]


def _record_identity(row_index: int, identity: str) -> None:
    grammar: dict = _load_grammar()
    grammar["rows"][row_index]["identity"] = identity
    with FIXTURE_PATH.open("w", encoding="utf-8") as fixture_file:
        _round_trip_yaml().dump(grammar, fixture_file)


def test_grammar_identities_are_unique():
    identities: list[str] = [row["identity"] for row in GRAMMAR["rows"]]
    assert len(identities) == len(set(identities))


@pytest.mark.parametrize("row_index", range(len(GRAMMAR["rows"])), ids=ROW_IDS)
def test_check_path_and_identity_match_grammar(row_index: int):
    row: dict = GRAMMAR["rows"][row_index]
    check_impl: CheckImpl = _build_check_impl(row)

    if os.environ.get(RECORD_FIXTURES_ENV_VAR) == "1":
        _record_identity(row_index, check_impl.identity)
        pytest.skip(f"Re-recorded {FIXTURE_PATH.name}; review the diff and rerun without {RECORD_FIXTURES_ENV_VAR}")

    assert check_impl.check_path == row["checkPath"]
    assert check_impl.identity == row["identity"], (
        f"Identity of '{row['checkPath']}' changed from {row['identity']} to {check_impl.identity}. "
        f"If that is intended, re-record with {RECORD_FIXTURES_ENV_VAR}=1."
    )
