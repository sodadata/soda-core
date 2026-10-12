"""Pin the wire ``checkPath`` and the identity of each check form, unscoped and scoped.

The rows live in ``snapshots/check_path_grammar.yml``, the list the Soda Cloud backend mirrors. Each row is
built as a one-check contract on the snapshot's dataset and parsed without executing, so no table is
needed. The data source is named like the dataset's first segment, because its name is part of the
identity. The ``checkPath`` of a row is the grammar and is written by hand; the identity is a snapshot.
The unscoped rows were taken on origin/main ac8c7474, before any scope code existed. The scoped rows
repeat each of them in the scope ``eu``, with the ``scope.eu:`` prefix.

To update the identities after an intended change, run with ``SODA_TEST_UPDATE_SNAPSHOTS=1`` and review
the snapshot diff before committing it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Optional

import duckdb
import pytest
from helpers.snapshot_updates import UPDATE_SNAPSHOTS_ENV_VAR, updating_snapshots
from ruamel.yaml import YAML
from soda_core.common.logs import Logs
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_core.contracts.impl.contract_verification_impl import CheckImpl, ContractImpl
from soda_core.contracts.impl.contract_yaml import ContractYaml
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

SNAPSHOT_PATH = Path(__file__).parent / "snapshots" / "check_path_grammar.yml"


def _round_trip_yaml() -> YAML:
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.width = 120
    return yaml


def _load_grammar() -> dict:
    return _round_trip_yaml().load(SNAPSHOT_PATH.read_text(encoding="utf-8"))


GRAMMAR: dict = _load_grammar()
ROW_IDS: list[str] = [f"{index}-{row['checkPath']}" for index, row in enumerate(GRAMMAR["rows"])]


def _check_scope(row: dict):
    [check_body] = row["check"].values()
    return (check_body or {}).get("scope")


def _build_contract_impl(contract: dict, check_selectors: Optional[list[CheckSelector]] = None) -> ContractImpl:
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
            check_selectors=check_selectors,
        )
        assert not logs.has_errors, logs.get_errors_str()
    finally:
        logs.close()
    return contract_impl


def _build_check_impl(row: dict) -> CheckImpl:
    column_name = row.get("column")
    contract: dict = {"dataset": GRAMMAR["dataset"]}
    if _check_scope(row) is not None:
        contract["scopes"] = GRAMMAR["scopes"]
    if column_name:
        contract["columns"] = [{"name": column_name, "checks": [row["check"]]}]
    else:
        contract["columns"] = [{"name": "id"}]
        contract["checks"] = [row["check"]]
    contract_impl: ContractImpl = _build_contract_impl(contract)
    assert len(contract_impl.all_check_impls) == 1
    return contract_impl.all_check_impls[0]


def _update_identity(row_index: int, identity: str) -> None:
    grammar: dict = _load_grammar()
    grammar["rows"][row_index]["identity"] = identity
    with SNAPSHOT_PATH.open("w", encoding="utf-8") as snapshot_file:
        _round_trip_yaml().dump(grammar, snapshot_file)


def test_grammar_identities_are_unique():
    if updating_snapshots():
        pytest.skip("Asserts the snapshot identities, which this run is updating")
    identities: list[str] = [row["identity"] for row in GRAMMAR["rows"]]
    assert len(identities) == len(set(identities))


@pytest.mark.parametrize("row_index", range(len(GRAMMAR["rows"])), ids=ROW_IDS)
def test_check_path_and_identity_match_grammar(row_index: int):
    row: dict = GRAMMAR["rows"][row_index]
    check_impl: CheckImpl = _build_check_impl(row)

    if updating_snapshots():
        _update_identity(row_index, check_impl.identity)
        pytest.skip(f"Updated {SNAPSHOT_PATH.name}; review the diff and rerun without {UPDATE_SNAPSHOTS_ENV_VAR}")

    scope_key = _check_scope(row)
    if scope_key is None:
        assert check_impl.scope is check_impl.contract_impl.base_scope
    else:
        # The declared scope, not a placeholder for an undeclared key, which would give the same path.
        assert check_impl.scope is check_impl.contract_impl.scopes[scope_key]
    assert check_impl.check_path == row["checkPath"]
    assert check_impl.identity == row["identity"], (
        f"Identity of '{row['checkPath']}' changed from {row['identity']} to {check_impl.identity}. "
        f"If that is intended, update the snapshot with {UPDATE_SNAPSHOTS_ENV_VAR}=1."
    )


def test_identical_checks_in_two_scopes_get_their_own_path_and_identity():
    contract: dict = {
        "dataset": GRAMMAR["dataset"],
        "scopes": {**GRAMMAR["scopes"], "us": {"name": "US", "filter": "country = 'US'"}},
        "columns": [
            {
                "name": "amount",
                "checks": [{"invalid": {"scope": "eu", "valid_min": 0}}, {"invalid": {"scope": "us", "valid_min": 0}}],
            }
        ],
    }
    # Building the contract also fails on a duplicate identity.
    [eu_check, us_check] = _build_contract_impl(contract).all_check_impls

    assert [eu_check.check_path, us_check.check_path] == [
        "scope.eu:columns.amount.checks.invalid",
        "scope.us:columns.amount.checks.invalid",
    ]
    assert eu_check.identity != us_check.identity


SELECTOR_CONTRACT: dict = {
    "dataset": GRAMMAR["dataset"],
    "scopes": GRAMMAR["scopes"],
    "columns": [
        {
            "name": "amount",
            "checks": [{"invalid": {"valid_min": 0}}, {"invalid": {"scope": "eu", "valid_min": 0}}],
        }
    ],
}


@pytest.mark.parametrize(
    "check_selectors, selected",
    [
        # -cp builds check_path selectors, which see the scope prefix.
        (CheckSelector.from_check_paths(["scope.eu:columns.amount.checks.invalid"]), [False, True]),
        (CheckSelector.from_check_paths(["columns.amount.checks.invalid"]), [True, False]),
        ([CheckSelector.parse("check_path=scope.eu:columns.amount.checks.invalid")], [False, True]),
        ([CheckSelector.parse("check_path=scope.eu:*")], [False, True]),
        # path and relative_path selectors never see it.
        ([CheckSelector.parse("path=columns.amount.checks.invalid")], [True, True]),
        ([CheckSelector.parse("relative_path=columns.amount.checks.invalid")], [True, True]),
    ],
    ids=["cp-scoped", "cp-unscoped", "check_path-scoped", "check_path-wildcard", "path", "relative_path"],
)
def test_check_path_selectors_see_the_scope_prefix(check_selectors: list[CheckSelector], selected: list[bool]):
    contract_impl: ContractImpl = _build_contract_impl(SELECTOR_CONTRACT, check_selectors=check_selectors)

    assert [check_impl.check_path for check_impl in contract_impl.all_check_impls] == [
        "columns.amount.checks.invalid",
        "scope.eu:columns.amount.checks.invalid",
    ]
    assert [
        CheckSelector.all_match(check_selectors, check_impl) for check_impl in contract_impl.all_check_impls
    ] == selected
