"""The scope skeleton: the ``Scope`` model, the unvalidated parse of ``scopes`` and
``scope``, ``scope_for``, the inactive skip and the unscoped identity.

A declared scope stays inactive in core, so its checks go up as EXCLUDED, and a file
without ``scopes`` and ``scope`` behaves exactly as before. Origin never reads ``scopes``
or ``scope``, so a kind without scope support must run every file here that origin runs.
A contract validates its scope input, see ``contract_yaml/test_scopes_parsing.py``, and
reports what these files get wrong.

Every test here drops the soda-scopes extension, so the file pins core alone even where soda-scopes is installed.
"""

from __future__ import annotations

import dataclasses
import time
from hashlib import blake2b
from types import MappingProxyType, SimpleNamespace
from typing import Optional

import duckdb
import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from helpers.scope_test_kinds import SCOPE_UNSUPPORTED_KIND, ScopeUnsupportedImpl
from helpers.scopes_extension_removal import without_scopes_extension  # noqa: F401
from helpers.test_functions import dedent_and_strip
from helpers.test_table import TestTableSpecification
from soda_core.check_collections.base import CheckCollectionImpl, CheckCollectionYaml
from soda_core.common.consistent_hash_builder import ConsistentHashBuilder
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.filtered_cte import build_filtered_cte
from soda_core.common.logs import Logs
from soda_core.common.sql_ast import SODA_FILTERED_CTE_NAME
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import (
    Check,
    CheckCollectionStatus,
    CheckOutcome,
    ContractVerificationSession,
)
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_core.contracts.impl.contract_verification_impl import CheckCollectionImplExtension, CheckImpl, ContractImpl
from soda_core.contracts.impl.contract_yaml import ContractYaml
from soda_core.contracts.impl.scope import BASE_SCOPE_KEY, RESERVED_SCOPE_KEYS, SCOPE_KEY_PATTERN, Scope, ScopeYaml
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

pytestmark = pytest.mark.usefixtures("without_scopes_extension")

KINDS: list[type[CheckCollectionImpl]] = [ContractImpl, ScopeUnsupportedImpl]
KIND_LINES: dict[type, str] = {ContractImpl: "", ScopeUnsupportedImpl: f"kind: {SCOPE_UNSUPPORTED_KIND}\n"}


def _parse(yaml_str: str) -> tuple[ContractYaml, Logs]:
    """``ContractYaml`` of ``yaml_str``, parsed without a session, and the closed logs of the parse."""
    logs = Logs()
    try:
        return ContractYaml.parse(yaml_source=ContractYamlSource.from_str(dedent_and_strip(yaml_str))), logs
    finally:
        logs.close()


def _build_impl(
    impl_class: type[CheckCollectionImpl], yaml, check_selectors: Optional[list[CheckSelector]] = None
) -> tuple[CheckCollectionImpl, Logs]:
    """``impl_class`` built without a data source from a yaml object or a YAML string, with the closed logs."""
    logs = Logs()
    try:
        if isinstance(yaml, str):
            yaml = ContractYaml.parse(yaml_source=ContractYamlSource.from_str(dedent_and_strip(yaml)))
        impl = impl_class(
            logs=logs,
            yaml=yaml,
            only_validate_without_execute=True,
            data_source_impl=None,
            check_selectors=check_selectors,
        )
        return impl, logs
    finally:
        logs.close()


def _verify(monkeypatch, yaml_str: str) -> tuple:
    """The result and the uploads of ``yaml_str`` verified on a two-row duckdb table the way 'soda contract verify'
    verifies any registered kind. The session logs capture the YAML parse, so one error there fails the file."""
    for env_var in ("SODA_SCAN_ID", "SODA_INSTRUCTION_ID", "SODA_SCAN_DATA_TIMESTAMP", "SODA_SCAN_DEFINITION"):
        monkeypatch.delenv(env_var, raising=False)
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE orders (id INTEGER)")
    connection.execute("INSERT INTO orders VALUES (1), (2)")
    data_source_impl = DuckDBDataSourceImpl.from_existing_cursor(connection, name="fx")
    soda_cloud = MockSodaCloud([MockResponse(status_code=200, json_object={"fileId": "fixture-file-id"})])

    session_result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(yaml_str.replace("ds/db/schema/table", "fx/main/orders"))],
        data_source_impls=[data_source_impl],
        soda_cloud_impl=soda_cloud,
        soda_cloud_publish_results=True,
    )

    [result] = session_result.contract_verification_results
    uploads = [
        request.json
        for request in soda_cloud.requests
        if isinstance(request.json, dict) and request.json.get("type") == "sodaCoreInsertScanResults"
    ]
    return result, uploads


def _two_check_file(scope_value: Optional[str], top: str = "", check_body: str = "") -> str:
    """One unscoped check, then a check with ``scope: <scope_value>``, or with no scope when it is None."""
    scope_line = f"      scope: {scope_value}\n" if scope_value is not None else ""
    return (
        f"dataset: ds/db/schema/table\n{top}columns: []\n"
        f"checks:\n  - row_count:\n  - row_count:\n      qualifier: s\n{check_body}{scope_line}"
    )


def test_scope_constants_and_model():
    assert BASE_SCOPE_KEY == "base"
    assert RESERVED_SCOPE_KEYS == frozenset({"base", "true", "false", "null", "yes", "no", "on", "off", "y", "n"})
    for key in ["eu", "a", "release-gate", "eu_west", "a" * 64]:
        assert SCOPE_KEY_PATTERN.fullmatch(key), key
    for key in ["", "Eu", "1eu", "-eu", "_eu", "eu west", "eu.west", "a" * 65, "eu\n"]:
        assert not SCOPE_KEY_PATTERN.fullmatch(key), key

    base = Scope(key=BASE_SCOPE_KEY)
    eu = Scope(key="eu", name="EU", filter="region = 'eu'")
    assert base.is_base and not eu.is_base
    assert not base.is_active and not eu.is_active
    assert (eu.cte, eu.row_count_metric, eu.check_attributes) == (None, None, {})
    assert base.cte_alias() == SODA_FILTERED_CTE_NAME
    assert eu.cte_alias() == "_soda_filtered_scope_eu"
    assert Scope(key="eu-west").cte_alias() == "_soda_filtered_scope__c3b6b924"

    cte = build_filtered_cte(DatasetIdentifier.parse("ds/db/schema/table"), eu.filter, eu.cte_alias())
    row_count_metric = object()
    eu.activate(cte=cte, row_count_metric=row_count_metric)
    assert eu.is_active
    assert eu.cte is cte and eu.row_count_metric is row_count_metric
    us = Scope(key="us")
    us.activate(cte=cte, row_count_metric=None)
    assert us.is_active and us.row_count_metric is None

    # Equality and hashing are object identity.
    other_eu = Scope(key="eu", name="EU", filter="region = 'eu'")
    assert eu == eu and eu != other_eu
    assert len({eu, other_eu}) == 2


SCOPES_NOT_A_MAPPING: str = "'scopes' must be an object that maps scope keys to scopes, but was"


@pytest.mark.parametrize(
    "scopes_yaml, contract_error",
    [
        ("", None),
        ("scopes:\n", f"{SCOPES_NOT_A_MAPPING} null"),
        ("scopes: null\n", f"{SCOPES_NOT_A_MAPPING} null"),
        ("scopes: {}\n", None),
        ("scopes: []\n", f"{SCOPES_NOT_A_MAPPING} a list"),
        ("scopes: [eu, us]\n", f"{SCOPES_NOT_A_MAPPING} a list"),
        ("scopes: eu\n", f"{SCOPES_NOT_A_MAPPING} a string"),
    ],
)
def test_contract_yaml_scopes_without_entries_read_as_no_scopes(scopes_yaml: str, contract_error: Optional[str]):
    # Only a contract validates 'scopes'; a kind without scope support reads it and logs nothing.
    for kind_line, errors in [("", [contract_error] if contract_error else []), (KIND_LINES[ScopeUnsupportedImpl], [])]:
        contract_yaml, logs = _parse(f"{kind_line}dataset: ds/db/schema/table\n{scopes_yaml}columns: []\n")
        assert type(contract_yaml.scopes) is dict and contract_yaml.scopes == {}
        assert logs.get_logs() == errors


def test_scope_yaml_fields_and_scope_from_yaml():
    # Parsed as a kind without scope support, which reads scope input without validating it.
    contract_yaml, logs = _parse(
        KIND_LINES[ScopeUnsupportedImpl]
        + dedent_and_strip(
            """
        dataset: ds/db/schema/table
        scopes:
          eu:
            name: Europe
            description: EU orders
            filter: |
              region = 'eu'
            check_attributes: {team: finance}
            schedule: {cron: "0 6 * * *", timezone: Europe/Brussels, variables: {LOOKBACK: 7}}
          bare: {name: Bare, filter: "   "}
          text: just a string
          wrong_types: {name: 5, description: [a], filter: 5, check_attributes: [a], schedule: weekly}
          wrong_schedule: {name: W, schedule: {cron: 5, timezone: [a], variables: text}}
          true: {name: T}
          null: {name: N}
          0: {name: Z}
          yes: {name: Y}
          base: {name: B}
          !region us: {name: US}
        columns: []
        """
        )
    )
    # Reading scopes logs nothing, not even for a key or a value of the wrong type.
    assert logs.get_logs() == []
    assert type(contract_yaml.scopes) is dict
    assert all(isinstance(scope_yaml, ScopeYaml) for scope_yaml in contract_yaml.scopes.values())
    keys = list(contract_yaml.scopes)
    assert keys[:5] == ["eu", "bare", "text", "wrong_types", "wrong_schedule"]
    # Each key is kept exactly as ruamel returned it.
    assert keys[5:10] == [True, None, 0, "yes", "base"]
    assert type(keys[5]) is bool and isinstance(keys[7], int)
    assert not isinstance(keys[10], str) and contract_yaml.scopes[keys[10]].name == "US"
    assert [scope_yaml.key for scope_yaml in contract_yaml.scopes.values()] == keys

    eu = contract_yaml.scopes["eu"]
    assert (eu.key, eu.name, eu.description, eu.filter) == ("eu", "Europe", "EU orders", "region = 'eu'")
    assert eu.check_attributes == {"team": "finance"}
    assert (eu.schedule.cron, eu.schedule.timezone) == ("0 6 * * *", "Europe/Brussels")
    assert eu.schedule.variables == {"LOOKBACK": 7}
    assert eu.schedule.location is not None
    assert eu.location.line is not None
    assert str(eu.location) == str(
        contract_yaml.yaml_object.read_value("scopes").create_location_from_yaml_dict_key("eu")
    )

    # A value of the wrong type reads as None, or {} for check_attributes. A body that is not a mapping has no object.
    for key, name in [("bare", "Bare"), ("text", None), ("wrong_types", None)]:
        scope_yaml = contract_yaml.scopes[key]
        assert (scope_yaml.name, scope_yaml.description, scope_yaml.filter) == (name, None, None), key
        assert (scope_yaml.check_attributes, scope_yaml.schedule) == ({}, None), key
        assert (scope_yaml.scope_yaml_object is None) == (key == "text"), key
    wrong_schedule = contract_yaml.scopes["wrong_schedule"].schedule
    assert [wrong_schedule.cron, wrong_schedule.timezone, wrong_schedule.variables] == [None] * 3

    scope = Scope.from_yaml(eu)
    assert (scope.key, scope.name, scope.description, scope.filter) == ("eu", "Europe", "EU orders", "region = 'eu'")
    assert scope.check_attributes == {"team": "finance"}
    assert scope.schedule is eu.schedule
    assert scope.cte is None and scope.row_count_metric is None
    with pytest.raises(TypeError):
        Scope.from_yaml(ScopeYaml(key=True, scope_yaml_object=None, location=None))


@pytest.mark.parametrize(
    "source_body, error",
    [
        ("    <<: *src\n", "'NoneType' object is not subscriptable"),
        ("    <<: *src\n    filter: id > 1\n", "'dataset'"),
    ],
    ids=["merge-keys-only", "merge-and-own-keys"],
)
def test_a_merge_key_outside_scopes_raises_as_on_origin(source_body: str, error: str):
    # Only the reads that scopes add fall back to the mapping's location for a key merged in with '<<'. Every other
    # read raises as on origin, so a data standard whose reconciliation source comes in through a merge key still
    # loses its reconciliation checks to one swallowed extension error, exactly as on origin.
    head = "dataset: ds/db/schema/table\nx-src: &src {dataset: ds/db/schema/source}\nreconciliation:\n  source:\n"
    contract_yaml, _ = _parse(f"{head}{source_body}columns: []\n")
    source = contract_yaml.yaml_object.read_object_opt("reconciliation").read_object_opt("source")
    with pytest.raises((KeyError, TypeError)) as raised:
        source.read_string("dataset")
    assert str(raised.value) == error


@pytest.mark.parametrize(
    "check_body, expected",
    [
        (None, None),
        ("", None),
        ("scope: eu", "eu"),
        ("scope: null", None),
        ("scope:", None),
        ("scope: 5", 5),
        ("scope: true", True),
        ("scope: {a: 1}", {"a": 1}),
        ("scope: [a, b]", ["a", "b"]),
    ],
    ids=["no-body", "absent", "string", "null", "empty", "number", "bool", "mapping", "list"],
)
def test_check_yaml_scope(check_body: Optional[str], expected):
    body = f"      qualifier: q\n      {check_body}\n" if check_body is not None else ""
    # Parsed as a kind without scope support, which reads a check's scope without validating it.
    kind_line = KIND_LINES[ScopeUnsupportedImpl]
    yaml_str = f"{kind_line}dataset: ds/db/schema/table\ncolumns: []\nchecks:\n  - row_count:\n{body}"
    contract_yaml, logs = _parse(yaml_str)
    scope = contract_yaml.checks[0].scope
    assert scope == expected
    if isinstance(expected, (bool, dict, list)):
        assert type(scope) is type(expected)
    if isinstance(expected, (dict, list)):
        # A plain copy prints the same on every parse, and never as a memory address.
        assert str(scope) == str(_parse(yaml_str)[0].checks[0].scope)
        assert "object at 0x" not in str(scope)
    assert logs.get_logs() == []


# A declared variable in every field of a scope body and its schedule. 'REGION' resolves to 'eu'.
SCOPE_INPUT_WITH_A_VARIABLE: str = """
    dataset: ds/db/schema/table
    variables: {REGION: {type: string, default: eu}}
    scopes:
      eu:
        name: ${var.REGION}
        description: in ${var.REGION}
        filter: |
          region = '${var.REGION}'
        check_attributes: {team: "${var.REGION}"}
        schedule: {cron: "${var.REGION}", timezone: "${var.REGION}", variables: {LOOKBACK: "${var.REGION}"}}
    columns: []
"""


@pytest.mark.parametrize(
    "kind_line, resolves",
    [
        ("", True),
        ("kind: contract\n", True),
        (f"kind: {SCOPE_UNSUPPORTED_KIND}\n", False),
        ("kind: nobody-registered-this\n", False),
    ],
    ids=["no-kind", "kind-contract", "kind-without-support", "unregistered-kind"],
)
def test_the_file_kind_decides_whether_scope_input_resolves_variables(kind_line: str, resolves: bool):
    # Parsed without a session, the way publication reads a file. A kind without scope support, and a kind nobody
    # registered, read scope input as written, so no variable in it can log or fail the file.
    contract_yaml, logs = _parse(kind_line + dedent_and_strip(SCOPE_INPUT_WITH_A_VARIABLE))

    value = "eu" if resolves else "${var.REGION}"
    eu = contract_yaml.scopes["eu"]
    assert (eu.name, eu.description, eu.filter) == (value, f"in {value}", f"region = '{value}'")
    assert eu.check_attributes == {"team": value}
    assert (eu.schedule.cron, eu.schedule.timezone, eu.schedule.variables) == (value, value, {"LOOKBACK": value})
    # The line comes from the declared variables, not from a scope read.
    assert logs.get_logs() == ["var.REGION = eu"]


def test_class_defaults_cover_yamls_and_impls_without_scopes():
    assert isinstance(CheckCollectionYaml.scopes, MappingProxyType) and not CheckCollectionYaml.scopes

    class _NeverReadsScopesYaml(CheckCollectionYaml):
        """Runs the base ``__init__`` only, like the metric-monitoring yamls."""

    bare_yaml = _NeverReadsScopesYaml(yaml_source=ContractYamlSource.from_str("dataset: ds/db/schema/table\n"))
    assert dict(bare_yaml.scopes) == {}

    assert CheckCollectionImpl.supports_scopes is False
    assert CheckCollectionImpl.base_scope is None
    assert isinstance(CheckCollectionImpl.scopes, MappingProxyType) and not CheckCollectionImpl.scopes
    assert "supports_scopes" in ContractImpl.__dict__ and ContractImpl.supports_scopes is True
    assert ScopeUnsupportedImpl.supports_scopes is False

    stub = ContractImpl.__new__(ContractImpl)
    assert (getattr(stub, "scopes", None) or {}) == {}
    assert getattr(stub, "base_scope", None) is None
    assert getattr(type(stub), "supports_scopes", False) is True

    # A yaml that is not a CheckCollectionYaml, like the data-standard test fake, must carry 'scopes' itself.
    duck_typed_yaml = SimpleNamespace(
        dataset="ds/db/schema/table",
        scopes={},
        filter=None,
        check_attributes={},
        columns=[],
        checks=[],
        yaml_source=SimpleNamespace(file_path="/fake/duck.yml", yaml_str_original="# duck", description="duck yaml"),
        yaml_object=SimpleNamespace(keys=lambda: []),
    )
    impl, logs = _build_impl(ScopeUnsupportedImpl, duck_typed_yaml)
    assert impl.scopes == {}
    assert impl.base_scope.is_base and impl.base_scope.is_active
    assert not logs.has_errors


# Three checks in the base scope, one in a declared scope, then five that get a placeholder. A tagged value is not a
# string, so '!tag eu' names no declared scope, while '!tag base' still reads as the base.
SCOPE_FOR_YAML: str = """
    dataset: ds/db/schema/table
    filter: id > 1
    check_attributes: {team: data}
    scopes:
      eu: {name: EU, filter: region = 'eu'}
      true: {name: bool key}
      base: {name: base key}
      us: {name: US}
    columns: []
    checks:
      - row_count: {qualifier: unscoped}
      - row_count: {qualifier: base, scope: base}
      - row_count: {qualifier: tagged-base, scope: !tag base}
      - row_count: {qualifier: eu, scope: eu}
      - row_count: {qualifier: tagged-eu, scope: !tag eu}
      - row_count: {qualifier: undeclared, scope: undeclared}
      - row_count: {qualifier: number, scope: 5}
      - row_count: {qualifier: mapping, scope: {a: 1}}
      - row_count: {qualifier: list, scope: [a, b]}
"""
PLACEHOLDER_KEYS: list[str] = ["eu", "undeclared", "5", "{'a': 1}", "['a', 'b']"]
# A contract reports the two keys that are no scope keys and every check scope that names no declared scope. A kind
# without scope support reports nothing.
BASE_RESERVED: str = "'base' is reserved for the checks without a scope"
SCOPE_FOR_ERRORS: dict[type, list[str]] = {
    ContractImpl: [
        "Invalid scope key true: a scope key must be a string, but YAML reads this one as a boolean",
        f"Invalid scope key 'base': {BASE_RESERVED}",
        f"Invalid check scope 'base': {BASE_RESERVED}",
        "Check 'scope' must name a declared scope, but was a tagged value: base",
        "Check 'scope' must name a declared scope, but was a tagged value: eu",
        "Check references unknown scope 'undeclared'. Declared scopes: ['eu', 'us']",
        "Check 'scope' must name a declared scope, but was a number: 5",
        "Check 'scope' must name a declared scope, but was an object",
        "Check 'scope' must name a declared scope, but was a list",
    ],
    ScopeUnsupportedImpl: [],
}


@pytest.mark.parametrize("impl_class", KINDS)
def test_scope_for(impl_class: type[CheckCollectionImpl]):
    impl, logs = _build_impl(impl_class, KIND_LINES[impl_class] + dedent_and_strip(SCOPE_FOR_YAML))
    assert logs.get_errors() == SCOPE_FOR_ERRORS[impl_class]

    # The base scope shares the collection's own objects and is active on the collection CTE.
    base_scope = impl.base_scope
    assert (base_scope.key, base_scope.is_base, base_scope.is_active) == (BASE_SCOPE_KEY, True, True)
    assert base_scope.cte is impl.cte and base_scope.row_count_metric is impl.row_count_metric_impl
    assert base_scope.filter is impl.filter and base_scope.check_attributes is impl.check_attributes
    assert (base_scope.filter, base_scope.check_attributes) == ("id > 1", {"team": "data"})
    assert base_scope.cte_alias() == impl.cte.alias == SODA_FILTERED_CTE_NAME

    # Declared scopes keep file order and start inactive. Non-string keys and the key 'base' are never scopes.
    assert list(impl.scopes) == ["eu", "us"]
    assert not any(scope.is_active for scope in impl.scopes.values())
    assert (impl.scopes["eu"].name, impl.scopes["eu"].filter) == ("EU", "region = 'eu'")
    assert base_scope not in impl.scopes.values()

    check_impls = impl.all_check_impls
    assert all(check_impl.scope is impl.base_scope for check_impl in check_impls[:3])
    assert check_impls[3].scope is impl.scopes["eu"]
    placeholders = [check_impl.scope for check_impl in check_impls[4:]]
    assert [placeholder.key for placeholder in placeholders] == PLACEHOLDER_KEYS
    for placeholder in placeholders:
        assert not placeholder.is_active and not placeholder.is_base
        assert all(placeholder is not scope for scope in impl.scopes.values())

    # Every check is selected. A check in an inactive scope is skipped and builds no metrics.
    assert all(check_impl.selected for check_impl in check_impls)
    assert [check_impl.skip for check_impl in check_impls] == [False] * 3 + [True] * 6
    assert [bool(check_impl.metrics) for check_impl in check_impls] == [True] * 3 + [False] * 6
    # 'selected' is the selector verdict alone.
    selector = CheckSelector.parse("qualifier=eu")
    selected_impl, _ = _build_impl(impl_class, KIND_LINES[impl_class] + dedent_and_strip(SCOPE_FOR_YAML), [selector])
    assert [check_impl.selected for check_impl in selected_impl.all_check_impls] == [False] * 3 + [True] + [False] * 5
    assert all(check_impl.skip for check_impl in selected_impl.all_check_impls)

    last_field = dataclasses.fields(Check)[-1]
    assert (last_field.name, last_field.default) == ("scope", None)
    check_infos = [check_impl._build_check_info() for check_impl in check_impls]
    assert [check.scope for check in check_infos] == [None] * 3 + ["eu"] + PLACEHOLDER_KEYS


def test_extension_constructor_sees_the_scopes_before_checks_are_parsed():
    seen: dict = {}

    class _ActivatingExtension(CheckCollectionImplExtension):
        """Activates every declared scope on the collection CTE, the way a scope extension would."""

        def __init__(self, contract_impl: CheckCollectionImpl):
            seen["scopes"] = list(contract_impl.scopes)
            seen["base_scope_active"] = contract_impl.base_scope.is_active
            for scope in contract_impl.scopes.values():
                scope.activate(cte=contract_impl.cte, row_count_metric=None)

    ContractImpl.register_extension("scope_skeleton_activation", _ActivatingExtension)
    try:
        impl, logs = _build_impl(ContractImpl, SCOPE_FOR_YAML)
    finally:
        ContractImpl.impl_extensions.pop("scope_skeleton_activation", None)

    assert seen == {"scopes": ["eu", "us"], "base_scope_active": True}
    eu = impl.all_check_impls[3]
    assert eu.scope.is_active and eu.metrics
    assert [check_impl.skip for check_impl in impl.all_check_impls] == [False] * 4 + [True] * 5
    assert logs.get_errors() == SCOPE_FOR_ERRORS[ContractImpl]


class _ContractStub:
    wire_source = "soda-contract"
    collection_id = None
    data_source_impl = SimpleNamespace(name="test_ds")
    dataset_prefix = "schema"
    dataset_name = "table"
    identity_prefix = ContractImpl.identity_prefix


def _identity(column_name: Optional[str] = None, qualifier: Optional[str] = None, **kwargs) -> str:
    column_impl = SimpleNamespace(column_yaml=SimpleNamespace(name=column_name)) if column_name else None
    return CheckImpl._build_identity(
        contract_impl=_ContractStub(), column_impl=column_impl, check_type="row_count", qualifier=qualifier, **kwargs
    )


def _identity_hash_with_scope_term(scope_term: str) -> str:
    # The scope term, then origin's terms in origin's order.
    expected = ConsistentHashBuilder(8)
    expected.add_property("scope", scope_term)
    origin_terms = [("dso", "test_ds"), ("pr", "schema"), ("ds", "table"), ("c", None), ("t", "row_count"), ("q", None)]
    for key, value in origin_terms:
        expected.add_property(key, value)
    return expected.get_hash()


def test_unscoped_identity_is_unchanged():
    without_keyword = _identity()
    assert _identity(scope_key=None) == without_keyword
    assert _identity(scope_key=BASE_SCOPE_KEY) == without_keyword
    assert _identity(qualifier="q", scope_key=BASE_SCOPE_KEY) == _identity(qualifier="q")


def _digest_term(value: str) -> str:
    return "#" + blake2b(value.encode("utf-8", "surrogatepass"), digest_size=8).hexdigest()


@pytest.mark.parametrize(
    "value, term",
    [(key, f"{key}:") for key in ["eu", "e", "true", "release-gate", "eu_west_1", "a" * 64]]
    + [
        (value, _digest_term(value)) for value in ["x:y", "Eu", "5", "{'a': 1}", "eu\n", "a" * 65, "\ud800", "<HexInt>"]
    ],
)
def test_the_scope_term_comes_first(value: str, term: str):
    # A valid scope key feeds itself and ':'. Any other value feeds '#', which no valid scope key starts with, and a
    # digest whose fixed length ends the term.
    assert (SCOPE_KEY_PATTERN.fullmatch(value) is not None) == term.endswith(":")
    assert _identity(scope_key=value) == _identity_hash_with_scope_term(term)


@pytest.mark.parametrize(
    "scoped, other",
    [
        # After 'q' the scope term would meet the free-text qualifier, and after 'ds' the column name.
        ({"qualifier": "x", "scope_key": "eu"}, {"qualifier": "xscopeeu"}),
        ({"column_name": "cx", "scope_key": "eu"}, {"column_name": "x", "scope_key": "euc"}),
        ({"scope_key": "eu"}, {"scope_key": "us"}),
        ({"scope_key": "eu"}, {}),
        ({"scope_key": "\ud800"}, {}),
        ({"scope_key": "\ud800"}, {"scope_key": "eu"}),
        # ':' closes the term only for a valid scope key, so an invalid value cannot end it early. The middle part is
        # what the builder feeds between the scope term and the qualifier, with no separators.
        (
            {"qualifier": "y:dsotest_dsprschemadstabletrow_countqz", "scope_key": "x"},
            {"qualifier": "z", "scope_key": "x:dsotest_dsprschemadstabletrow_countqy"},
        ),
    ],
)
def test_scoped_identity_never_collides(scoped: dict, other: dict):
    assert _identity(**scoped) != _identity(**other)


def test_identical_checks_in_two_scopes_get_different_identities():
    impl, logs = _build_impl(
        ContractImpl,
        """
        dataset: ds/db/schema/table
        scopes:
          eu: {name: EU}
          us: {name: US}
        columns: []
        checks:
          - row_count:
          - row_count: {scope: eu}
          - row_count: {scope: us}
        """,
    )
    identities = [check_impl.identity for check_impl in impl.all_check_impls]
    assert len(set(identities)) == 3
    assert not logs.has_errors

    unscoped_file, _ = _build_impl(ContractImpl, "dataset: ds/db/schema/table\ncolumns: []\nchecks:\n  - row_count:\n")
    assert identities[0] == unscoped_file.all_check_impls[0].identity


test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("scope_skeleton")
    .column_integer("id")
    .rows(rows=[(1,), (2,), (3,)])
    .build()
)


def test_inactive_scope_check_is_excluded(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    session_result = data_source_test_helper.verify_contract(
        test_table=test_table,
        contract_yaml_str="""
            scopes:
              eu: {name: EU, filter: id > 1}
            checks:
              - row_count:
              - row_count: {scope: eu}
        """,
    )

    result = session_result.contract_verification_results[0]
    assert [(check_result.check.scope, check_result.outcome) for check_result in result.check_results] == [
        (None, CheckOutcome.PASSED),
        ("eu", CheckOutcome.EXCLUDED),
    ]
    assert result.number_of_checks_excluded == 1
    assert not session_result.has_errors


# A value nested deeper than a copy can recurse, built from a chain of anchors so the YAML parser itself never
# recurses deeply. Origin copies the anchors in file order and never reads them as scope input.
ANCHOR_CHAIN_DEPTH: int = 800
DEEPEST_ALIAS: str = f"*l{ANCHOR_CHAIN_DEPTH - 1}"
# Deep enough that str() of the value recurses too far as well on Python 3.10, 3.11 and 3.14, while 3.12 and 3.13
# still print it. Origin takes seconds per file at this depth and grows fast past it.
STR_RECURSION_DEPTH: int = 1200


def _anchor_chain(indent: str, depth: int = ANCHOR_CHAIN_DEPTH) -> str:
    links = [f"{indent}- &l{index} [*l{index - 1}]\n" for index in range(1, depth)]
    return f"{indent}- &l0 [x]\n" + "".join(links)


def _shared_anchors(indent: str, fan_out: int = 10, levels: int = 6) -> str:
    # Each level lists the level before it fan_out times, so the value grows fan_out-fold per level while the file
    # grows by one short line.
    lines = [f"{indent}- &b0 [{', '.join(['x'] * fan_out)}]\n"]
    for level in range(1, levels):
        lines.append(f"{indent}- &b{level} [{', '.join([f'*b{level - 1}'] * fan_out)}]\n")
    return "".join(lines)


# Scope input that origin runs, since it never reads it, and that failed a file of every kind in review. Each file has
# one unscoped and one scoped check. ruamel keeps no position for a merged or tagged key, str() rejects a huge int,
# UTF-8 a lone surrogate, a copy recurses too far on a deep value and expands shared anchors, and a variable inside a
# value that is not a string is never resolved.
SCOPE_FILES_ORIGIN_RUNS: dict[str, str] = {
    "merge-key-in-scope-body": _two_check_file(
        "eu", top="x-d: &d {name: Shared, check_attributes: {team: finance}}\nscopes: {eu: {<<: *d, filter: id > 1}}\n"
    ),
    "merge-key-in-scope-schedule": _two_check_file(
        "eu", top="x-d: &d {cron: '0 6 * * *'}\nscopes:\n  eu:\n    schedule:\n      <<: *d\n      timezone: UTC\n"
    ),
    "merge-key-in-scopes": _two_check_file("eu", top="x-d: &d {us: {name: US}}\nscopes:\n  <<: *d\n  eu: {name: EU}\n"),
    "merge-keys-only-in-scope-body": _two_check_file(
        "eu", top="x-d: &d {name: Shared, filter: id > 1}\nscopes:\n  eu:\n    <<: *d\n"
    ),
    "merge-keys-only-in-check-body": (
        "dataset: ds/db/schema/table\nx-d: &d {scope: eu}\nscopes:\n  eu: {name: EU}\ncolumns: []\n"
        "checks:\n  - row_count:\n  - row_count:\n      <<: *d\n"
    ),
    "merge-key-at-root": _two_check_file("eu", top="x-d: &d {scopes: {eu: {name: EU}}}\n<<: *d\n"),
    "tagged-key-in-scopes": _two_check_file("eu", top="scopes:\n  !region eu: {name: EU}\n"),
    "huge-int": _two_check_file("0x" + "f" * 3700),
    "huge-int-in-a-mapping": _two_check_file("{a: 0x" + "f" * 3700 + "}"),
    "lone-surrogate": _two_check_file('"\\ud800"'),
    "tagged-lone-surrogate": _two_check_file('!tag "\\ud800"'),
    "declared-lone-surrogate": _two_check_file('"\\ud800"', top='scopes:\n  "\\ud800":\n    name: odd\n'),
    "deep-check-scope": _two_check_file(DEEPEST_ALIAS, check_body="      x_anchors:\n" + _anchor_chain("        ")),
    "deep-check-scope-past-str": _two_check_file(
        f"*l{STR_RECURSION_DEPTH - 1}",
        check_body="      x_anchors:\n" + _anchor_chain("        ", STR_RECURSION_DEPTH),
    ),
    "deep-scopes-block": _two_check_file(
        "eu", top=f"x_anchors:\n{_anchor_chain('  ')}scopes:\n  eu:\n    name: EU\n    description: {DEEPEST_ALIAS}\n"
    ),
    # The anchors sit inside the scope body, so the body copies and only the one value does not.
    "deep-scope-body-value": _two_check_file(
        "eu",
        top=f"scopes:\n  eu:\n    name: EU\n    x_anchors:\n{_anchor_chain('      ')}    filter: {DEEPEST_ALIAS}\n",
    ),
    "deep-schedule-value": _two_check_file(
        "eu",
        top="scopes:\n  eu:\n    name: EU\n    schedule:\n      cron: '0 6 * * *'\n"
        f"      x_anchors:\n{_anchor_chain('        ')}      variables: {DEEPEST_ALIAS}\n",
    ),
    "shared-anchors": _two_check_file("*b5", check_body="      x_anchors:\n" + _shared_anchors("        ")),
    "variable-in-a-list": _two_check_file('["${var.NOPE}"]'),
    "variable-in-a-mapping": _two_check_file('{a: "${var.NOPE}"}'),
    "variable-in-a-nested-list": _two_check_file('[[x, "${var.NOPE}"]]'),
}


# What a contract reports for each of those files. Only the merged keys are valid scope input, so every other file
# fails as a contract, while a kind without scope support still runs it as origin does.
KEY_PATTERN_REASON: str = (
    "a scope key starts with a lowercase letter, followed by at most 63 lowercase letters, digits, '_' or '-'"
)
NOT_A_DECLARED_SCOPE: str = "Check 'scope' must name a declared scope, but was"
CONTRACT_ERRORS_FOR_SCOPE_FILES_ORIGIN_RUNS: dict[str, list[str]] = {
    "merge-key-in-scope-body": [],
    "merge-key-in-scope-schedule": ["Scope 'eu' has no 'name'"],
    "merge-key-in-scopes": [],
    "merge-keys-only-in-scope-body": [],
    "merge-keys-only-in-check-body": [],
    "merge-key-at-root": [],
    "tagged-key-in-scopes": [
        "Invalid scope key eu: a scope key must be a string, but YAML reads this one as a tagged value",
        "Check references unknown scope 'eu'. No scopes are declared",
    ],
    "huge-int": [f"{NOT_A_DECLARED_SCOPE} a number: <HexInt>"],
    "huge-int-in-a-mapping": [f"{NOT_A_DECLARED_SCOPE} an object"],
    "lone-surrogate": ["Check references unknown scope '\\ud800'. No scopes are declared"],
    "tagged-lone-surrogate": [f"{NOT_A_DECLARED_SCOPE} a tagged value: \\ud800"],
    "declared-lone-surrogate": [f"Invalid scope key '\\ud800': {KEY_PATTERN_REASON}"],
    "deep-check-scope": [f"{NOT_A_DECLARED_SCOPE} a list"],
    "deep-check-scope-past-str": [f"{NOT_A_DECLARED_SCOPE} a list"],
    # A 'scopes' block too deep to copy reads as no scopes.
    "deep-scopes-block": [
        "'description' of scope 'eu' must be a string, but was a list",
        "Check references unknown scope 'eu'. No scopes are declared",
    ],
    "deep-scope-body-value": [
        "Unknown key 'x_anchors' in scope 'eu'",
        "'filter' of scope 'eu' must be a string, but was a list",
    ],
    "deep-schedule-value": [
        "Unknown key 'x_anchors' in the schedule of scope 'eu'",
        "'variables' in the schedule of scope 'eu' must be an object, but was a list",
    ],
    "shared-anchors": [f"{NOT_A_DECLARED_SCOPE} a list"],
    "variable-in-a-list": [f"{NOT_A_DECLARED_SCOPE} a list"],
    "variable-in-a-mapping": [f"{NOT_A_DECLARED_SCOPE} an object"],
    "variable-in-a-nested-list": [f"{NOT_A_DECLARED_SCOPE} a list"],
}


def _assert_scoped_identity_is_stable(impl_class: type[CheckCollectionImpl], impl, yaml_str: str) -> None:
    unscoped, scoped = impl.all_check_impls
    assert not scoped.scope.is_base
    assert scoped.identity != unscoped.identity
    assert scoped.identity == _build_impl(impl_class, yaml_str)[0].all_check_impls[1].identity


@pytest.mark.parametrize("yaml_str", list(SCOPE_FILES_ORIGIN_RUNS.values()), ids=list(SCOPE_FILES_ORIGIN_RUNS))
def test_scope_input_origin_ignores_never_fails_a_kind_without_scope_support(monkeypatch, yaml_str):
    yaml_str = KIND_LINES[ScopeUnsupportedImpl] + yaml_str
    impl, logs = _build_impl(ScopeUnsupportedImpl, yaml_str)
    assert not logs.has_errors
    _assert_scoped_identity_is_stable(ScopeUnsupportedImpl, impl, yaml_str)

    # A failed file of a kind without scope support drops out of the upload, and the dataset-wide sweep then archives
    # its checks.
    result, uploads = _verify(monkeypatch, yaml_str)
    assert result.error is None, repr(result.error)
    assert result.get_errors() == []
    outcomes = [check_result.outcome for check_result in result.check_results]
    assert outcomes == [CheckOutcome.PASSED, CheckOutcome.EXCLUDED]
    assert result.number_of_checks_excluded == 1
    assert [len(upload["checks"]) for upload in uploads] == [2]


@pytest.mark.parametrize("file_key", list(SCOPE_FILES_ORIGIN_RUNS))
def test_a_contract_reports_the_scope_input_origin_ignores(monkeypatch, file_key: str):
    yaml_str = SCOPE_FILES_ORIGIN_RUNS[file_key]
    errors = CONTRACT_ERRORS_FOR_SCOPE_FILES_ORIGIN_RUNS[file_key]
    impl, logs = _build_impl(ContractImpl, yaml_str)
    assert logs.get_errors() == errors
    _assert_scoped_identity_is_stable(ContractImpl, impl, yaml_str)

    result, uploads = _verify(monkeypatch, yaml_str)
    assert result.error is None, repr(result.error)
    assert result.get_errors() == errors
    if errors:
        assert result.status == CheckCollectionStatus.ERROR
        assert result.check_results == []
        assert _uploaded_checks(uploads) == [[]]
    else:
        outcomes = [check_result.outcome for check_result in result.check_results]
        assert outcomes == [CheckOutcome.PASSED, CheckOutcome.EXCLUDED]
        assert result.number_of_checks_excluded == 1
        assert [len(upload["checks"]) for upload in uploads] == [2]


def test_scope_reads_of_input_origin_ignores():
    def scopes(file_key: str) -> dict:
        return _parse(SCOPE_FILES_ORIGIN_RUNS[file_key])[0].scopes

    # Merged keys read like written ones, and ruamel puts them after the mapping's own keys.
    eu = scopes("merge-key-in-scope-body")["eu"]
    assert (eu.name, eu.check_attributes, eu.filter) == ("Shared", {"team": "finance"}, "id > 1")
    schedule = scopes("merge-key-in-scope-schedule")["eu"].schedule
    assert (schedule.cron, schedule.timezone) == ("0 6 * * *", "UTC")
    assert [(key, scope.name) for key, scope in scopes("merge-key-in-scopes").items()] == [("eu", "EU"), ("us", "US")]
    eu = scopes("merge-keys-only-in-scope-body")["eu"]
    assert (eu.name, eu.filter) == ("Shared", "id > 1")
    assert _parse(SCOPE_FILES_ORIGIN_RUNS["merge-keys-only-in-check-body"])[0].checks[1].scope == "eu"
    assert [(key, scope.name) for key, scope in scopes("merge-key-at-root").items()] == [("eu", "EU")]

    # A check scope too deep to copy stays as parsed: never None, which would run the check unscoped, and never a
    # string, so it can never name a declared scope.
    for file_key in ["deep-check-scope", "deep-check-scope-past-str"]:
        unscoped, scoped = _parse(SCOPE_FILES_ORIGIN_RUNS[file_key])[0].checks
        assert unscoped.scope is None
        assert isinstance(scoped.scope, list), file_key
    # Such a value in a scope body or a schedule reads as absent when the mapping around it still copies, and a whole
    # 'scopes' block that does not copy reads as no scopes.
    eu = scopes("deep-scope-body-value")["eu"]
    assert (eu.name, eu.filter) == ("EU", None)
    schedule = scopes("deep-schedule-value")["eu"].schedule
    assert (schedule.cron, schedule.variables) == ("0 6 * * *", None)
    assert scopes("deep-scopes-block") == {}


@pytest.mark.parametrize("impl_class", KINDS)
def test_a_scope_value_built_from_shared_anchors_reads_without_expanding_it(impl_class: type[CheckCollectionImpl]):
    # About 500 bytes whose scope value expands to a million scalars. Origin reads it in milliseconds. Each further
    # level multiplies the expanded value, and the time to build or print it, by ten.
    yaml_str = SCOPE_FILES_ORIGIN_RUNS["shared-anchors"]
    assert len(yaml_str) < 600
    started = time.monotonic()
    _build_impl(impl_class, KIND_LINES[impl_class] + yaml_str)
    elapsed = time.monotonic() - started
    assert elapsed < 5, f"building the impl took {elapsed:.1f}s"


# Each place in the scope input where a variable can be used, with 'REF' standing for the reference. None of these
# files has a scoped check, so origin, which never reads 'scopes', runs both checks and logs nothing.
UNDECLARED_VARIABLE_IN_SCOPE_INPUT: dict[str, str] = {
    "scope-filter": "scopes:\n  eu:\n    name: EU\n    filter: 'REF'\n",
    "scope-name": "scopes:\n  eu:\n    name: 'REF'\n",
    "scope-description": "scopes:\n  eu:\n    name: EU\n    description: 'REF'\n",
    "scope-check-attributes-value": "scopes:\n  eu:\n    name: EU\n    check_attributes:\n      team: 'REF'\n",
    "scopes-block": "scopes: 'REF'\n",
    "scope-body": "scopes:\n  eu: 'REF'\n",
    "schedule-cron": "scopes:\n  eu:\n    name: EU\n    schedule:\n      cron: 'REF'\n",
    "schedule-timezone": (
        "scopes:\n  eu:\n    name: EU\n    schedule:\n      cron: '0 6 * * *'\n      timezone: 'REF'\n"
    ),
    "schedule-variables": (
        "scopes:\n  eu:\n    name: EU\n    schedule:\n      cron: '0 6 * * *'\n      variables: {LOOKBACK: 'REF'}\n"
    ),
}
# A check's scope value holding the reference, alone or inside a longer string. Origin resolves variables one level
# into a check body, so it logs the reference once from its own read of the body and fails the file. That read leaves
# a lone reference as null and a reference inside a longer string as written. A contract reads the value once more,
# like a check 'filter', so the longer string logs a second line there.
UNDECLARED_VARIABLE_IN_CHECK_SCOPE: dict[str, str] = {
    "check-scope": "'REF'",
    "check-scope-inside-a-string": "'eu-REF'",
}
# The key of the check's scope. A lone reference read as null is the base scope, and a longer string as written names
# a placeholder.
CHECK_SCOPE_KEY_AS_READ: dict[str, str] = {"'REF'": BASE_SCOPE_KEY, "'eu-REF'": "eu-REF"}
UNDECLARED_VARIABLE_MESSAGES: dict[str, str] = {
    "${var.NOPE}": "Variable 'NOPE' was used and not declared",
    "${soda.NOPE}": "Variable 'NOPE' was used and not available in the 'soda' namespace",
}


def _undeclared_variable_file(kind_line: str, scopes_block: str, scope_value: Optional[str], reference: str) -> str:
    return (kind_line + _two_check_file(scope_value, top=scopes_block)).replace("REF", reference)


def _uploaded_checks(uploads: list) -> list:
    return [
        [(check["identities"]["vc1"], check["outcome"]) for check in upload.get("checks", [])] for upload in uploads
    ]


def _variable_log_lines(result, uploads: list) -> tuple:
    return (
        [line for line in result.get_logs() if line.startswith("Variable 'NOPE'")],
        [
            [log["message"] for log in upload.get("logs", []) if log["message"].startswith("Variable 'NOPE'")]
            for upload in uploads
        ],
    )


@pytest.mark.parametrize("reference", list(UNDECLARED_VARIABLE_MESSAGES))
@pytest.mark.parametrize(
    "scopes_block", list(UNDECLARED_VARIABLE_IN_SCOPE_INPUT.values()), ids=list(UNDECLARED_VARIABLE_IN_SCOPE_INPUT)
)
def test_unsupported_kind_ends_as_origin_on_an_undeclared_variable_in_scope_input(
    monkeypatch, scopes_block: str, reference: str
):
    kind_line = KIND_LINES[ScopeUnsupportedImpl]
    origin, origin_uploads = _verify(monkeypatch, _undeclared_variable_file(kind_line, "", None, reference))
    result, uploads = _verify(monkeypatch, _undeclared_variable_file(kind_line, scopes_block, None, reference))

    assert result.status == origin.status == CheckCollectionStatus.PASSED
    assert [check_result.outcome for check_result in result.check_results] == [CheckOutcome.PASSED] * 2
    assert result.get_errors() == []
    assert result.get_warnings() == []
    assert _variable_log_lines(result, uploads) == ([], [[]])
    assert _uploaded_checks(uploads) == _uploaded_checks(origin_uploads)
    assert [len(upload["checks"]) for upload in uploads] == [2]


# A contract also rejects a 'scopes' block or a scope body that is a string, whatever the string holds.
CONTRACT_ERRORS_AFTER_THE_UNDECLARED_VARIABLE: dict[str, list[str]] = {
    "scopes-block": [f"{SCOPES_NOT_A_MAPPING} a string"],
    "scope-body": ["Scope 'eu' must be an object with a 'name', but was a string"],
}


@pytest.mark.parametrize("reference", list(UNDECLARED_VARIABLE_MESSAGES))
@pytest.mark.parametrize("scope_input", list(UNDECLARED_VARIABLE_IN_SCOPE_INPUT))
def test_contract_logs_an_undeclared_variable_in_scope_input(scope_input: str, reference: str):
    _, logs = _parse(_undeclared_variable_file("", UNDECLARED_VARIABLE_IN_SCOPE_INPUT[scope_input], None, reference))
    assert logs.get_errors() == [
        UNDECLARED_VARIABLE_MESSAGES[reference],
        *CONTRACT_ERRORS_AFTER_THE_UNDECLARED_VARIABLE.get(scope_input, []),
    ]


@pytest.mark.parametrize("reference", list(UNDECLARED_VARIABLE_MESSAGES))
@pytest.mark.parametrize(
    "scope_value", list(UNDECLARED_VARIABLE_IN_CHECK_SCOPE.values()), ids=list(UNDECLARED_VARIABLE_IN_CHECK_SCOPE)
)
@pytest.mark.parametrize("impl_class", [ScopeUnsupportedImpl, ContractImpl], ids=["unsupported-kind", "contract"])
def test_an_undeclared_variable_in_a_check_scope_value(
    monkeypatch, impl_class: type[CheckCollectionImpl], scope_value: str, reference: str
):
    yaml_str = _undeclared_variable_file(KIND_LINES[impl_class], "scopes: {eu: {name: EU}}\n", scope_value, reference)
    scope = _build_impl(impl_class, yaml_str)[0].all_check_impls[1].scope
    assert scope.key == CHECK_SCOPE_KEY_AS_READ[scope_value].replace("REF", reference)
    assert scope.is_base or not scope.is_active

    result, uploads = _verify(monkeypatch, yaml_str)

    # A kind without scope support never reads 'scope' a second time, so it logs only origin's line. A contract then
    # also reports the scope the longer string names.
    reads = 2 if impl_class is ContractImpl and scope_value == "'eu-REF'" else 1
    messages = [UNDECLARED_VARIABLE_MESSAGES[reference]] * reads
    scope_errors = (
        [f"Check references unknown scope '{scope.key}'. Declared scopes: ['eu']"]
        if impl_class is ContractImpl and scope_value == "'eu-REF'"
        else []
    )
    assert result.status == CheckCollectionStatus.ERROR
    assert result.check_results == []
    assert result.get_errors() == messages + scope_errors
    assert _variable_log_lines(result, uploads) == (messages, [messages])
    assert _uploaded_checks(uploads) == [[]]
