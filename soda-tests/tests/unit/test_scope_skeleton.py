"""The scope skeleton: the ``Scope`` model, the parse of ``scopes`` and ``scope``,
``scope_for``, the inactive skip and the unscoped identity.

A declared scope stays inactive in core, so its checks go up as NOT_EVALUATED, and a file
without ``scopes`` and ``scope`` behaves exactly as before. A kind without scope support
fails a file that sets either. A contract validates its scope input, see
``contract_yaml/test_scopes_parsing.py``, and reports what the odd files here get wrong.

Every test here drops the soda-scopes extension, so the file pins core alone even where soda-scopes is installed.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Optional

import duckdb
import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from helpers.scope_test_kinds import SCOPE_UNSUPPORTED_KIND
from helpers.scopes_extension_removal import without_scopes_extension  # noqa: F401
from helpers.test_functions import dedent_and_strip
from helpers.test_table import TestTableSpecification
from soda_core.check_collections.base import CheckCollectionImpl, CheckCollectionYaml
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.filtered_cte import build_filtered_cte
from soda_core.common.logs import Logs
from soda_core.common.sql_ast import SODA_FILTERED_CTE_NAME
from soda_core.common.yaml import ContractYamlSource, YamlObject
from soda_core.contracts.contract_verification import CheckCollectionStatus, CheckOutcome, ContractVerificationSession
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_core.contracts.impl.contract_verification_impl import CheckCollectionImplExtension, CheckImpl, ContractImpl
from soda_core.contracts.impl.contract_yaml import ContractYaml
from soda_core.contracts.impl.scope import BASE_SCOPE_KEY, INVALID_SCOPE_KEY, Scope, ScopeYaml, unsupported_scopes_error
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

pytestmark = pytest.mark.usefixtures("without_scopes_extension")

UNSUPPORTED_KIND_LINE: str = f"kind: {SCOPE_UNSUPPORTED_KIND}\n"
UNSUPPORTED: str = unsupported_scopes_error(SCOPE_UNSUPPORTED_KIND)


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
    base = Scope(key=BASE_SCOPE_KEY)
    eu = Scope(key="eu", name="EU", filter="region = 'eu'")
    assert base.is_base and not eu.is_base
    assert not base.is_active and not eu.is_active
    assert (eu.cte, eu.row_count_metric, eu.check_attributes) == (None, None, {})

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


SCOPES_NULL: str = "YAML key 'scopes' must not be null"
SCOPES_A_LIST: str = "YAML key 'scopes' expected one of ['dict'], but was YAML list"


@pytest.mark.parametrize(
    "scopes_yaml, contract_error",
    [
        ("", None),
        ("scopes:\n", SCOPES_NULL),
        ("scopes: null\n", SCOPES_NULL),
        ("scopes: {}\n", None),
        ("scopes: []\n", SCOPES_A_LIST),
        ("scopes: [eu, us]\n", SCOPES_A_LIST),
        ("scopes: eu\n", "YAML key 'scopes' expected one of ['dict'], but was str"),
    ],
)
def test_contract_yaml_scopes_without_entries_read_as_no_scopes(scopes_yaml: str, contract_error: Optional[str]):
    # A kind without scope support rejects any 'scopes' key, even an empty one.
    unsupported_errors = [UNSUPPORTED] if scopes_yaml else []
    for kind_line, errors in [
        ("", [contract_error] if contract_error else []),
        (UNSUPPORTED_KIND_LINE, unsupported_errors),
    ]:
        contract_yaml, logs = _parse(f"{kind_line}dataset: ds/db/schema/table\n{scopes_yaml}columns: []\n")
        assert type(contract_yaml.scopes) is dict and contract_yaml.scopes == {}
        assert logs.get_logs() == errors


def test_scope_yaml_fields_and_scope_from_yaml():
    # Read with ScopeYaml alone, without the rest of the contract.
    logs = Logs()
    yaml_object: YamlObject = ContractYamlSource.from_str(
        dedent_and_strip(
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
    ).parse()
    scopes = ScopeYaml.parse_scopes(yaml_object)
    logs.close()
    # Reading scopes logs each bad key and each value of the wrong type, and still reads every scope.
    assert logs.get_logs() == [
        "Scope 'text' must be an object with a 'name', but was a string",
        "YAML key 'name' expected one of ['str'], but was int",
        "YAML key 'description' expected one of ['str'], but was YAML list",
        "YAML key 'filter' expected one of ['str'], but was int",
        "YAML key 'check_attributes' expected one of ['dict'], but was YAML list",
        "YAML key 'schedule' expected one of ['dict'], but was str",
        "YAML key 'cron' expected one of ['str'], but was int",
        "YAML key 'timezone' expected one of ['str'], but was YAML list",
        "YAML key 'variables' expected one of ['dict'], but was str",
        "Invalid scope key true: a scope key must be a string, but YAML reads this one as a boolean",
        "Invalid scope key null: a scope key must be a string, but YAML reads this one as null",
        "Invalid scope key 0: a scope key must be a string, but YAML reads this one as a number",
        "Invalid scope key 'yes': 'yes' is reserved, because YAML parsers can read it as a boolean or null",
        "Invalid scope key 'base': 'base' is reserved for the checks without a scope",
        "Invalid scope key us: a scope key must be a string, but YAML reads this one as a tagged value",
    ]
    assert type(scopes) is dict
    assert all(isinstance(scope_yaml, ScopeYaml) for scope_yaml in scopes.values())
    keys = list(scopes)
    assert keys[:5] == ["eu", "bare", "text", "wrong_types", "wrong_schedule"]
    # Each key is kept exactly as ruamel returned it.
    assert keys[5:10] == [True, None, 0, "yes", "base"]
    assert type(keys[5]) is bool and isinstance(keys[7], int)
    assert not isinstance(keys[10], str) and scopes[keys[10]].name == "US"
    assert [scope_yaml.key for scope_yaml in scopes.values()] == keys

    eu = scopes["eu"]
    assert (eu.key, eu.name, eu.description, eu.filter) == ("eu", "Europe", "EU orders", "region = 'eu'")
    assert eu.check_attributes == {"team": "finance"}
    assert (eu.schedule.cron, eu.schedule.timezone) == ("0 6 * * *", "Europe/Brussels")
    assert eu.schedule.variables == {"LOOKBACK": 7}
    assert eu.schedule.location is not None
    assert eu.location.line is not None
    assert str(eu.location) == str(yaml_object.read_value("scopes").create_location_from_yaml_dict_key("eu"))

    # A value of the wrong type reads as None, or {} for check_attributes. A body that is not a mapping has no object.
    for key, name in [("bare", "Bare"), ("text", None), ("wrong_types", None)]:
        scope_yaml = scopes[key]
        assert (scope_yaml.name, scope_yaml.description, scope_yaml.filter) == (name, None, None), key
        assert (scope_yaml.check_attributes, scope_yaml.schedule) == ({}, None), key
        assert (scope_yaml.scope_yaml_object is None) == (key == "text"), key
    wrong_schedule = scopes["wrong_schedule"].schedule
    assert [wrong_schedule.cron, wrong_schedule.timezone, wrong_schedule.variables] == [None] * 3

    scope = Scope.from_yaml(eu)
    assert (scope.key, scope.name, scope.description, scope.filter) == ("eu", "Europe", "EU orders", "region = 'eu'")
    assert scope.check_attributes == {"team": "finance"}
    assert scope.schedule is eu.schedule
    assert scope.cte is None and scope.row_count_metric is None
    with pytest.raises(TypeError):
        Scope.from_yaml(ScopeYaml(key=True, scope_yaml_object=None, location=None))


@pytest.mark.parametrize(
    "source_body",
    ["    <<: *src\n", "    <<: *src\n    filter: id > 1\n"],
    ids=["merge-keys-only", "merge-and-own-keys"],
)
def test_a_merge_key_outside_scopes_reads_like_a_written_one(source_body: str):
    # ruamel keeps no position for a key merged in with '<<'. Every read falls back to the mapping's location, so a
    # reconciliation source that comes in through a merge key reads instead of raising.
    head = "dataset: ds/db/schema/table\nx-src: &src {dataset: ds/db/schema/source}\nreconciliation:\n  source:\n"
    contract_yaml, _ = _parse(f"{head}{source_body}columns: []\n")
    source = contract_yaml.yaml_object.read_object_opt("reconciliation").read_object_opt("source")
    logs = Logs()
    try:
        assert source.read_string("dataset") == "ds/db/schema/source"
    finally:
        logs.close()
    assert logs.get_errors() == []


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
    yaml_str = f"dataset: ds/db/schema/table\ncolumns: []\nchecks:\n  - row_count:\n{body}"
    scope = _parse(yaml_str)[0].checks[0].scope
    assert scope == expected
    if isinstance(expected, bool):
        assert type(scope) is bool
    if isinstance(expected, (dict, list)):
        assert isinstance(scope, type(expected))


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


def test_a_contract_resolves_variables_in_scope_input():
    contract_yaml, logs = _parse(dedent_and_strip(SCOPE_INPUT_WITH_A_VARIABLE))

    eu = contract_yaml.scopes["eu"]
    assert (eu.name, eu.description, eu.filter) == ("eu", "in eu", "region = 'eu'")
    assert eu.check_attributes == {"team": "eu"}
    assert (eu.schedule.cron, eu.schedule.timezone, eu.schedule.variables) == ("eu", "eu", {"LOOKBACK": "eu"})
    assert not logs.has_errors


@pytest.mark.parametrize(
    "kind",
    [SCOPE_UNSUPPORTED_KIND, "nobody-registered-this"],
    ids=["kind-without-support", "unregistered-kind"],
)
def test_a_kind_without_scope_support_rejects_scopes_and_check_scopes(kind: str):
    contract_yaml, logs = _parse(
        f"kind: {kind}\n"
        + dedent_and_strip(SCOPE_INPUT_WITH_A_VARIABLE)
        + "\nchecks:\n  - row_count:\n  - row_count: {qualifier: eu, scope: eu}\n"
    )

    assert contract_yaml.scopes == {}
    assert logs.get_errors() == [unsupported_scopes_error(kind)] * 2
    assert all(record.location is not None for record in logs.gatherer.get_error_logs())


def test_a_kind_with_its_own_yaml_class_rejects_scopes():
    class _NeverReadsScopesYaml(CheckCollectionYaml):
        """Runs the base ``__init__`` only, like the metric-monitoring yamls."""

    logs = Logs()
    yaml = _NeverReadsScopesYaml(
        yaml_source=ContractYamlSource.from_str(f"{UNSUPPORTED_KIND_LINE}dataset: ds/db/schema/table\nscopes: {{}}\n")
    )
    logs.close()
    assert dict(yaml.scopes) == {}
    assert logs.get_errors() == [UNSUPPORTED]


# Two checks in the base scope, then a mix of one check in a declared scope and six that get a placeholder. A tagged
# value is not a string, so neither '!tag base' nor '!tag eu' names a scope.
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
# The scope keys of checks 2 to 8. A placeholder keeps a valid key that names no declared scope.
SCOPE_KEYS: list[str] = [INVALID_SCOPE_KEY, "eu", INVALID_SCOPE_KEY, "undeclared"] + [INVALID_SCOPE_KEY] * 3
# A contract reports the two keys that are no scope keys and every check scope that names no declared scope.
BASE_RESERVED: str = "'base' is reserved for the checks without a scope"
SCOPE_FOR_ERRORS: list[str] = [
    "Invalid scope key true: a scope key must be a string, but YAML reads this one as a boolean",
    f"Invalid scope key 'base': {BASE_RESERVED}",
    f"Invalid check scope 'base': {BASE_RESERVED}",
    "Check 'scope' must name a declared scope, but was a tagged value: base",
    "Check 'scope' must name a declared scope, but was a tagged value: eu",
    "Check references unknown scope 'undeclared'. Declared scopes: ['eu', 'us']",
    "Check 'scope' must name a declared scope, but was a number: 5",
    "Check 'scope' must name a declared scope, but was an object",
    "Check 'scope' must name a declared scope, but was a list",
]


def test_scope_for():
    impl, logs = _build_impl(ContractImpl, SCOPE_FOR_YAML)
    assert logs.get_errors() == SCOPE_FOR_ERRORS

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
    assert all(check_impl.scope is impl.base_scope for check_impl in check_impls[:2])
    assert check_impls[3].scope is impl.scopes["eu"]
    assert [check_impl.scope.key for check_impl in check_impls[2:]] == SCOPE_KEYS
    placeholders = [check_impl.scope for index, check_impl in enumerate(check_impls) if index > 1 and index != 3]
    for placeholder in placeholders:
        assert not placeholder.is_active and not placeholder.is_base
        assert all(placeholder is not scope for scope in impl.scopes.values())

    # A check in an inactive scope builds no metrics.
    assert [check_impl.in_inactive_scope for check_impl in check_impls] == [False] * 2 + [True] * 7
    assert [bool(check_impl.metrics) for check_impl in check_impls] == [True] * 2 + [False] * 7
    # A selector that picks the check in the inactive scope does not make it build metrics.
    selector = CheckSelector.parse("qualifier=eu")
    selected_impl, _ = _build_impl(ContractImpl, SCOPE_FOR_YAML, [selector])
    assert not any(check_impl.metrics for check_impl in selected_impl.all_check_impls)

    check_infos = [check_impl._build_check_info() for check_impl in check_impls]
    assert [check.scope for check in check_infos] == [None] * 2 + SCOPE_KEYS


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
    assert [check_impl.in_inactive_scope for check_impl in impl.all_check_impls] == [False, False, True, False] + [
        True
    ] * 5
    assert logs.get_errors() == SCOPE_FOR_ERRORS


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


@pytest.mark.parametrize(
    "scoped, other",
    [
        # After 'q' the scope term would meet the free-text qualifier, and after 'ds' the column name.
        ({"qualifier": "x", "scope_key": "eu"}, {"qualifier": "xscopeeu"}),
        ({"column_name": "cx", "scope_key": "eu"}, {"column_name": "x", "scope_key": "euc"}),
        ({"scope_key": "eu"}, {"scope_key": "us"}),
        ({"scope_key": "eu"}, {}),
        ({"scope_key": INVALID_SCOPE_KEY}, {}),
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


def test_a_check_in_an_inactive_scope_is_not_evaluated(data_source_test_helper: DataSourceTestHelper):
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
        ("eu", CheckOutcome.NOT_EVALUATED),
    ]
    assert result.number_of_checks_excluded == 0
    assert result.get_errors() == [NOT_EVALUATING_ONE_EU_CHECK]


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


# Odd scope input that review found. Each file has one unscoped and one scoped check. ruamel keeps no position for a merged or tagged key, str() rejects a huge int,
# UTF-8 a lone surrogate, a copy recurses too far on a deep value and expands shared anchors, and a variable inside a
# value that is not a string is never resolved.
ODD_SCOPE_FILES: dict[str, str] = {
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


# What a contract reports for each of those files. Only the merged keys are valid scope input.
KEY_PATTERN_REASON: str = (
    "a scope key starts with a lowercase letter, followed by at most 63 lowercase letters, digits, '_' or '-'"
)
NOT_A_DECLARED_SCOPE: str = "Check 'scope' must name a declared scope, but was"
CONTRACT_ERRORS_FOR_ODD_SCOPE_FILES: dict[str, list[str]] = {
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
        "YAML value is nested too deeply to read",
        "Check references unknown scope 'eu'. No scopes are declared",
    ],
    "deep-scope-body-value": [
        "Unknown key 'x_anchors' in scope 'eu'",
        "YAML key 'filter' expected one of ['str'], but was YAML list",
    ],
    "deep-schedule-value": [
        "Unknown key 'x_anchors' in the schedule of scope 'eu'",
        "YAML key 'variables' expected one of ['dict'], but was YAML list",
    ],
    "shared-anchors": [f"{NOT_A_DECLARED_SCOPE} a list"],
    "variable-in-a-list": [f"{NOT_A_DECLARED_SCOPE} a list"],
    "variable-in-a-mapping": [f"{NOT_A_DECLARED_SCOPE} an object"],
    "variable-in-a-nested-list": [f"{NOT_A_DECLARED_SCOPE} a list"],
}


NOT_EVALUATING_ONE_EU_CHECK: str = (
    "Not evaluating 1 check in scope 'eu': running checks in a scope needs a Soda extension that runs scopes."
)


def _assert_scoped_identity_is_stable(impl_class: type[CheckCollectionImpl], impl, yaml_str: str) -> None:
    unscoped, scoped = impl.all_check_impls
    assert not scoped.scope.is_base
    assert scoped.identity != unscoped.identity
    assert scoped.identity == _build_impl(impl_class, yaml_str)[0].all_check_impls[1].identity


@pytest.mark.parametrize(
    "file_key", ["merge-key-in-scope-body", "merge-keys-only-in-check-body", "huge-int", "lone-surrogate"]
)
def test_a_kind_without_scope_support_fails_the_file(monkeypatch, file_key: str):
    result, uploads = _verify(monkeypatch, UNSUPPORTED_KIND_LINE + ODD_SCOPE_FILES[file_key])
    assert UNSUPPORTED in result.get_errors()
    assert result.status == CheckCollectionStatus.ERROR
    assert result.check_results == []
    assert _uploaded_checks(uploads) == [[]]


@pytest.mark.parametrize("file_key", list(ODD_SCOPE_FILES))
def test_a_contract_reports_odd_scope_input(monkeypatch, file_key: str):
    yaml_str = ODD_SCOPE_FILES[file_key]
    errors = CONTRACT_ERRORS_FOR_ODD_SCOPE_FILES[file_key]
    impl, logs = _build_impl(ContractImpl, yaml_str)
    assert logs.get_errors() == errors
    _assert_scoped_identity_is_stable(ContractImpl, impl, yaml_str)

    result, uploads = _verify(monkeypatch, yaml_str)
    assert result.error is None, repr(result.error)
    assert result.status == CheckCollectionStatus.ERROR
    if errors:
        assert result.get_errors() == errors
        assert result.check_results == []
        assert _uploaded_checks(uploads) == [[]]
    else:
        # A valid file: core alone runs no declared scope, so its check is not evaluated, with one error.
        assert result.get_errors() == [NOT_EVALUATING_ONE_EU_CHECK]
        outcomes = [check_result.outcome for check_result in result.check_results]
        assert outcomes == [CheckOutcome.PASSED, CheckOutcome.NOT_EVALUATED]
        assert [len(upload["checks"]) for upload in uploads] == [2]


def test_scope_reads_of_odd_scope_input():
    def scopes(file_key: str) -> dict:
        return _parse(ODD_SCOPE_FILES[file_key])[0].scopes

    # Merged keys read like written ones, and ruamel puts them after the mapping's own keys.
    eu = scopes("merge-key-in-scope-body")["eu"]
    assert (eu.name, eu.check_attributes, eu.filter) == ("Shared", {"team": "finance"}, "id > 1")
    schedule = scopes("merge-key-in-scope-schedule")["eu"].schedule
    assert (schedule.cron, schedule.timezone) == ("0 6 * * *", "UTC")
    assert [(key, scope.name) for key, scope in scopes("merge-key-in-scopes").items()] == [("eu", "EU"), ("us", "US")]
    eu = scopes("merge-keys-only-in-scope-body")["eu"]
    assert (eu.name, eu.filter) == ("Shared", "id > 1")
    assert _parse(ODD_SCOPE_FILES["merge-keys-only-in-check-body"])[0].checks[1].scope == "eu"
    assert [(key, scope.name) for key, scope in scopes("merge-key-at-root").items()] == [("eu", "EU")]

    # A deep check scope stays as parsed: never None, which would run the check unscoped, and never a string, so it
    # can never name a declared scope.
    for file_key in ["deep-check-scope", "deep-check-scope-past-str"]:
        unscoped, scoped = _parse(ODD_SCOPE_FILES[file_key])[0].checks
        assert unscoped.scope is None
        assert isinstance(scoped.scope, list), file_key
    # Such a value in a scope body or a schedule reads as absent when the mapping around it still copies, and a whole
    # 'scopes' block that does not copy reads as no scopes.
    eu = scopes("deep-scope-body-value")["eu"]
    assert (eu.name, eu.filter) == ("EU", None)
    schedule = scopes("deep-schedule-value")["eu"].schedule
    assert (schedule.cron, schedule.variables) == ("0 6 * * *", None)
    assert scopes("deep-scopes-block") == {}


# Each place in the scope input where a variable can be used, with 'REF' standing for the reference.
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
# A check's scope value holding the reference, alone or inside a longer string. A scope key is fixed, so the contract
# rejects both. The read of the check body still resolves variables one level deep, so it logs the reference once.
UNDECLARED_VARIABLE_IN_CHECK_SCOPE: dict[str, str] = {
    "check-scope": "'REF'",
    "check-scope-inside-a-string": "'eu-REF'",
}
UNDECLARED_VARIABLE_MESSAGES: dict[str, str] = {
    "${var.NOPE}": "Variable 'NOPE' was used and not declared",
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


# A contract also rejects a 'scopes' block or a scope body that is a string, whatever the string holds. A 'scopes'
# block that is not a mapping is never read, so its reference is never resolved.
CONTRACT_ERRORS_AFTER_THE_UNDECLARED_VARIABLE: dict[str, list[str]] = {
    "scopes-block": ["YAML key 'scopes' expected one of ['dict'], but was SingleQuotedScalarString"],
    "scope-body": ["Scope 'eu' must be an object with a 'name', but was a string"],
}
SCOPE_INPUT_NEVER_RESOLVED: frozenset[str] = frozenset({"scopes-block"})


@pytest.mark.parametrize("reference", list(UNDECLARED_VARIABLE_MESSAGES))
@pytest.mark.parametrize("scope_input", list(UNDECLARED_VARIABLE_IN_SCOPE_INPUT))
def test_contract_logs_an_undeclared_variable_in_scope_input(scope_input: str, reference: str):
    _, logs = _parse(_undeclared_variable_file("", UNDECLARED_VARIABLE_IN_SCOPE_INPUT[scope_input], None, reference))
    resolve_messages = [] if scope_input in SCOPE_INPUT_NEVER_RESOLVED else [UNDECLARED_VARIABLE_MESSAGES[reference]]
    assert logs.get_errors() == [*resolve_messages, *CONTRACT_ERRORS_AFTER_THE_UNDECLARED_VARIABLE.get(scope_input, [])]


@pytest.mark.parametrize("reference", list(UNDECLARED_VARIABLE_MESSAGES))
@pytest.mark.parametrize(
    "scope_value", list(UNDECLARED_VARIABLE_IN_CHECK_SCOPE.values()), ids=list(UNDECLARED_VARIABLE_IN_CHECK_SCOPE)
)
def test_a_check_scope_cannot_use_a_variable(monkeypatch, scope_value: str, reference: str):
    yaml_str = _undeclared_variable_file("", "scopes: {eu: {name: EU}}\n", scope_value, reference)
    scope = _build_impl(ContractImpl, yaml_str)[0].all_check_impls[1].scope
    assert scope.key == INVALID_SCOPE_KEY and not scope.is_active

    result, uploads = _verify(monkeypatch, yaml_str)

    # A check's 'scope' is never resolved, so the reference only fails as a scope, with no resolver error.
    written = scope_value.strip("'").replace("REF", reference)
    assert result.status == CheckCollectionStatus.ERROR
    assert result.check_results == []
    assert result.get_errors() == [
        f"Check 'scope' cannot use a variable, but was '{written}'. Name a declared scope key"
    ]
    assert _variable_log_lines(result, uploads) == ([], [[]])
    assert _uploaded_checks(uploads) == [[]]
