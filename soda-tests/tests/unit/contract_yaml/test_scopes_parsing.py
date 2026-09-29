"""Validation of ``scopes``, a check's ``scope`` and scope schedules.

A contract checks its scope input while the YAML is parsed, so ``soda contract test`` and publication report
the same errors. Each error names the key it is about, carries its location and ends the file with errors, which
``soda contract test`` reports with exit code 3. A kind without scope support never validates scope input.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pytest
from helpers.scope_test_kinds import SCOPE_UNSUPPORTED_KIND, ScopeUnsupportedImpl
from helpers.test_functions import dedent_and_strip
from ruamel.yaml import YAML
from soda_core.check_collections.base import CheckCollectionImpl
from soda_core.cli import cli
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers import contract as contract_handlers
from soda_core.cli.handlers.contract import handle_test_contract
from soda_core.common.exceptions import YamlParserException
from soda_core.common.logging_constants import ExtraKeys
from soda_core.common.logs import Logs
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.api import test_api
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_core.contracts.impl.contract_verification_impl import CheckImpl, ContractImpl
from soda_core.contracts.impl.contract_yaml import CheckYaml, ContractYaml
from soda_core.contracts.impl.scope import RESERVED_SCOPE_KEYS, scope_key_error

SCOPE_KEYS_FIXTURE_PATH: Path = Path(__file__).parent.parent / "fixtures" / "scope_keys.yml"

KIND_LINE_WITHOUT_SCOPE_SUPPORT: str = f"kind: {SCOPE_UNSUPPORTED_KIND}\n"

KEY_PATTERN_REASON: str = (
    "a scope key starts with a lowercase letter, followed by at most 63 lowercase letters, digits, '_' or '-'"
)
BASE_REASON: str = "'base' is reserved for the checks without a scope"


def _reserved_reason(word: str) -> str:
    return f"'{word}' is reserved, because YAML parsers can read it as a boolean or null"


def _contract(scopes_block: str = "", check_body: str = "", kind_line: str = "") -> str:
    """A contract with an unscoped check and a check with ``check_body`` under ``scopes_block``."""
    return (
        f"{kind_line}dataset: ds/db/schema/table\n{scopes_block}columns: []\n"
        f"checks:\n  - row_count:\n  - row_count:\n      qualifier: scoped\n{check_body}"
    )


def _parse(yaml_str: str) -> tuple[ContractYaml, Logs]:
    """``ContractYaml`` of ``yaml_str``, parsed without a session the way publication parses it."""
    logs = Logs()
    try:
        return ContractYaml.parse(yaml_source=ContractYamlSource.from_str(yaml_str)), logs
    finally:
        logs.close()


def _build_contract_impl(
    yaml_str: str, check_selectors: Optional[list[CheckSelector]] = None, impl_class=ContractImpl
) -> tuple[CheckCollectionImpl, Logs]:
    """``impl_class`` of ``yaml_str`` built without a data source, the way 'soda contract test' builds it."""
    logs = Logs()
    try:
        yaml = ContractYaml.parse(yaml_source=ContractYamlSource.from_str(yaml_str))
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


def _soda_contract_test(monkeypatch, tmp_path: Path, yaml_str: str) -> tuple[ExitCode, list[str]]:
    """The exit code of 'soda contract test' on ``yaml_str`` and the errors of the file."""
    monkeypatch.setattr(test_api.soda_telemetry, "ingest_contract_verification_session_result", lambda **_: None)
    session_results: list = []

    def recording_test_contract(**kwargs):
        session_result = test_api.test_contract(**kwargs)
        session_results.append(session_result)
        return session_result

    monkeypatch.setattr(contract_handlers, "test_contract", recording_test_contract)
    contract_file = tmp_path / "contract.yml"
    contract_file.write_text(yaml_str)
    exit_code = handle_test_contract(contract_file_path=str(contract_file), variables=None)
    [session_result] = session_results
    return exit_code, session_result.get_errors()


# Scope input a contract rejects: the scopes block, the check's scope lines and the errors, in order.
INVALID_SCOPE_INPUT: dict[str, tuple[str, str, list[str]]] = {
    # Keys of 'scopes'
    "key-with-an-uppercase-letter": (
        "scopes:\n  Eu: {name: EU}\n",
        "",
        [f"Invalid scope key 'Eu': {KEY_PATTERN_REASON}"],
    ),
    "key-starting-with-a-digit": (
        'scopes:\n  "9a": {name: A}\n',
        "",
        [f"Invalid scope key '9a': {KEY_PATTERN_REASON}"],
    ),
    "key-with-a-dot": ("scopes:\n  eu.west: {name: A}\n", "", [f"Invalid scope key 'eu.west': {KEY_PATTERN_REASON}"]),
    "key-of-65-characters": (
        f"scopes:\n  {'a' * 65}: {{name: A}}\n",
        "",
        [f"Invalid scope key '{'a' * 65}': {KEY_PATTERN_REASON}"],
    ),
    "key-base": ("scopes:\n  base: {name: Base}\n", "", [f"Invalid scope key 'base': {BASE_REASON}"]),
    **{
        f"reserved-key-{word}": (
            f'scopes:\n  "{word}": {{name: X}}\n',
            "",
            [f"Invalid scope key '{word}': {_reserved_reason(word)}"],
        )
        for word in sorted(RESERVED_SCOPE_KEYS - {"base"})
    },
    "key-true": (
        "scopes:\n  true: {name: T}\n",
        "",
        ["Invalid scope key true: a scope key must be a string, but YAML reads this one as a boolean"],
    ),
    "key-null": (
        "scopes:\n  null: {name: N}\n",
        "",
        ["Invalid scope key null: a scope key must be a string, but YAML reads this one as null"],
    ),
    "key-zero": (
        "scopes:\n  0: {name: Z}\n",
        "",
        ["Invalid scope key 0: a scope key must be a string, but YAML reads this one as a number"],
    ),
    # The scopes block
    "scopes-empty-value": (
        "scopes:\n",
        "",
        ["'scopes' must be an object that maps scope keys to scopes, but was null"],
    ),
    "scopes-null": ("scopes: null\n", "", ["'scopes' must be an object that maps scope keys to scopes, but was null"]),
    "scopes-list": (
        "scopes: [eu]\n",
        "",
        ["'scopes' must be an object that maps scope keys to scopes, but was a list"],
    ),
    "scopes-string": (
        "scopes: eu\n",
        "",
        ["'scopes' must be an object that maps scope keys to scopes, but was a string"],
    ),
    # A check's scope
    "unknown-scope": (
        "scopes:\n  eu: {name: EU}\n  us: {name: US}\n",
        "      scope: apac\n",
        ["Check references unknown scope 'apac'. Declared scopes: ['eu', 'us']"],
    ),
    "unknown-scope-without-scopes": (
        "",
        "      scope: eu\n",
        ["Check references unknown scope 'eu'. No scopes are declared"],
    ),
    "scope-base": (
        "scopes:\n  eu: {name: EU}\n",
        "      scope: base\n",
        [f"Invalid check scope 'base': {BASE_REASON}"],
    ),
    "scope-reserved-word": (
        "scopes:\n  eu: {name: EU}\n",
        "      scope: 'yes'\n",
        [f"Invalid check scope 'yes': {_reserved_reason('yes')}"],
    ),
    "scope-null": (
        "scopes:\n  eu: {name: EU}\n",
        "      scope: null\n",
        ["Check 'scope' must name a declared scope, but was null"],
    ),
    "scope-empty-value": (
        "scopes:\n  eu: {name: EU}\n",
        "      scope:\n",
        ["Check 'scope' must name a declared scope, but was null"],
    ),
    # A reference in a namespace the engine does not know reads as null without an error of its own.
    "scope-reference-in-an-unknown-namespace": (
        "scopes:\n  eu: {name: EU}\n",
        "      scope: ${nope.SCOPE}\n",
        ["Check 'scope' must name a declared scope, but '${nope.SCOPE}' resolved to null"],
    ),
    "scope-number": (
        "scopes:\n  eu: {name: EU}\n",
        "      scope: 5\n",
        ["Check 'scope' must name a declared scope, but was a number: 5"],
    ),
    "scope-boolean": (
        "scopes:\n  eu: {name: EU}\n",
        "      scope: true\n",
        ["Check 'scope' must name a declared scope, but was a boolean: true"],
    ),
    "scope-mapping": (
        "scopes:\n  eu: {name: EU}\n",
        "      scope: {eu: 1}\n",
        ["Check 'scope' must name a declared scope, but was an object"],
    ),
    "scope-list": (
        "scopes:\n  eu: {name: EU}\n",
        "      scope: [eu]\n",
        ["Check 'scope' must name a declared scope, but was a list"],
    ),
    # A scope's body
    "unknown-key-in-scope": ("scopes:\n  eu: {name: EU, owner: finance}\n", "", ["Unknown key 'owner' in scope 'eu'"]),
    "scope-without-name": ("scopes:\n  eu: {description: Europe}\n", "", ["Scope 'eu' has no 'name'"]),
    "scope-body-empty-value": ("scopes:\n  eu:\n", "", ["Scope 'eu' must be an object with a 'name', but was null"]),
    "scope-body-string": (
        "scopes:\n  eu: Europe\n",
        "",
        ["Scope 'eu' must be an object with a 'name', but was a string"],
    ),
    "name-number": ("scopes:\n  eu: {name: 5}\n", "", ["'name' of scope 'eu' must be a string, but was a number"]),
    "name-null": ("scopes:\n  eu: {name: null}\n", "", ["'name' of scope 'eu' must be a string, but was null"]),
    "description-list": (
        "scopes:\n  eu: {name: EU, description: [a]}\n",
        "",
        ["'description' of scope 'eu' must be a string, but was a list"],
    ),
    "filter-number": (
        "scopes:\n  eu: {name: EU, filter: 5}\n",
        "",
        ["'filter' of scope 'eu' must be a string, but was a number"],
    ),
    "filter-boolean": (
        "scopes:\n  eu: {name: EU, filter: true}\n",
        "",
        ["'filter' of scope 'eu' must be a string, but was a boolean"],
    ),
    "filter-null": (
        "scopes:\n  eu: {name: EU, filter: null}\n",
        "",
        ["'filter' of scope 'eu' must be a string, but was null"],
    ),
    "check-attributes-list": (
        "scopes:\n  eu: {name: EU, check_attributes: [a]}\n",
        "",
        ["'check_attributes' of scope 'eu' must be an object, but was a list"],
    ),
    # A scope's schedule
    "schedule-null": (
        "scopes:\n  eu: {name: EU, schedule: null}\n",
        "",
        ["'schedule' of scope 'eu' must be an object with a 'cron', but was null"],
    ),
    "schedule-inherit": (
        "scopes:\n  eu: {name: EU, schedule: inherit}\n",
        "",
        ["'schedule' of scope 'eu' must be an object with a 'cron', but was a string"],
    ),
    "schedule-weekly": (
        'scopes:\n  eu: {name: EU, schedule: "weekly"}\n',
        "",
        ["'schedule' of scope 'eu' must be an object with a 'cron', but was a string"],
    ),
    "schedule-without-cron": (
        "scopes:\n  eu: {name: EU, schedule: {timezone: UTC}}\n",
        "",
        ["The schedule of scope 'eu' has no 'cron'"],
    ),
    "schedule-unknown-key": (
        "scopes:\n  eu: {name: EU, schedule: {cron: '0 6 * * *', every: day}}\n",
        "",
        ["Unknown key 'every' in the schedule of scope 'eu'"],
    ),
    "schedule-cron-number": (
        "scopes:\n  eu: {name: EU, schedule: {cron: 6}}\n",
        "",
        ["'cron' in the schedule of scope 'eu' must be a string, but was a number"],
    ),
    "schedule-timezone-list": (
        "scopes:\n  eu: {name: EU, schedule: {cron: '0 6 * * *', timezone: [UTC]}}\n",
        "",
        ["'timezone' in the schedule of scope 'eu' must be a string, but was a list"],
    ),
    "schedule-variables-list": (
        "scopes:\n  eu: {name: EU, schedule: {cron: '0 6 * * *', variables: [A]}}\n",
        "",
        ["'variables' in the schedule of scope 'eu' must be an object, but was a list"],
    ),
    "schedule-variable-boolean": (
        "scopes:\n  eu: {name: EU, schedule: {cron: '0 6 * * *', variables: {FLAG: true}}}\n",
        "",
        ["Variable 'FLAG' in the schedule of scope 'eu' must be a string or a number, but was a boolean"],
    ),
    "schedule-variable-list": (
        "scopes:\n  eu: {name: EU, schedule: {cron: '0 6 * * *', variables: {DAYS: [1, 2]}}}\n",
        "",
        ["Variable 'DAYS' in the schedule of scope 'eu' must be a string or a number, but was a list"],
    ),
    "schedule-variable-null": (
        "scopes:\n  eu: {name: EU, schedule: {cron: '0 6 * * *', variables: {LOOKBACK: null}}}\n",
        "",
        ["Variable 'LOOKBACK' in the schedule of scope 'eu' must be a string or a number, but was null"],
    ),
    "schedule-variable-name-number": (
        "scopes:\n  eu: {name: EU, schedule: {cron: '0 6 * * *', variables: {1: a}}}\n",
        "",
        ["Variable name 1 in the schedule of scope 'eu' must be a string, but YAML reads it as a number"],
    ),
}


def _invalid_contract(case: str, kind_line: str = "") -> str:
    scopes_block, check_body, _ = INVALID_SCOPE_INPUT[case]
    return _contract(scopes_block, check_body, kind_line)


@pytest.mark.parametrize("case", list(INVALID_SCOPE_INPUT))
def test_invalid_scope_input_is_an_error_where_the_yaml_is_parsed(case: str):
    _, logs = _parse(_invalid_contract(case))
    assert logs.get_errors() == INVALID_SCOPE_INPUT[case][2]
    for record in logs.gatherer.get_error_logs():
        location = getattr(record, ExtraKeys.LOCATION, None)
        assert location is not None and location.line is not None, record.getMessage()


@pytest.mark.parametrize("case", list(INVALID_SCOPE_INPUT))
def test_invalid_scope_input_fails_soda_contract_test(monkeypatch, tmp_path, case: str):
    exit_code, errors = _soda_contract_test(monkeypatch, tmp_path, _invalid_contract(case))
    assert exit_code == ExitCode.LOG_ERRORS
    assert errors == INVALID_SCOPE_INPUT[case][2]


@pytest.mark.parametrize("case", list(INVALID_SCOPE_INPUT))
def test_a_kind_without_scope_support_reads_invalid_scope_input_as_written(monkeypatch, tmp_path, case: str):
    yaml_str = _invalid_contract(case, KIND_LINE_WITHOUT_SCOPE_SUPPORT)
    _, logs = _parse(yaml_str)
    assert logs.get_logs() == []
    _, logs = _build_contract_impl(yaml_str, impl_class=ScopeUnsupportedImpl)
    assert logs.get_errors() == []
    assert _soda_contract_test(monkeypatch, tmp_path, yaml_str) == (ExitCode.OK, [])


VALID_SCOPE_INPUT: dict[str, tuple[str, str]] = {
    "no-scopes": ("", ""),
    "empty-scopes": ("scopes: {}\n", ""),
    "name-only": ("scopes:\n  eu: {name: EU}\n", "      scope: eu\n"),
    "every-key": (
        "scopes:\n"
        "  eu:\n"
        "    name: Europe\n"
        "    description: EU orders\n"
        "    filter: region = 'eu'\n"
        "    check_attributes: {team: finance, priority: 1}\n"
        "    schedule:\n"
        "      cron: '0 7 * * 1-5'\n"
        "      timezone: Europe/Brussels\n"
        "      variables: {REGION: eu, LOOKBACK: 7, RATIO: 0.5}\n"
        "  release-gate: {name: Release gate}\n",
        "      scope: release-gate\n",
    ),
    "empty-name": ('scopes:\n  eu: {name: ""}\n', "      scope: eu\n"),
    "variables-in-scope-input": (
        "variables: {REGION: {default: eu}, CRON: {default: '0 6 * * *'}}\n"
        "scopes:\n"
        "  eu:\n"
        "    name: ${var.REGION}\n"
        "    filter: region = '${var.REGION}'\n"
        "    schedule: {cron: '${var.CRON}', variables: {REGION: '${var.REGION}'}}\n",
        "      scope: eu\n",
    ),
}


@pytest.mark.parametrize("case", list(VALID_SCOPE_INPUT))
def test_valid_scope_input_passes(monkeypatch, tmp_path, case: str):
    scopes_block, check_body = VALID_SCOPE_INPUT[case]
    yaml_str = _contract(scopes_block, check_body)
    _, logs = _parse(yaml_str)
    assert logs.get_errors() == []
    assert _soda_contract_test(monkeypatch, tmp_path, yaml_str) == (ExitCode.OK, [])


def test_a_schema_check_accepts_a_scope(monkeypatch, tmp_path):
    yaml_str = _contract("scopes:\n  eu: {name: EU}\n").replace("checks:\n", "checks:\n  - schema:\n      scope: eu\n")
    _, logs = _parse(yaml_str)
    assert logs.get_errors() == []
    assert _soda_contract_test(monkeypatch, tmp_path, yaml_str) == (ExitCode.OK, [])


SCOPE_ENVIRONMENT_VARIABLE: str = "SODA_TEST_SCOPE_KEY"


def _environment_scope_contract(kind_line: str = "") -> str:
    return _contract("scopes:\n  eu: {name: EU}\n", f"      scope: ${{env.{SCOPE_ENVIRONMENT_VARIABLE}}}\n", kind_line)


def test_a_check_scope_from_an_unset_environment_variable_is_an_error(monkeypatch, tmp_path):
    # An unset environment variable reads as null without an error of its own. The check must not end up in the base
    # scope, where core runs it over the whole dataset.
    monkeypatch.delenv(SCOPE_ENVIRONMENT_VARIABLE, raising=False)
    errors = [f"Check 'scope' must name a declared scope, but '${{env.{SCOPE_ENVIRONMENT_VARIABLE}}}' resolved to null"]
    _, logs = _parse(_environment_scope_contract())
    assert logs.get_errors() == errors
    [record] = logs.gatherer.get_error_logs()
    assert getattr(record, ExtraKeys.LOCATION).line is not None
    assert _soda_contract_test(monkeypatch, tmp_path, _environment_scope_contract()) == (ExitCode.LOG_ERRORS, errors)

    unsupported: str = _environment_scope_contract(KIND_LINE_WITHOUT_SCOPE_SUPPORT)
    _, logs = _build_contract_impl(unsupported, impl_class=ScopeUnsupportedImpl)
    assert logs.get_errors() == []
    assert _soda_contract_test(monkeypatch, tmp_path, unsupported) == (ExitCode.OK, [])


def test_a_check_scope_from_a_set_environment_variable_is_validated(monkeypatch):
    monkeypatch.setenv(SCOPE_ENVIRONMENT_VARIABLE, "apac")
    _, logs = _parse(_environment_scope_contract())
    assert logs.get_errors() == ["Check references unknown scope 'apac'. Declared scopes: ['eu']"]

    monkeypatch.setenv(SCOPE_ENVIRONMENT_VARIABLE, "eu")
    impl, logs = _build_contract_impl(_environment_scope_contract())
    assert logs.get_errors() == []
    assert [(check_impl.scope.key, check_impl.skip) for check_impl in impl.all_check_impls] == [
        ("base", False),
        ("eu", True),
    ]


def test_a_file_without_scopes_parses_as_before():
    contract_yaml, logs = _parse(_contract())
    assert contract_yaml.scopes == {}
    assert [check_yaml.scope for check_yaml in contract_yaml.checks] == [None, None]
    assert logs.get_logs() == []


def test_a_duplicate_scope_key_fails_soda_contract_test(monkeypatch, tmp_path):
    contract_file = tmp_path / "contract.yml"
    contract_file.write_text(_contract("scopes:\n  eu: {name: A}\n  us: {name: U}\n  eu: {name: B}\n"))

    # The YAML parser raises, and the CLI turns what the handler raises into exit code 3.
    with pytest.raises(YamlParserException) as raised:
        handle_test_contract(contract_file_path=str(contract_file), variables=None)
    # ruamel names the key and marks its second occurrence, on line 5 column 3.
    assert str(raised.value).startswith('YAML syntax error: found duplicate key "eu"'), str(raised.value)
    assert str(raised.value).endswith(f"{contract_file}[5,3]"), str(raised.value)

    monkeypatch.setattr(sys, "argv", ["soda", "contract", "test", "-c", str(contract_file)])
    # The CLI configures logging for the whole process, which the tests after this one rely on.
    monkeypatch.setattr(cli, "_configure_logging", lambda verbose: None)
    with pytest.raises(SystemExit) as exit_info:
        cli.execute()
    assert exit_info.value.code == ExitCode.LOG_ERRORS


def test_the_errors_carry_the_location_of_the_key_they_name():
    yaml_str = _contract(
        "scopes:\n  eu:\n    name: EU\n    owner: finance\n    schedule:\n      timezone: UTC\n",
        "      scope: apac\n",
    )
    _, logs = _parse(yaml_str)
    records = logs.gatherer.get_error_logs()
    # ruamel counts lines and columns from 0.
    assert [(record.getMessage(), record.location.line, record.location.column) for record in records] == [
        ("Unknown key 'owner' in scope 'eu'", 4, 11),
        ("The schedule of scope 'eu' has no 'cron'", 6, 6),
        ("Check references unknown scope 'apac'. Declared scopes: ['eu']", 12, 13),
    ]


# A contract that uses every part of scopes, with its variables declared and referenced the way the engine reads them:
# 'name: {default: value}' and '${var.name}'. The reconciliation block needs an extension that core does not have.
SCOPED_EXAMPLE: str = """
    dataset: snowflake/analytics/orders

    variables:
      active_filter:
        default: is_deleted = false
      finance_filter:
        default: |
          is_deleted = false
          AND order_date >= DATE_TRUNC('month', CURRENT_DATE)
      freshness_hours:
        default: 24
      minimum_amount:
        default: 0

    filter: ${var.active_filter}

    soda_runner:
      checks_schedule:
        cron: "0 6 * * *"
        timezone: UTC

    check_attributes:
      domain: commerce
      environment: production
      owner: data-platform-team

    scopes:
      finance:
        name: Finance
        description: Orders in the current accounting month
        filter: ${var.finance_filter}
        schedule:
          cron: "0 7 * * 1-5"
          timezone: Europe/Brussels
        check_attributes:
          domain: finance
          environment: production
          owner: finance-team

      operations:
        name: Operations
        description: Frequent operational checks
        schedule:
          cron: "*/30 * * * *"
          timezone: UTC
        check_attributes:
          domain: operations
          environment: production
          owner: operations-team

      release-gate:
        name: Release gate
        description: Critical checks invoked programmatically
        # schedule omitted: manual or programmatic only
        check_attributes:
          domain: data-platform
          environment: production
          owner: data-platform-team

    checks:
      - row_count:
          threshold:
            must_be_greater_than: 0

      - freshness:
          scope: finance
          column: updated_at
          threshold:
            must_be_less_than: ${var.freshness_hours}
            unit: hour

      - freshness:
          scope: operations
          column: updated_at
          threshold:
            must_be_less_than: ${var.freshness_hours}
            unit: hour

      - failed_rows:
          scope: release-gate
          expression: |
            order_id IS NULL
            OR updated_at IS NULL
          threshold:
            must_be: 0

    columns:
      - name: order_id
        data_type: VARCHAR
        checks:
          - missing:
          - duplicate:
              scope: operations

      - name: amount
        data_type: NUMBER
        checks:
          - invalid:
              scope: finance
              valid_min: ${var.minimum_amount}
          - invalid:
              scope: release-gate
              valid_min: ${var.minimum_amount}

      - name: updated_at
        data_type: TIMESTAMP

    reconciliation:
      sources:
        - name: payments
          dataset: postgres/payments/settlements

      checks:
        - aggregate_diff:
            scope: finance
            source: payments
            function: sum
            source_column: settled_amount
            target_column: amount
            threshold:
              metric: percent
              must_be_less_than: 1
"""


def test_the_scoped_example_passes_soda_contract_test(monkeypatch, tmp_path):
    assert _soda_contract_test(monkeypatch, tmp_path, dedent_and_strip(SCOPED_EXAMPLE)) == (ExitCode.OK, [])

    contract_yaml, logs = _parse(dedent_and_strip(SCOPED_EXAMPLE))
    assert list(contract_yaml.scopes) == ["finance", "operations", "release-gate"]
    finance = contract_yaml.scopes["finance"]
    assert finance.filter == "is_deleted = false\nAND order_date >= DATE_TRUNC('month', CURRENT_DATE)"
    assert (finance.schedule.cron, finance.schedule.timezone) == ("0 7 * * 1-5", "Europe/Brussels")
    assert contract_yaml.scopes["release-gate"].schedule is None
    assert logs.get_errors() == []


def test_the_top_level_schedule_stays_an_ignored_key():
    # The top-level 'schedule' is not part of the scope syntax: like any unknown root key it is read by nobody, next to
    # a legacy schedule or not, and the legacy schedule logs nothing new.
    for top in [
        "schedule: inherit\n",
        "schedule: {cron: '0 6 * * *'}\nsoda_runner:\n  checks_schedule: {cron: '0 7 * * *'}\n",
        "schedule: {cron: '0 6 * * *'}\nsoda_agent:\n  checks_schedule: {cron: '0 7 * * *'}\n",
        "soda_runner:\n  checks_schedule: {cron: '0 7 * * *'}\nscopes:\n  eu: {name: EU}\n",
    ]:
        _, logs = _parse(_contract(top))
        assert logs.get_logs() == [], top


def _scope_keys_fixture() -> dict:
    return YAML(typ="safe").load(SCOPE_KEYS_FIXTURE_PATH.read_text(encoding="utf-8"))


def test_the_scope_keys_fixture_matches_the_engine_key_check():
    fixture = _scope_keys_fixture()
    assert all(isinstance(key, str) for key in fixture["valid"] + fixture["invalid"])
    assert RESERVED_SCOPE_KEYS <= set(fixture["invalid"])
    assert {"a", "eu-west", "eu_west", "a-"} <= set(fixture["valid"])
    assert {"", "9a", "-a", "Base", "eu.west", "eu:west"} <= set(fixture["invalid"])
    assert {len(key) for key in fixture["valid"]} >= {1, 64}
    assert 65 in {len(key) for key in fixture["invalid"]}

    assert [key for key in fixture["valid"] if scope_key_error(key) is not None] == []
    assert [key for key in fixture["invalid"] if scope_key_error(key) is None] == []


@pytest.mark.parametrize("key", _scope_keys_fixture()["valid"] + _scope_keys_fixture()["invalid"])
def test_a_contract_declares_exactly_the_valid_fixture_keys(key: str):
    yaml_str = _contract(f"scopes:\n  {key!r}: {{name: X}}\n".replace("'", '"'))
    _, logs = _parse(yaml_str)
    if key in _scope_keys_fixture()["valid"]:
        assert logs.get_errors() == []
    else:
        [error] = logs.get_errors()
        assert error.startswith(f"Invalid scope key '{key}': "), error


class _ExtensionChecks:
    """Parses the checks under a root 'x_extension_checks' key itself, as an extension that parses its own check
    types does, instead of through ``ContractYaml``."""

    def __init__(self, contract_impl):
        self.contract_impl = contract_impl

    def parse_checks(self, contract_impl) -> list:
        extension_checks = contract_impl.yaml.yaml_object.read_list_of_objects_opt("x_extension_checks") or []
        check_impls = []
        for check_yaml_object in extension_checks:
            [check_type_name] = check_yaml_object.keys()
            check_yaml = CheckYaml.parse_check_yaml(
                check_type_name=check_type_name,
                check_body_yaml_object=check_yaml_object.read_object(check_type_name),
                column_yaml=None,
            )
            check_impls.append(CheckImpl.parse_check(contract_impl=contract_impl, check_yaml=check_yaml))
        return check_impls

    def build_queries(self, contract_impl) -> list:
        return []


EXTENSION_CHECKS_YAML: str = """
    dataset: ds/db/schema/table
    scopes:
      eu: {name: EU}
    columns: []
    checks:
      - row_count:
      - row_count: {qualifier: core-base, scope: base}
    x_extension_checks:
      - row_count: {qualifier: eu, scope: eu}
      - row_count: {qualifier: base, scope: base}
      - row_count: {qualifier: undeclared, scope: apac}
      - row_count: {qualifier: number, scope: 5}
      - row_count: {qualifier: null-scope, scope: null}
"""


@pytest.mark.parametrize("impl_class", [ContractImpl, ScopeUnsupportedImpl], ids=["contract", "unsupported-kind"])
def test_checks_an_extension_parses_are_checked_where_their_scope_is_resolved(impl_class):
    CheckCollectionImpl.register_extension("scope_parsing_extension_checks", _ExtensionChecks)
    kind_line = KIND_LINE_WITHOUT_SCOPE_SUPPORT if impl_class is ScopeUnsupportedImpl else ""
    try:
        impl, logs = _build_contract_impl(kind_line + dedent_and_strip(EXTENSION_CHECKS_YAML), impl_class=impl_class)
    finally:
        CheckCollectionImpl.impl_extensions.pop("scope_parsing_extension_checks", None)

    assert [check_impl.check_yaml.qualifier for check_impl in impl.all_check_impls] == [
        None,
        "core-base",
        "eu",
        "base",
        "undeclared",
        "number",
        "null-scope",
    ]
    if impl_class is ScopeUnsupportedImpl:
        assert logs.get_errors() == []
        return
    # The check ContractYaml parsed is reported once, while the YAML is parsed. The checks the extension parsed are
    # reported where their scope is resolved.
    assert logs.get_errors() == [
        f"Invalid check scope 'base': {BASE_REASON}",
        f"Invalid check scope 'base': {BASE_REASON}",
        "Check references unknown scope 'apac'. Declared scopes: ['eu']",
        "Check 'scope' must name a declared scope, but was a number: 5",
        "Check 'scope' must name a declared scope, but was null",
    ]
    locations = [record.location for record in logs.gatherer.get_error_logs()]
    assert all(location is not None and location.line is not None for location in locations)


NUDGE_YAML: str = """
    dataset: ds/db/schema/table
    scopes:
      eu: {name: EU}
      us: {name: US}
    columns:
      - name: id
        checks:
          - missing:
          - missing: {scope: us}
    checks:
      - row_count:
      - row_count: {qualifier: eu, scope: eu}
      - row_count: {qualifier: us, scope: us}
"""
CONTRACT_NUDGE: str = (
    "Excluded 3 checks whose scope is not active. Running checks in a scope needs a Soda extension that runs scopes."
)


def _nudge_lines(logs: Logs) -> list[str]:
    return [line for line in logs.get_logs() if line.startswith("Excluded ")]


def test_a_contract_logs_one_nudge_for_the_checks_in_inactive_scopes():
    impl, logs = _build_contract_impl(dedent_and_strip(NUDGE_YAML))
    assert [check_impl.skip for check_impl in impl.all_check_impls] == [False, True, False, True, True]
    assert _nudge_lines(logs) == [CONTRACT_NUDGE]
    assert logs.get_errors() == []


def test_the_nudge_counts_one_check():
    yaml_str = _contract("scopes:\n  eu: {name: EU}\n", "      scope: eu\n")
    _, logs = _build_contract_impl(yaml_str)
    assert _nudge_lines(logs) == [
        "Excluded 1 check whose scope is not active. Running checks in a scope needs a Soda extension that runs scopes."
    ]


def test_no_nudge_without_a_selected_check_in_an_inactive_scope():
    # No scoped check at all.
    _, logs = _build_contract_impl(_contract("scopes:\n  eu: {name: EU}\n"))
    assert _nudge_lines(logs) == []
    # Scoped checks that no selector selects are excluded as deselected checks, without the nudge.
    _, logs = _build_contract_impl(dedent_and_strip(NUDGE_YAML), check_selectors=[CheckSelector.parse("type=missing")])
    assert _nudge_lines(logs) == [
        "Excluded 1 check whose scope is not active. Running checks in a scope needs a Soda extension that runs scopes."
    ]
    _, logs = _build_contract_impl(
        dedent_and_strip(NUDGE_YAML), check_selectors=[CheckSelector.parse("qualifier=none")]
    )
    assert _nudge_lines(logs) == []


def test_a_kind_without_scope_support_names_itself_in_the_nudge():
    impl, logs = _build_contract_impl(
        KIND_LINE_WITHOUT_SCOPE_SUPPORT + dedent_and_strip(NUDGE_YAML), impl_class=ScopeUnsupportedImpl
    )
    assert [check_impl.skip for check_impl in impl.all_check_impls] == [False, True, False, True, True]
    assert _nudge_lines(logs) == [
        f"Excluded 3 checks with a scope: kind '{SCOPE_UNSUPPORTED_KIND}' does not support scopes."
    ]
    assert logs.get_errors() == []
