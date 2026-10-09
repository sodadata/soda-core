"""Validation of ``scopes``, a check's ``scope`` and scope schedules.

A contract checks its scope input while the YAML is parsed, so ``soda contract test`` and publication report
the same errors. Each error names the key it is about, carries its location and ends the file with errors, which
``soda contract test`` reports with exit code 3. A kind without scope support rejects any scope input.

Every test here drops the soda-scopes extension, so the file pins core alone even where soda-scopes is installed.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pytest
from helpers.scope_test_kinds import SCOPE_UNSUPPORTED_KIND, ScopeUnsupportedImpl
from helpers.scopes_extension_removal import without_scopes_extension  # noqa: F401
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
from soda_core.contracts.impl.scope import RESERVED_SCOPE_KEYS, scope_key_error, unsupported_scopes_error

pytestmark = pytest.mark.usefixtures("without_scopes_extension")

SCOPE_KEYS_FIXTURE_PATH: Path = Path(__file__).parent.parent / "fixtures" / "scope_keys.yml"

KIND_LINE_WITHOUT_SCOPE_SUPPORT: str = f"kind: {SCOPE_UNSUPPORTED_KIND}\n"
UNSUPPORTED: str = unsupported_scopes_error(SCOPE_UNSUPPORTED_KIND)

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
    "key-with-a-variable": (
        'scopes:\n  "${var.REGION}": {name: A}\n',
        "",
        ["Invalid scope key '${var.REGION}': a scope key is fixed, so it cannot use a variable"],
    ),
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
        ["YAML key 'scopes' must not be null"],
    ),
    "scopes-null": ("scopes: null\n", "", ["YAML key 'scopes' must not be null"]),
    "scopes-list": (
        "scopes: [eu]\n",
        "",
        ["YAML key 'scopes' expected one of ['dict'], but was YAML list"],
    ),
    "scopes-string": (
        "scopes: eu\n",
        "",
        ["YAML key 'scopes' expected one of ['dict'], but was str"],
    ),
    # A check's scope
    "unknown-scope": (
        "scopes:\n  eu: {name: EU}\n  us: {name: US}\n",
        "      scope: apac\n",
        ["Check references unknown scope 'apac'. Declared scopes: ['eu', 'us']"],
    ),
    "unknown-scope-next-to-an-invalid-key": (
        "scopes:\n  Bad: {name: Bad}\n  eu: {name: EU}\n",
        "      scope: zzz\n",
        [
            f"Invalid scope key 'Bad': {KEY_PATTERN_REASON}",
            "Check references unknown scope 'zzz'. Declared scopes: ['eu']",
        ],
    ),
    "unknown-scope-next-to-invalid-keys-only": (
        "scopes:\n  Bad: {name: Bad}\n",
        "      scope: zzz\n",
        [
            f"Invalid scope key 'Bad': {KEY_PATTERN_REASON}",
            "Check references unknown scope 'zzz'. No valid scopes are declared",
        ],
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
    # A scope key is fixed, so a check's scope never takes a variable, whatever its namespace.
    "scope-reference-in-an-unknown-namespace": (
        "scopes:\n  eu: {name: EU}\n",
        "      scope: ${nope.SCOPE}\n",
        ["Check 'scope' cannot use a variable, but was '${nope.SCOPE}'. Name a declared scope key"],
    ),
    "scope-variable-inside-a-string": (
        "scopes:\n  eu: {name: EU}\n",
        "      scope: eu-${nope.SCOPE}\n",
        ["Check 'scope' cannot use a variable, but was 'eu-${nope.SCOPE}'. Name a declared scope key"],
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
    "name-number": ("scopes:\n  eu: {name: 5}\n", "", ["YAML key 'name' expected one of ['str'], but was int"]),
    "name-null": ("scopes:\n  eu: {name: null}\n", "", ["YAML key 'name' must not be null"]),
    "description-list": (
        "scopes:\n  eu: {name: EU, description: [a]}\n",
        "",
        ["YAML key 'description' expected one of ['str'], but was YAML list"],
    ),
    "filter-number": (
        "scopes:\n  eu: {name: EU, filter: 5}\n",
        "",
        ["YAML key 'filter' expected one of ['str'], but was int"],
    ),
    "filter-boolean": (
        "scopes:\n  eu: {name: EU, filter: true}\n",
        "",
        ["YAML key 'filter' expected one of ['str'], but was bool"],
    ),
    "filter-null": (
        "scopes:\n  eu: {name: EU, filter: null}\n",
        "",
        ["YAML key 'filter' must not be null"],
    ),
    "check-attributes-list": (
        "scopes:\n  eu: {name: EU, check_attributes: [a]}\n",
        "",
        ["YAML key 'check_attributes' expected one of ['dict'], but was YAML list"],
    ),
    # A scope's schedule
    "schedule-null": (
        "scopes:\n  eu: {name: EU, schedule: null}\n",
        "",
        ["YAML key 'schedule' must not be null"],
    ),
    "schedule-inherit": (
        "scopes:\n  eu: {name: EU, schedule: inherit}\n",
        "",
        ["YAML key 'schedule' expected one of ['dict'], but was str"],
    ),
    "schedule-weekly": (
        'scopes:\n  eu: {name: EU, schedule: "weekly"}\n',
        "",
        ["YAML key 'schedule' expected one of ['dict'], but was DoubleQuotedScalarString"],
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
        ["YAML key 'cron' expected one of ['str'], but was int"],
    ),
    "schedule-timezone-list": (
        "scopes:\n  eu: {name: EU, schedule: {cron: '0 6 * * *', timezone: [UTC]}}\n",
        "",
        ["YAML key 'timezone' expected one of ['str'], but was YAML list"],
    ),
    "schedule-variables-list": (
        "scopes:\n  eu: {name: EU, schedule: {cron: '0 6 * * *', variables: [A]}}\n",
        "",
        ["YAML key 'variables' expected one of ['dict'], but was YAML list"],
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


def _invalid_contract(case: str) -> str:
    scopes_block, check_body, _ = INVALID_SCOPE_INPUT[case]
    return _contract(scopes_block, check_body)


@pytest.mark.parametrize("case", list(INVALID_SCOPE_INPUT))
def test_invalid_scope_input_is_an_error_where_the_yaml_is_parsed(case: str):
    _, logs = _parse(_invalid_contract(case))
    assert logs.get_errors() == INVALID_SCOPE_INPUT[case][2]
    for record in logs.gatherer.get_error_logs():
        location = getattr(record, ExtraKeys.LOCATION, None)
        assert location is not None and location.line is not None, record.getMessage()


def test_invalid_scope_input_fails_soda_contract_test(monkeypatch, tmp_path):
    # Every case above is an error where the YAML is parsed, and 'soda contract test' reports that parse.
    case = next(iter(INVALID_SCOPE_INPUT))
    exit_code, errors = _soda_contract_test(monkeypatch, tmp_path, _invalid_contract(case))
    assert exit_code == ExitCode.LOG_ERRORS
    assert errors == INVALID_SCOPE_INPUT[case][2]


@pytest.mark.parametrize(
    "scopes_block, check_body, errors",
    [
        ("scopes:\n  eu: {name: EU}\n", "", [UNSUPPORTED]),
        ("scopes: {}\n", "", [UNSUPPORTED]),
        ("", "      scope: eu\n", [UNSUPPORTED]),
        ("", "      scope: null\n", [UNSUPPORTED]),
        ("scopes:\n  eu: {name: EU}\n", "      scope: eu\n", [UNSUPPORTED] * 2),
    ],
    ids=["scopes", "empty-scopes", "check-scope", "null-check-scope", "both"],
)
def test_a_kind_without_scope_support_fails_on_scope_input(monkeypatch, tmp_path, scopes_block, check_body, errors):
    yaml_str = _contract(scopes_block, check_body, KIND_LINE_WITHOUT_SCOPE_SUPPORT)
    _, logs = _parse(yaml_str)
    assert logs.get_errors() == errors
    assert all(record.location is not None for record in logs.gatherer.get_error_logs())
    assert _soda_contract_test(monkeypatch, tmp_path, yaml_str) == (ExitCode.LOG_ERRORS, errors)


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


def test_a_check_scope_cannot_use_an_environment_variable(monkeypatch, tmp_path):
    value = "eu"
    # The check fails the file whatever the variable holds, so the same contract never moves its checks between
    # scopes from one run to the next.
    monkeypatch.setenv(SCOPE_ENVIRONMENT_VARIABLE, value)
    errors = [
        f"Check 'scope' cannot use a variable, but was '${{env.{SCOPE_ENVIRONMENT_VARIABLE}}}'. "
        "Name a declared scope key"
    ]
    _, logs = _parse(_environment_scope_contract())
    assert logs.get_errors() == errors
    [record] = logs.gatherer.get_error_logs()
    assert getattr(record, ExtraKeys.LOCATION).line is not None
    assert _soda_contract_test(monkeypatch, tmp_path, _environment_scope_contract()) == (ExitCode.LOG_ERRORS, errors)


def test_a_duplicate_scope_key_fails_soda_contract_test(monkeypatch, tmp_path):
    contract_file = tmp_path / "contract.yml"
    contract_file.write_text(_contract("scopes:\n  eu: {name: A}\n  us: {name: U}\n  eu: {name: B}\n"))

    # The YAML parser raises, and the CLI turns what the handler raises into exit code 3.
    with pytest.raises(YamlParserException):
        handle_test_contract(contract_file_path=str(contract_file), variables=None)

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
        # 'scopes', the check ContractYaml parsed and the five the extension parsed.
        assert logs.get_errors() == [UNSUPPORTED] * 7
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


def test_no_nudge_without_a_scoped_check():
    impl, logs = _build_contract_impl(_contract("scopes:\n  eu: {name: EU}\n"))
    assert impl.count_checks_excluded_for_their_scope() == 0
    assert _nudge_lines(logs) == []


def test_the_nudge_counts_scoped_checks_whatever_the_selectors_pick():
    for selector in ["type=missing", "qualifier=none"]:
        impl, logs = _build_contract_impl(dedent_and_strip(NUDGE_YAML), check_selectors=[CheckSelector.parse(selector)])
        assert impl.count_checks_excluded_for_their_scope() == 3, selector
        assert _nudge_lines(logs) == [CONTRACT_NUDGE], selector


def test_no_nudge_for_a_check_whose_scope_is_not_declared():
    # The check already logs an error, and no extension would run a scope nobody declared.
    impl, logs = _build_contract_impl(_contract("scopes:\n  eu: {name: EU}\n", "      scope: nope\n"))
    assert logs.get_errors() == ["Check references unknown scope 'nope'. Declared scopes: ['eu']"]
    assert impl.count_checks_excluded_for_their_scope() == 0
    assert _nudge_lines(logs) == []
    # Next to a check in a declared scope, only that one counts.
    yaml_str = _contract("scopes:\n  eu: {name: EU}\n", "      scope: nope\n") + "  - row_count:\n      scope: eu\n"
    impl, _ = _build_contract_impl(yaml_str)
    assert impl.count_checks_excluded_for_their_scope() == 1
