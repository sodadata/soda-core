"""Pin what Soda Cloud receives for scoped checks.

A scoped check carries its scope in its path, attributes and definition. A file of a kind without scope support
declares scopes too and never applies them. Its checks upload the top-level check attributes and filter whatever
their scope, and no scope input fails the file.
"""

from __future__ import annotations

import pytest
from helpers.orders_contract import scoped_contract_yaml, uploaded_payloads, verify_contract, verify_session
from helpers.scope_test_kinds import SCOPE_UNSUPPORTED_KIND


def _scoped_checks(monkeypatch) -> list[dict]:
    _, payload = verify_contract(monkeypatch, scoped_contract_yaml())
    return [check for check in payload["checks"] if check["checkPath"].startswith("scope.")]


def test_scoped_checks_carry_the_scope_prefix(monkeypatch):
    assert [check["checkPath"] for check in _scoped_checks(monkeypatch)] == [
        "scope.eu:columns.amount.checks.invalid",
        "scope.eu:checks.row_count.2",
        "scope.us:checks.row_count",
    ]


def test_scoped_checks_carry_the_scope_check_attributes(monkeypatch):
    # The scope's check attributes under the check's own, never the top-level ones.
    assert [check["resourceAttributes"] for check in _scoped_checks(monkeypatch)] == [
        [
            {"name": "team", "value": "data-eng-eu"},
            {"name": "region", "value": "eu-west"},
            {"name": "owner", "value": "finance"},
        ],
        [{"name": "team", "value": "data-eng-eu"}, {"name": "region", "value": "eu"}],
        [],
    ]


def test_scoped_checks_carry_the_scope_filter_in_their_definition(monkeypatch):
    # The scope filter in place of the top-level one; a scope without a filter shows none.
    assert [check["definition"] for check in _scoped_checks(monkeypatch)] == [
        "filter: country IN ('BE', 'NL', 'DE')\n"
        "columns:\n"
        "- name: amount\n"
        "  checks:\n"
        "  - scope: eu\n"
        "    valid_min: 0\n"
        "    attributes:\n"
        "      owner: finance\n"
        "      region: eu-west\n",
        "filter: country IN ('BE', 'NL', 'DE')\n"
        "checks:\n"
        "- scope: eu\n"
        "  qualifier: 2\n"
        "  threshold:\n"
        "    must_be_greater_than: 1\n",
        "checks:\n- scope: us\n",
    ]


# A file of a kind without scope support, which reads scope input as written and never applies it. Its checks carry
# the top-level filter and check attributes whatever their scope, as before scopes existed, and its scope input never
# fails the file.
UNSUPPORTED_KIND_YAML = f"""
    kind: {SCOPE_UNSUPPORTED_KIND}
    dataset: fixture_ds/main/orders
    filter: status <> 'cancelled'
    check_attributes:
      team: data-eng
    columns: []
    checks:
      - row_count:
      - row_count:
          scope: eu
          qualifier: declared
      - row_count:
          scope: undeclared
          qualifier: undeclared
    scopes:
      eu:
        name: EU
        filter: country = 'BE'
        check_attributes:
          team: data-eng-eu
          region: eu
"""


def test_kind_without_scope_support_keeps_the_top_level_attributes_and_filter(monkeypatch):
    session_result, payload = verify_contract(monkeypatch, UNSUPPORTED_KIND_YAML)

    assert session_result.get_errors() == []
    assert [(check["checkPath"], check["outcome"]) for check in payload["checks"]] == [
        ("checks.row_count", "pass"),
        ("checks.row_count.declared", "excluded"),
        ("checks.row_count.undeclared", "excluded"),
    ]
    assert [check["resourceAttributes"] for check in payload["checks"]] == [[{"name": "team", "value": "data-eng"}]] * 3
    assert [check["definition"].split("checks:")[0] for check in payload["checks"]] == [
        "filter: status <> 'cancelled'\n"
    ] * 3


def _unsupported_kind_yaml_with_scope_check_attribute(attribute_line: str) -> str:
    return f"""
        kind: {SCOPE_UNSUPPORTED_KIND}
        dataset: fixture_ds/main/orders
        columns: []
        checks:
          - row_count:
          - row_count:
              scope: eu
              qualifier: scoped
        scopes:
          eu:
            name: EU
            check_attributes:
              {attribute_line}
    """


# Scope check attributes the payload cannot carry: a tagged value, a key that is not a string and a set.
UNSENDABLE_SCOPE_CHECK_ATTRIBUTES = pytest.mark.parametrize(
    "attribute_line",
    ["owner: !custom x", "1: numeric-key", "labels: !!set {a, b}"],
    ids=["tagged-value", "non-string-key", "set-value"],
)


@UNSENDABLE_SCOPE_CHECK_ATTRIBUTES
def test_kind_without_scope_support_never_fails_on_scope_check_attributes(monkeypatch, attribute_line):
    session_result, payload = verify_contract(
        monkeypatch, _unsupported_kind_yaml_with_scope_check_attribute(attribute_line)
    )

    assert session_result.get_errors() == []
    assert [check["outcome"] for check in payload["checks"]] == ["pass", "excluded"]


@UNSENDABLE_SCOPE_CHECK_ATTRIBUTES
def test_kind_without_scope_support_uploads_next_to_a_contract_despite_scope_check_attributes(
    monkeypatch, attribute_line
):
    contract_yaml: str = """
        dataset: fixture_ds/main/orders
        columns: []
        checks:
          - row_count:
    """
    session_result, soda_cloud = verify_session(
        monkeypatch, [_unsupported_kind_yaml_with_scope_check_attribute(attribute_line), contract_yaml]
    )

    assert session_result.get_errors() == []
    assert [[check["outcome"] for check in payload["checks"]] for payload in uploaded_payloads(soda_cloud)] == [
        ["pass", "excluded"],
        ["pass"],
    ]
