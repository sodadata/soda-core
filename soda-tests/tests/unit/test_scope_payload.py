"""Pin what Soda Cloud receives for scoped checks.

A scoped check carries its scope in its path, attributes and definition.
"""

from __future__ import annotations

from helpers.orders_contract import scoped_contract_yaml, verify_contract


def _scoped_checks(monkeypatch) -> list[dict]:
    _, payload = verify_contract(monkeypatch, scoped_contract_yaml())
    return [check for check in payload["checks"] if check["checkPath"].startswith("scope.")]


def test_scoped_checks_carry_their_scope_in_path_attributes_and_definition(monkeypatch):
    scoped_checks = _scoped_checks(monkeypatch)

    assert [check["checkPath"] for check in scoped_checks] == [
        "scope.eu:columns.amount.checks.invalid",
        "scope.eu:checks.row_count.2",
        "scope.us:checks.row_count",
    ]
    # The scope's check attributes under the check's own, never the top-level ones.
    assert [check["resourceAttributes"] for check in scoped_checks] == [
        [
            {"name": "team", "value": "data-eng-eu"},
            {"name": "region", "value": "eu-west"},
            {"name": "owner", "value": "finance"},
        ],
        [{"name": "team", "value": "data-eng-eu"}, {"name": "region", "value": "eu"}],
        [],
    ]
    # The scope filter in place of the top-level one; a scope without a filter shows none.
    assert [check["definition"] for check in scoped_checks] == [
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
