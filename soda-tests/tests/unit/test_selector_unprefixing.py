"""Lock the separation between ``Check.relative_path`` (selector) and
``Check.check_path`` (wire).

Selector matching uses the stripped, yaml-internal ``Check.relative_path``.
The wire path with the collection prefix is wire-only and never affects
``--check-selector`` resolution. This keeps user-facing CLI behavior
identical between contracts and data-standard subtypes — a user writing
``--check-selector path=columns.email.checks.missing`` matches the check
on both subtypes without having to know the collection name.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from soda_core.contracts.impl.check_selector import CheckSelector


def _make_check_impl(*, path: str, wire_source: str, collection_id):
    """Build a minimal CheckImpl-shaped stub the selector matchers read.

    The selector code reads ``check_impl.relative_path``, ``check_impl.type``,
    ``check_impl.name``, ``check_impl.column_impl``,
    ``check_impl.check_yaml.qualifier``, and ``check_impl.attributes`` —
    we mirror exactly that surface.
    """
    check_impl = MagicMock()
    check_impl.relative_path = path
    check_impl.type = "missing"
    check_impl.name = "No missing values"
    check_impl.column_impl = None
    check_impl.check_yaml = MagicMock()
    check_impl.check_yaml.qualifier = None
    check_impl.attributes = {}
    # The back-ref the production ``check_path`` property reads — exposed so
    # this stub can also answer ``check_path`` if a caller needs it.
    check_impl.contract_impl = MagicMock()
    check_impl.contract_impl.wire_source = wire_source
    check_impl.contract_impl.collection_id = collection_id
    return check_impl


def test_selector_matches_stripped_path_for_contract():
    """Baseline (today's behavior): contract subtype, selector matches
    against the stripped path. Locks that the rename of the wire-bound
    field to ``full_path`` did not accidentally redirect the selector.
    """
    selector = CheckSelector.parse("path=columns.email.checks.missing")
    check = _make_check_impl(
        path="columns.email.checks.missing",
        wire_source="soda-contract",
        collection_id=None,
    )
    assert selector.matches(check)


def test_selector_matches_stripped_path_for_data_standard():
    """Data-standard subtype: selector still matches against ``Check.path``
    (the stripped yaml-internal path). The collection_id prefix exists on
    ``full_path`` only — never on the selector side.
    """
    selector = CheckSelector.parse("path=columns.email.checks.missing")
    check = _make_check_impl(
        path="columns.email.checks.missing",
        wire_source="data-standard",
        collection_id="my_pii_standard",
    )
    assert selector.matches(check)


def test_selector_does_not_match_full_path_with_prefix():
    """The wire-prefixed form ``"{collection_id}.{path}"`` must NOT match
    the selector — the prefix is wire-only. A user writing
    ``--check-selector path=my_pii_standard.columns.email.checks.missing``
    against a data-standard impl is asking for a path that does not exist
    in the stripped form, and gets no match.
    """
    selector = CheckSelector.parse("path=my_pii_standard.columns.email.checks.missing")
    check = _make_check_impl(
        path="columns.email.checks.missing",
        wire_source="data-standard",
        collection_id="my_pii_standard",
    )
    assert not selector.matches(check)


def test_selector_wildcard_matches_stripped_path_for_data_standard():
    """Wildcard matching still works against the stripped path on
    non-contract subtypes.
    """
    selector = CheckSelector.parse("path=columns.email.*")
    check = _make_check_impl(
        path="columns.email.checks.missing",
        wire_source="data-standard",
        collection_id="my_pii_standard",
    )
    assert selector.matches(check)


def test_check_selector_supported_fields_does_not_include_check_path():
    """``check_path`` is wire-only — it must not appear in the public
    selector surface. If a future change exposes it, the selector behavior
    pins above start matching wire-prefixed forms, breaking the contract
    that selectors are subtype-agnostic.
    """
    assert "check_path" not in CheckSelector.SUPPORTED_FIELDS
    assert "full_path" not in CheckSelector.SUPPORTED_FIELDS
    assert "path" in CheckSelector.SUPPORTED_FIELDS
