"""Lock the separation between ``Check.relative_path`` (selector field) and
``Check.check_path`` (full wire path used by --check-paths / the FE).

``relative_path=`` always matches the stripped yaml-internal path on both
contract and data-standard subtypes.  ``check_path=`` matches the full wire
path (which for data standards carries the option-3 prefix
``"{wire_source}.{collection_id}:{relative_path}"``).  A ``relative_path=``
selector with a prefixed value does NOT match — that would silently fan-out
to a check the user did not target.

``from_check_paths`` is the entry-point used by the CLI and the FE; it emits
``check_path`` selectors so the full prefixed path is matched exactly.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from soda_core.contracts.impl.check_selector import CheckSelector


def _make_check_impl(*, relative_path: str, check_path: str | None = None):
    """Build a minimal CheckImpl-shaped stub.

    ``relative_path`` — stripped yaml-internal path (selector field).
    ``check_path``    — full wire path; defaults to ``relative_path``
                        (contract behaviour, where both are identical).
    """
    check_impl = MagicMock()
    check_impl.relative_path = relative_path
    check_impl.check_path = check_path if check_path is not None else relative_path
    check_impl.type = "missing"
    check_impl.name = "No missing values"
    check_impl.column_impl = None
    check_impl.check_yaml = MagicMock()
    check_impl.check_yaml.qualifier = None
    check_impl.attributes = {}
    return check_impl


def test_relative_path_selector_matches_stripped_path_for_contract():
    """``relative_path=`` matches the stripped path on contract subtypes."""
    selector = CheckSelector.parse("relative_path=columns.email.checks.missing")
    check = _make_check_impl(relative_path="columns.email.checks.missing")
    assert selector.matches(check)


def test_relative_path_selector_matches_stripped_path_for_data_standard():
    """``relative_path=`` matches the stripped path on data-standard subtypes
    even though their ``check_path`` carries the option-3 prefix.
    """
    selector = CheckSelector.parse("relative_path=columns.email.checks.missing")
    check = _make_check_impl(
        relative_path="columns.email.checks.missing",
        check_path="data-standard.my_pii_standard:columns.email.checks.missing",
    )
    assert selector.matches(check)


def test_relative_path_selector_does_not_match_prefixed_form():
    """A ``relative_path=`` selector with a collection-prefixed value must NOT
    match — the prefix is wire-only and never appears in ``relative_path``.
    """
    selector = CheckSelector.parse("relative_path=data-standard.my_pii_standard:columns.email.checks.missing")
    check = _make_check_impl(
        relative_path="columns.email.checks.missing",
        check_path="data-standard.my_pii_standard:columns.email.checks.missing",
    )
    assert not selector.matches(check)


def test_check_path_selector_matches_full_path_for_data_standard():
    """``check_path=`` matches a data-standard check by its full option-3 wire path."""
    selector = CheckSelector.parse("check_path=data-standard.my_pii_standard:columns.email.checks.missing")
    check = _make_check_impl(
        relative_path="columns.email.checks.missing",
        check_path="data-standard.my_pii_standard:columns.email.checks.missing",
    )
    assert selector.matches(check)


def test_check_path_selector_matches_bare_path_for_contract():
    """For contracts ``check_path == relative_path``, so a bare path still
    matches via the ``check_path`` selector.
    """
    selector = CheckSelector.parse("check_path=columns.email.checks.missing")
    check = _make_check_impl(relative_path="columns.email.checks.missing")
    assert selector.matches(check)


def test_from_check_paths_targets_check_path_field():
    """``from_check_paths`` must emit ``check_path`` selectors — not ``path``
    or ``relative_path`` — so the full wire path is matched exactly.
    """
    selectors = CheckSelector.from_check_paths(["my_std.columns.email.checks.missing"])
    assert len(selectors) == 1
    assert selectors[0].field == "check_path"
    assert selectors[0].value == "my_std.columns.email.checks.missing"
