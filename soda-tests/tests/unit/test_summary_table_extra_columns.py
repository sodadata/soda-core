"""``CheckCollectionImpl.log_table_extra_columns`` and
``CheckCollectionImpl.log_table_header_overrides`` — the opt-in seams of the
summary results table (``build_summary_table``).

Default ``{}`` for both leaves the table unchanged; an overriding subtype (the metric-monitoring BACKFILL
collection, whose rows are otherwise visually identical — one per backfilled
window) gets new columns inserted right after "Check" (participating in the
row sort at that position) and may replace existing cells (e.g.
"Diagnostics") in place. Header overrides rename rendered headers only (e.g.
metric monitoring renders "Check" as "Monitor"); row-dict keys stay internal.
"""

from __future__ import annotations

from soda_core.check_collections.base import CheckCollectionImpl
from soda_core.contracts.contract_verification import Check, CheckOutcome, CheckResult
from tabulate import tabulate


def _check_result(name: str, column_name=None, outcome=CheckOutcome.PASSED, identity=None) -> CheckResult:
    check = Check(
        column_name=column_name,
        type="row_count",
        qualifier=None,
        name=name,
        path=f"checks.{name}",
        identity=identity or f"identity-{name}",
        definition="d",
        contract_file_line=1,
        contract_file_column=1,
        threshold=None,
        attributes=None,
        location=None,
    )
    return CheckResult(check=check, outcome=outcome)


class _PlainCollection(CheckCollectionImpl):
    """No override; empty ``kind`` keeps it out of the registry."""


class _WindowedCollection(CheckCollectionImpl):
    """Backfill-style override: per-row Window column + Diagnostics override."""

    def log_table_extra_columns(self, check_result: CheckResult) -> dict:
        return dict(self.extra_columns_by_identity.get(check_result.check.identity, {}))


class _MonitoringStyleCollection(_WindowedCollection):
    """Monitoring-style override: the "Check" column renders as "Monitor"."""

    def log_table_header_overrides(self) -> dict[str, str]:
        return {"Check": "Monitor"}


def _instance(cls, **attributes):
    impl = object.__new__(cls)
    for key, value in attributes.items():
        setattr(impl, key, value)
    return impl


def test_default_hook_is_empty_and_keeps_the_table_unchanged():
    impl = _instance(_PlainCollection)
    check_results = [
        _check_result("b check"),
        _check_result("a check", column_name="col_1"),
        _check_result("a check", column_name="col_1", outcome=CheckOutcome.FAILED, identity="identity-2"),
    ]

    assert CheckCollectionImpl.log_table_extra_columns(impl, check_results[0]) == {}
    assert CheckCollectionImpl.log_table_header_overrides(impl) == {}

    # the default rendering formula
    expected_rows = [check_result.log_table_row() for check_result in check_results]
    expected_rows.sort(key=lambda row: (row["Column"], row["Check"], row["Outcome"]))
    previous_column_name = None
    for row in expected_rows:
        if previous_column_name == row["Column"]:
            row["Column"] = ""
        else:
            previous_column_name = row["Column"]
    expected = tabulate(expected_rows, headers="keys", tablefmt="grid")

    assert impl.build_summary_table(check_results) == expected
    assert "Window" not in expected


def test_extra_columns_are_inserted_after_check_and_sort_the_rows():
    impl = _instance(
        _WindowedCollection,
        extra_columns_by_identity={
            "identity-late": {"Window": "2026-07-09"},
            "identity-early": {"Window": "2026-07-07"},
            "identity-mid": {"Window": "2026-07-08"},
        },
    )
    # same check name; insertion order deliberately scrambled, and the
    # late row PASSED / others NOT_EVALUATED so an outcome-first sort
    # would break the chronological order.
    check_results = [
        _check_result("rows", outcome=CheckOutcome.PASSED, identity="identity-late"),
        _check_result("rows", outcome=CheckOutcome.NOT_EVALUATED, identity="identity-early"),
        _check_result("rows", outcome=CheckOutcome.NOT_EVALUATED, identity="identity-mid"),
    ]

    table = impl.build_summary_table(check_results)
    lines = table.splitlines()
    header = next(line for line in lines if "Column" in line and "Outcome" in line)
    columns = [column.strip() for column in header.strip("|").split("|")]
    assert columns.index("Window") == columns.index("Check") + 1

    positions = [table.index(window) for window in ("2026-07-07", "2026-07-08", "2026-07-09")]
    assert positions == sorted(positions), "rows must sort by the extra column, not by outcome"


def test_extra_columns_replace_existing_cells_in_place():
    impl = _instance(
        _WindowedCollection,
        extra_columns_by_identity={
            "identity-1": {"Window": "2026-07-09", "Diagnostics": "value: 4"},
        },
    )
    table = impl.build_summary_table([_check_result("rows", identity="identity-1")])

    lines = table.splitlines()
    header = next(line for line in lines if "Column" in line and "Outcome" in line)
    columns = [column.strip() for column in header.strip("|").split("|")]
    assert columns.count("Diagnostics") == 1, "an existing key must not become a second column"
    assert columns[-1] == "Diagnostics"
    assert "value: 4" in table


def test_header_overrides_rename_the_rendered_header_only():
    impl = _instance(
        _MonitoringStyleCollection,
        extra_columns_by_identity={
            "identity-late": {"Window": "2026-07-09"},
            "identity-early": {"Window": "2026-07-07", "Diagnostics": "value: 4"},
        },
    )
    check_results = [
        _check_result("rows", outcome=CheckOutcome.PASSED, identity="identity-late"),
        _check_result("rows", outcome=CheckOutcome.NOT_EVALUATED, identity="identity-early"),
    ]

    table = impl.build_summary_table(check_results)
    lines = table.splitlines()
    header = next(line for line in lines if "Column" in line and "Outcome" in line)
    columns = [column.strip() for column in header.strip("|").split("|")]

    # Rename only: "Monitor" replaces "Check" at the same position ...
    assert "Monitor" in columns
    assert "Check" not in columns
    # ... and extra columns keyed off the INTERNAL "Check" key still insert
    # right after it, and existing-cell overrides still land in place.
    assert columns.index("Window") == columns.index("Monitor") + 1
    assert columns.count("Diagnostics") == 1
    assert "value: 4" in table
    # rows still sort chronologically on the extra column
    positions = [table.index(window) for window in ("2026-07-07", "2026-07-09")]
    assert positions == sorted(positions)
