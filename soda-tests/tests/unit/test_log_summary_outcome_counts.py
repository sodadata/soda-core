"""``CheckCollectionImpl.build_log_summary`` counts each check outcome in one
row of the summary table, and ``CheckResult.is_excluded`` is a property like
``is_passed``, ``is_warned``, ``is_failed`` and ``is_not_evaluated``.

The summary's last branch reads ``elif check_result.is_excluded:``. Every
``CheckOutcome`` other than EXCLUDED is claimed by an earlier branch, so only
an outcome outside the enum tells a property from a method: a bound method is
always truthy and would count it as excluded.
"""

from __future__ import annotations

from enum import Enum

import pytest
from soda_core.check_collections.base import CheckCollectionImpl
from soda_core.contracts.contract_verification import Check, CheckOutcome, CheckResult


class _OutcomeOutsideTheEnum(Enum):
    SKIPPED = "SKIPPED"


class _NoErrorsLogs:
    def get_errors(self) -> list:
        return []


class _SummaryCollection(CheckCollectionImpl):
    """No override; empty ``kind`` keeps it out of the registry."""


def _check_result(name: str, outcome) -> CheckResult:
    check = Check(
        column_name=None,
        type="row_count",
        qualifier=None,
        name=name,
        relative_path=f"checks.{name}",
        check_path=f"checks.{name}",
        identity=f"identity-{name}",
        definition="d",
        contract_file_line=1,
        contract_file_column=1,
        threshold=None,
        attributes=None,
        location=None,
    )
    return CheckResult(check=check, outcome=outcome)


def _summary_counts(check_results: list[CheckResult]) -> dict[str, int]:
    impl = object.__new__(_SummaryCollection)
    impl.logs = _NoErrorsLogs()
    lines = impl.build_log_summary("ds/schema/table", check_results)

    counts: dict[str, int] = {}
    for line in lines[lines.index("# Summary:") + 1 :]:
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) >= 2 and cells[1].isdigit():
            counts[cells[0]] = int(cells[1])
    return counts


_OUTCOME_ROWS = ["Passed", "Failed", "Warned", "Not Evaluated", "Excluded"]


def test_summary_counts_each_outcome_once():
    counts = _summary_counts([_check_result(f"check {outcome.name}", outcome) for outcome in CheckOutcome])

    assert counts == {
        "Checks": 5,
        "Passed": 1,
        "Failed": 1,
        "Warned": 1,
        "Not Evaluated": 1,
        "Excluded": 1,
        "Runtime Errors": 0,
    }


def test_outcome_outside_the_enum_is_not_counted_as_excluded():
    check_results = [_check_result(f"check {outcome.name}", outcome) for outcome in CheckOutcome]
    check_results.append(_check_result("check SKIPPED", _OutcomeOutsideTheEnum.SKIPPED))

    counts = _summary_counts(check_results)

    assert {row: counts[row] for row in _OUTCOME_ROWS} == {row: 1 for row in _OUTCOME_ROWS}


@pytest.mark.parametrize("name", ["is_passed", "is_warned", "is_failed", "is_not_evaluated", "is_excluded"])
def test_outcome_predicates_are_properties(name: str):
    assert isinstance(CheckResult.__dict__[name], property)


def test_is_excluded_reads_the_outcome():
    assert _check_result("excluded", CheckOutcome.EXCLUDED).is_excluded is True
    assert _check_result("passed", CheckOutcome.PASSED).is_excluded is False
