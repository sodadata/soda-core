"""``count_check_outcomes`` counts the check results of a run per outcome for the summary table, and
``CheckResult.is_excluded`` reads the outcome like the other outcome predicates."""

from __future__ import annotations

from enum import Enum

import pytest
from soda_core.check_collections.base import count_check_outcomes
from soda_core.contracts.contract_verification import Check, CheckOutcome, CheckResult


class _OutcomeOutsideTheEnum(Enum):
    SKIPPED = "SKIPPED"


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


def test_counts_every_outcome():
    # Fails when a new CheckOutcome is added and the counting is not updated for it.
    check_results = [_check_result(f"check {outcome.name}", outcome) for outcome in CheckOutcome]

    assert count_check_outcomes(check_results) == {outcome: 1 for outcome in CheckOutcome}


def test_counts_each_result_under_its_own_outcome():
    outcomes = [CheckOutcome.PASSED, CheckOutcome.PASSED, CheckOutcome.FAILED, CheckOutcome.EXCLUDED]

    counts = count_check_outcomes([_check_result(f"check {index}", outcome) for index, outcome in enumerate(outcomes)])

    assert counts == {
        CheckOutcome.PASSED: 2,
        CheckOutcome.FAILED: 1,
        CheckOutcome.WARN: 0,
        CheckOutcome.NOT_EVALUATED: 0,
        CheckOutcome.EXCLUDED: 1,
    }


def test_counts_nothing_without_results():
    assert count_check_outcomes([]) == {outcome: 0 for outcome in CheckOutcome}


@pytest.mark.parametrize("outcome", [_OutcomeOutsideTheEnum.SKIPPED, "PASSED", None], ids=["other-enum", "str", "none"])
def test_an_outcome_it_cannot_count_raises(outcome):
    check_results = [_check_result("passed", CheckOutcome.PASSED), _check_result("unknown", outcome)]

    with pytest.raises(ValueError, match="Cannot count check outcome .* of 'unknown'"):
        count_check_outcomes(check_results)


def test_is_excluded_reads_the_outcome():
    assert _check_result("excluded", CheckOutcome.EXCLUDED).is_excluded is True
    assert _check_result("passed", CheckOutcome.PASSED).is_excluded is False
