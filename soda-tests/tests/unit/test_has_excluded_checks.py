"""Unit tests for ``has_excluded_checks`` on the per-file result and on both session results.

``CheckCollectionSessionResult.has_excluded_checks`` and ``ContractVerificationSessionResult.has_excluded_checks``
OR ``has_excluded_checks`` over their per-file results, so they only work when ``CheckCollectionResult`` defines it.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from soda_core.check_collections.base import CheckCollectionResult, CheckCollectionSessionResult
from soda_core.common.logs import Location
from soda_core.contracts.contract_verification import (
    Check,
    CheckCollectionStatus,
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationResult,
    ContractVerificationSessionResult,
    YamlFileContentInfo,
)


def _make_check_result(outcome: CheckOutcome) -> CheckResult:
    return CheckResult(
        check=Check(
            column_name=None,
            type="row_count",
            qualifier=None,
            name="row count",
            relative_path="checks.row_count",
            check_path="checks.row_count",
            identity="abc",
            definition="row_count: ...",
            contract_file_line=1,
            contract_file_column=1,
            threshold=None,
            attributes={},
            location=Location(file_path="fake.yml", line=1, column=1),
        ),
        outcome=outcome,
    )


def _make_result(
    outcomes: list[CheckOutcome], result_class: type[CheckCollectionResult] = ContractVerificationResult
) -> CheckCollectionResult:
    now = datetime.now(tz=timezone.utc)
    return result_class(
        check_collection=Contract(
            data_source_name="test_ds",
            dataset_prefix=["s"],
            dataset_name="t",
            soda_qualified_dataset_name="test_ds/s/t",
            source=YamlFileContentInfo(source_content_str=None, local_file_path=None),
        ),
        data_source=None,
        data_timestamp=now,
        started_timestamp=now,
        ended_timestamp=now,
        status=CheckCollectionStatus.PASSED,
        measurements=[],
        check_results=[_make_check_result(outcome) for outcome in outcomes],
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
    )


@pytest.mark.parametrize(
    "outcomes",
    [
        [],
        [CheckOutcome.PASSED, CheckOutcome.FAILED, CheckOutcome.WARN, CheckOutcome.NOT_EVALUATED],
    ],
    ids=["no_checks", "no_excluded_checks"],
)
def test_check_collection_result_has_excluded_checks_is_false_without_excluded_results(outcomes):
    result = _make_result(outcomes, result_class=CheckCollectionResult)

    assert result.number_of_checks_excluded == 0
    assert result.has_excluded_checks is False


def test_check_collection_result_has_excluded_checks_is_true_with_one_excluded_result():
    result = _make_result([CheckOutcome.PASSED, CheckOutcome.EXCLUDED], result_class=CheckCollectionResult)

    assert result.number_of_checks_excluded == 1
    assert result.has_excluded_checks is True


SESSION_OUTCOMES = pytest.mark.parametrize(
    "outcomes_per_result, expected",
    [
        ([], False),
        ([[CheckOutcome.PASSED], [CheckOutcome.FAILED]], False),
        ([[CheckOutcome.PASSED], [CheckOutcome.EXCLUDED]], True),
        ([[CheckOutcome.EXCLUDED], [CheckOutcome.PASSED]], True),
    ],
    ids=["no_results", "none_excluded", "last_excluded", "first_excluded"],
)


@SESSION_OUTCOMES
def test_check_collection_session_result_has_excluded_checks_ors_across_results(outcomes_per_result, expected):
    session_result = CheckCollectionSessionResult(
        results=[_make_result(outcomes, result_class=CheckCollectionResult) for outcomes in outcomes_per_result]
    )

    assert session_result.has_excluded_checks is expected


@SESSION_OUTCOMES
def test_contract_verification_session_result_has_excluded_checks_ors_across_results(outcomes_per_result, expected):
    session_result = ContractVerificationSessionResult(
        contract_verification_results=[_make_result(outcomes) for outcomes in outcomes_per_result]
    )

    assert session_result.number_of_checks_excluded == (1 if expected else 0)
    assert session_result.has_excluded_checks is expected
