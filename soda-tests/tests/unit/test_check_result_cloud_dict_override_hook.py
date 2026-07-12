"""Per-CheckResult cloud-dict override hook.

``CheckResult.build_soda_cloud_check_dict(contract, wire_source)`` is the
checks-side counterpart of ``CheckCollectionResult.measurement_dicts``:

* the BASE implementation returns ``None`` → ``_build_check_results_cloud_json_dicts``
  falls back to the generic ``_build_check_result_cloud_dict``;
* a subclass returning a dict lands as-is in the built ``checks`` list —
  the seam non-contract subtypes (metric monitoring's ``anomalyDetection``
  checks) use to emit wire shapes the generic builder cannot express.
"""

from __future__ import annotations

from typing import Optional

from soda_core.common.logs import Location
from soda_core.common.soda_cloud import (
    _build_check_result_cloud_dict,
    _build_check_results_cloud_json_dicts,
)
from soda_core.contracts.contract_verification import (
    Check,
    CheckCollectionStatus,
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationResult,
    DataSource,
    YamlFileContentInfo,
)


def _make_check() -> Check:
    return Check(
        column_name="id",
        type="row_count",
        qualifier=None,
        name="row count",
        path="checks.row_count",
        full_path="checks.row_count",
        identity="abc",
        definition="row_count: ...",
        contract_file_line=1,
        contract_file_column=1,
        threshold=None,
        attributes={},
        location=Location(file_path="fake.yml", line=1, column=1),
    )


def _make_contract() -> Contract:
    return Contract(
        data_source_name="test_ds",
        dataset_prefix=["some", "schema"],
        dataset_name="CUSTOMERS",
        soda_qualified_dataset_name="test_ds/some/schema/CUSTOMERS",
        source=YamlFileContentInfo(source_content_str=None, local_file_path="fake.yml"),
    )


def _make_verification_result(check_results: list[CheckResult]) -> ContractVerificationResult:
    from datetime import datetime, timezone

    now = datetime.now(tz=timezone.utc)
    return ContractVerificationResult(
        check_collection=_make_contract(),
        data_source=DataSource(name="test_ds", type="postgres"),
        data_timestamp=now,
        started_timestamp=now,
        ended_timestamp=now,
        status=CheckCollectionStatus.PASSED,
        measurements=[],
        check_results=check_results,
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
        post_processing_stages=[],
    )


SENTINEL_DICT = {
    "identity": "custom-identity",
    "type": "anomalyDetection",
    "candidate": True,
    "metrics": ["some-metric-identity"],
}


class _OverridingCheckResult(CheckResult):
    def build_soda_cloud_check_dict(self, contract: Contract, wire_source: str) -> Optional[dict]:
        return dict(SENTINEL_DICT)


def test_base_check_result_hook_returns_none():
    check_result = CheckResult(check=_make_check(), outcome=CheckOutcome.PASSED)
    assert check_result.build_soda_cloud_check_dict(contract=_make_contract(), wire_source="soda-contract") is None


def test_plain_check_result_dict_equals_generic_builder():
    """With the base (None-returning) hook, the built check dict must equal the
    generic builder's output."""
    check_result = CheckResult(
        check=_make_check(),
        outcome=CheckOutcome.PASSED,
        diagnostic_metric_values={"check_rows_tested": 0, "dataset_rows_tested": 0},
    )
    verification_result = _make_verification_result([check_result])

    built = _build_check_results_cloud_json_dicts(verification_result, wire_source="soda-contract")
    expected = _build_check_result_cloud_dict(
        contract=_make_contract(), check_result=check_result, wire_source="soda-contract"
    )

    assert built == [expected]


def test_override_dict_lands_verbatim_in_checks_list():
    override_result = _OverridingCheckResult(check=_make_check(), outcome=CheckOutcome.NOT_EVALUATED)
    plain_result = CheckResult(
        check=_make_check(),
        outcome=CheckOutcome.PASSED,
        diagnostic_metric_values={"check_rows_tested": 0, "dataset_rows_tested": 0},
    )
    verification_result = _make_verification_result([override_result, plain_result])

    built = _build_check_results_cloud_json_dicts(verification_result, wire_source="other-subtype")

    assert built[0] == SENTINEL_DICT, "Non-None hook return must be used verbatim"
    assert built[1] == _build_check_result_cloud_dict(
        contract=_make_contract(), check_result=plain_result, wire_source="other-subtype"
    ), "Sibling plain CheckResults keep the generic-builder dict"
