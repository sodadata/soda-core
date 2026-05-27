"""Lock the ``wire_source`` plumbing from the impl class to the Cloud payload.

Asserts that ``ContractImpl.wire_source`` ("soda-contract") flows through
``send_contract_result`` → ``_build_contract_result_json_dict`` →
``_build_check_result_cloud_dict`` to the emitted check dict's ``"source"``
field. Any future subtype declaring its own ``wire_source`` flows through
the same path without engine changes.
"""

from __future__ import annotations

from soda_core.check_collections.base import CheckCollectionImpl
from soda_core.common.logs import Location
from soda_core.common.soda_cloud import (
    _build_check_result_cloud_dict,
    _build_check_results_cloud_json_dicts,
    _build_contract_result_json_dict,
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
from soda_core.contracts.impl.contract_verification_impl import ContractImpl


def _make_check_result() -> CheckResult:
    check = Check(
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
    return CheckResult(
        check=check,
        outcome=CheckOutcome.PASSED,
        diagnostic_metric_values={"check_rows_tested": 0, "dataset_rows_tested": 0},
    )


def _make_contract() -> Contract:
    return Contract(
        data_source_name="test_ds",
        dataset_prefix=["some", "schema"],
        dataset_name="CUSTOMERS",
        soda_qualified_dataset_name="test_ds/some/schema/CUSTOMERS",
        source=YamlFileContentInfo(source_content_str=None, local_file_path="fake.yml"),
    )


def test_contract_impl_declares_soda_contract_wire_source():
    """The class attribute is the source of truth — nothing else."""
    assert ContractImpl.wire_source == "soda-contract"


def test_base_check_collection_impl_has_empty_wire_source():
    """The base declares empty; subclasses must override.

    Catches accidental hardcoding back into the engine.
    """
    assert CheckCollectionImpl.wire_source == ""


def test_build_check_result_cloud_dict_uses_supplied_wire_source():
    """``_build_check_result_cloud_dict(wire_source=...)`` stamps the value on each check.

    The contract a future non-contract subtype relies on — its
    ``wire_source`` class attribute will flow through ``verify()`` →
    ``send_contract_result`` → ``_build_check_result_cloud_dict`` to the
    payload without engine changes.
    """
    emitted_default = _build_check_result_cloud_dict(contract=_make_contract(), check_result=_make_check_result())
    assert (
        emitted_default["source"] == "soda-contract"
    ), "Default kwarg must preserve BC for callers that don't pass wire_source"

    emitted_explicit = _build_check_result_cloud_dict(
        contract=_make_contract(),
        check_result=_make_check_result(),
        wire_source="soda-contract",
    )
    assert emitted_explicit["source"] == "soda-contract"

    emitted_future = _build_check_result_cloud_dict(
        contract=_make_contract(),
        check_result=_make_check_result(),
        wire_source="other-subtype",
    )
    assert emitted_future["source"] == "other-subtype"


def test_build_contract_result_json_dict_threads_wire_source_through_to_checks():
    """End-to-end (in-memory): wire_source on the outer call lands on every check."""
    from datetime import datetime, timezone

    now = datetime.now(tz=timezone.utc)
    verification_result = ContractVerificationResult(
        check_collection=_make_contract(),
        data_source=DataSource(name="test_ds", type="postgres"),
        data_timestamp=now,
        started_timestamp=now,
        ended_timestamp=now,
        status=CheckCollectionStatus.PASSED,
        measurements=[],
        check_results=[_make_check_result(), _make_check_result()],
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
        post_processing_stages=[],
    )

    payload = _build_contract_result_json_dict(verification_result, wire_source="other-subtype")
    assert payload["checks"], "Expected check dicts in the upload payload"
    for check_dict in payload["checks"]:
        assert check_dict["source"] == "other-subtype"


def test_build_check_results_cloud_json_dicts_defaults_to_soda_contract():
    """Helper-level BC: omitting wire_source still produces ``soda-contract``."""
    from datetime import datetime, timezone

    now = datetime.now(tz=timezone.utc)
    verification_result = ContractVerificationResult(
        check_collection=_make_contract(),
        data_source=DataSource(name="test_ds", type="postgres"),
        data_timestamp=now,
        started_timestamp=now,
        ended_timestamp=now,
        status=CheckCollectionStatus.PASSED,
        measurements=[],
        check_results=[_make_check_result()],
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
        post_processing_stages=[],
    )
    check_dicts = _build_check_results_cloud_json_dicts(verification_result)
    assert check_dicts is not None
    assert check_dicts[0]["source"] == "soda-contract"


def _make_verification_result_for_definition_name():
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
        check_results=[],
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
        post_processing_stages=[],
    )


def test_build_contract_result_definition_name_defaults_to_qualified_name():
    """N4: with no ``scan_definition_suffix``, the scan-def name is the
    dataset's qualified name verbatim — matching the contract default."""
    payload = _build_contract_result_json_dict(_make_verification_result_for_definition_name())
    assert payload["definitionName"] == "test_ds/some/schema/CUSTOMERS"


def test_build_contract_result_definition_name_applies_subtype_suffix():
    """A non-empty ``scan_definition_suffix`` appends ``_<suffix>`` to the
    qualified name; subtypes opt in by declaring the attribute on their
    impl class."""
    payload = _build_contract_result_json_dict(
        _make_verification_result_for_definition_name(),
        scan_definition_suffix="other_subtype_scan",
    )
    assert payload["definitionName"] == "test_ds/some/schema/CUSTOMERS_other_subtype_scan"


def test_contract_impl_declares_no_scan_definition_suffix():
    """Contracts use the bare qualified name — no suffix, no surprises."""
    assert ContractImpl.scan_definition_suffix is None


def test_base_check_collection_impl_declares_no_scan_definition_suffix():
    """Base default is ``None``; subtypes opt in by declaring the class attribute."""
    assert CheckCollectionImpl.scan_definition_suffix is None


def test_build_scan_definition_name_does_not_branch_on_wire_source():
    """The scan-def name no longer branches on ``wire_source``. Any
    non-contract ``wire_source`` value passed without a suffix must
    yield the bare qualified name — subtypes drive scan-def naming
    via ``scan_definition_suffix``, not via wire_source literals."""
    payload = _build_contract_result_json_dict(
        _make_verification_result_for_definition_name(),
        wire_source="other-subtype",
    )
    assert payload["definitionName"] == "test_ds/some/schema/CUSTOMERS", (
        "wire_source must not influence scan-def naming any more — subtypes " "drive that via scan_definition_suffix."
    )
