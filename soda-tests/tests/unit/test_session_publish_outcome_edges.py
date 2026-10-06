"""Corners next to the publish-outcome cases in ``test_session_publish_outcome``.

Two wire sources in one session, a per-file contract rejected after its sibling went up,
a session without a default subtype, and a collection that errored next to a rejected
file upload. In each, every upload that goes up carries the errors of what it leaves
out, or, when nothing goes up, a managed scan is marked failed with the files' records.
What did not reach Soda Cloud makes the run exit RESULTS_NOT_SENT_TO_CLOUD.
"""

from __future__ import annotations

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.check_collections.session import execute_check_collections
from soda_core.cli.exit_codes import ExitCode, session_result_to_exit_code
from soda_core.contracts.impl.contract_verification_impl import ContractImpl
from unit import test_session_publish_outcome as outcome


class _OtherWireSourceImpl(outcome._OutcomeImpl):
    kind = "publish-outcome-other-wire-source-test"
    wire_source = "other-outcome-collection"


class _OtherWireSource(outcome._Source):
    def parse(self) -> outcome._YamlObject:
        return outcome._YamlObject(_OtherWireSourceImpl.kind)


def _execute(monkeypatch, sources, managed: bool, default_impl_class, soda_cloud=None):
    outcome._set_scan_id(monkeypatch, managed)
    soda_cloud = soda_cloud if soda_cloud is not None else outcome._SodaCloud()
    session_result = execute_check_collections(
        yaml_sources=sources,
        data_source_impl=None,
        soda_cloud_impl=soda_cloud,
        publish_results=True,
        default_impl_class=default_impl_class,
    )
    return session_result.results, session_result_to_exit_code(session_result), soda_cloud


def test_errored_group_next_to_an_uploaded_group_of_another_wire_source_rides_along_in_its_insert(monkeypatch):
    """The errored group evaluated nothing, so alone it would mark the scan failed. The
    other group's insert reaches the scan, though, and a mark after it would turn that
    scan FAILED and replace its logs. So the errored group's records go up in that insert,
    which then has errors, and the run exits RESULTS_NOT_SENT_TO_CLOUD for its results."""
    results, exit_code, soda_cloud = _execute(
        monkeypatch,
        [outcome._Source("healthy-a"), _OtherWireSource("unparseable-b")],
        managed=True,
        default_impl_class=outcome._OutcomeImpl,
    )

    [insert] = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    assert [check["checkPath"] for check in insert["checks"]] == ["checks.healthy-a"]
    assert insert["hasErrors"] is True
    errors = outcome._log_messages(insert, level="error")
    assert "unparseable-b does not parse" in errors
    assert any("unparseable-b.yml is not part of this upload" in message for message in errors)
    assert soda_cloud.requests_of_type("sodaCoreMarkScanFailed") == []
    assert [result.sending_results_to_soda_cloud_failed for result in results] == [False, True]
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_ad_hoc_file_that_never_became_a_collection_next_to_excluded_goes_up_with_errors(monkeypatch):
    results, exit_code, soda_cloud = _execute(
        monkeypatch,
        [outcome._Source("broken-yaml"), outcome._Source("excluded-b")],
        managed=False,
        default_impl_class=outcome._OutcomeImpl,
    )

    [insert] = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    assert insert["hasErrors"] is True
    assert insert["resultsIngestionMode"] == "partial"
    assert [check["checkPath"] for check in insert["checks"]] == ["checks.excluded-b"]
    assert any("broken-yaml.yml is not valid YAML" in m for m in outcome._log_messages(insert, level="error"))
    assert exit_code == ExitCode.LOG_ERRORS


def test_per_file_contract_rejected_after_an_uploaded_sibling_marks_the_scan_failed(
    data_source_test_helper: DataSourceTestHelper, monkeypatch
):
    """Only the Python API runs several per-file contracts under one managed scan id: the
    CLI and the launcher verify one. Each file decides alone in verify(), so the rejected
    second file still marks a scan its sibling's insert completed."""
    session_result, exit_code, soda_cloud = outcome._verify_contracts(
        data_source_test_helper,
        monkeypatch,
        [outcome._HEALTHY_CONTRACT, outcome._REJECTED_HEALTHY_CONTRACT],
        managed=True,
        soda_cloud=outcome._SodaCloud(reject_file_upload_containing=outcome._REJECTED_UPLOAD_MARKER),
    )

    assert len(soda_cloud.requests_of_type("sodaCoreInsertScanResults")) == 1
    [mark] = soda_cloud.requests_of_type("sodaCoreMarkScanFailed")
    assert any("did not upload to Soda Cloud" in m for m in outcome._log_messages(mark, level="error"))
    assert [r.sending_results_to_soda_cloud_failed for r in session_result.contract_verification_results] == [
        False,
        True,
    ]
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


# ---------------------------------------------------------------------------------------
# A session without a default subtype
# ---------------------------------------------------------------------------------------


@outcome._MANAGED
def test_session_without_default_subtype_carries_a_file_of_unknown_kind_in_its_only_combined_upload(
    monkeypatch, managed: bool
):
    """With one combined upload in the session, the file whose kind is unknown can only
    belong to it, so it rides along the way it does with a default subtype."""
    results, exit_code, soda_cloud = _execute(
        monkeypatch,
        [outcome._Source("broken-yaml"), outcome._Source("healthy-b")],
        managed=managed,
        default_impl_class=None,
    )

    [insert] = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    assert insert["hasErrors"] is True
    assert [check["checkPath"] for check in insert["checks"]] == ["checks.healthy-b"]
    assert any("broken-yaml.yml is not valid YAML" in m for m in outcome._log_messages(insert, level="error"))
    assert soda_cloud.requests_of_type("sodaCoreMarkScanFailed") == []
    assert exit_code == ExitCode.LOG_ERRORS


@outcome._MANAGED
def test_session_without_default_subtype_flags_a_file_of_unknown_kind_next_to_two_combined_uploads(
    monkeypatch, managed: bool
):
    """With two combined uploads the file could belong to either, so neither upload can
    claim it: it is flagged as not sent. Its error goes up in both uploads, so neither
    reads clean, and no scan is marked failed after them."""
    results, exit_code, soda_cloud = _execute(
        monkeypatch,
        [outcome._Source("broken-yaml"), outcome._Source("healthy-b"), _OtherWireSource("healthy-c")],
        managed=managed,
        default_impl_class=None,
    )

    inserts = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    assert len(inserts) == 2
    for insert in inserts:
        assert insert["hasErrors"] is True
        assert any("broken-yaml.yml is not valid YAML" in m for m in outcome._log_messages(insert, level="error"))
    assert soda_cloud.requests_of_type("sodaCoreMarkScanFailed") == []
    assert [result.sending_results_to_soda_cloud_failed for result in results] == [True, False, False]
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


# ---------------------------------------------------------------------------------------
# A collection that errored before its results, next to a rejected file upload
# ---------------------------------------------------------------------------------------


_REJECTED_UNPARSEABLE_CONTRACT = f"""
    {outcome._REJECTED_UPLOAD_MARKER}
    columns:
      - name: id
    checks:
      - not_a_check_type:
"""


@pytest.mark.parametrize(
    "managed, combined, checks_yamls",
    [
        (True, True, [_REJECTED_UNPARSEABLE_CONTRACT]),
        (False, True, [_REJECTED_UNPARSEABLE_CONTRACT]),
        (True, True, [outcome._UNPARSEABLE_CONTRACT, outcome._REJECTED_HEALTHY_CONTRACT]),
        (True, False, [_REJECTED_UNPARSEABLE_CONTRACT]),
        (False, False, [_REJECTED_UNPARSEABLE_CONTRACT]),
    ],
    ids=[
        "combined_own_file_rejected-managed",
        "combined_own_file_rejected-ad_hoc",
        "combined_sibling_file_rejected-managed",
        "per_file_own_file_rejected-managed",
        "per_file_own_file_rejected-ad_hoc",
    ],
)
def test_collection_erroring_before_results_next_to_a_rejected_file_marks_the_scan_failed_with_its_error(
    data_source_test_helper: DataSourceTestHelper, monkeypatch, managed: bool, combined: bool, checks_yamls
):
    """A managed scan is marked failed once, with the parse error among its records, and
    nothing is inserted. An ad-hoc run whose own file was rejected sends nothing. Both
    exit RESULTS_NOT_SENT_TO_CLOUD."""
    if combined:
        monkeypatch.setattr(ContractImpl, "combine_uploads", True)
    session_result, exit_code, soda_cloud = outcome._verify_contracts(
        data_source_test_helper,
        monkeypatch,
        checks_yamls,
        managed=managed,
        soda_cloud=outcome._SodaCloud(reject_file_upload_containing=outcome._REJECTED_UPLOAD_MARKER),
    )

    assert soda_cloud.requests_of_type("sodaCoreInsertScanResults") == []
    marks = soda_cloud.requests_of_type("sodaCoreMarkScanFailed")
    if managed:
        [mark] = marks
        assert mark["scanId"] == outcome._SCAN_ID
        errors = outcome._log_messages(mark, level="error")
        assert any("not_a_check_type" in message for message in errors)
        assert any("did not upload to Soda Cloud" in message for message in errors)
    else:
        assert marks == []
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


# A check that errors while it is evaluated, so its collection has check results.
_EVAL_ERROR_CONTRACT = """
    columns:
      - name: id
    checks:
      - failed_rows:
          query: |
            SELECT * FROM no_such_table
"""


@pytest.mark.parametrize(
    "errored_contract, error_fragment",
    [(outcome._UNPARSEABLE_CONTRACT, "not_a_check_type"), (_EVAL_ERROR_CONTRACT, "no_such_table")],
    ids=["unparseable", "eval_error"],
)
def test_ad_hoc_combined_error_next_to_a_rejected_file_goes_up_without_it(
    data_source_test_helper: DataSourceTestHelper, monkeypatch, errored_contract: str, error_fragment: str
):
    """An ad-hoc run has no scan to mark, so the errored contract goes up without its
    rejected sibling, in an insert with errors, and the run exits RESULTS_NOT_SENT_TO_CLOUD."""
    monkeypatch.setattr(ContractImpl, "combine_uploads", True)
    session_result, exit_code, soda_cloud = outcome._verify_contracts(
        data_source_test_helper,
        monkeypatch,
        [errored_contract, outcome._REJECTED_HEALTHY_CONTRACT],
        managed=False,
        soda_cloud=outcome._SodaCloud(reject_file_upload_containing=outcome._REJECTED_UPLOAD_MARKER),
    )

    [insert] = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    assert insert["hasErrors"] is True
    assert any(error_fragment in message for message in outcome._log_messages(insert, level="error"))
    assert soda_cloud.requests_of_type("sodaCoreMarkScanFailed") == []
    assert [r.sending_results_to_soda_cloud_failed for r in session_result.contract_verification_results] == [
        False,
        True,
    ]
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
