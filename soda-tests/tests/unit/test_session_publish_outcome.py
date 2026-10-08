"""What a run that publishes to Soda Cloud sends when a collection errors or cannot be sent.

Every collection's outcome must reach Soda Cloud, or the scan is marked failed and the
exit code says so. No collection may drop out of a combined upload that then reads as a
clean run:

* a collection that errors before it has check results goes up in the combined upload
  when a sibling evaluated checks, so the upload has errors and carries the error;
* when nothing else evaluated a check, a managed scan is marked failed instead, so a
  check filter that selects only the broken collection never goes up clean;
* a collection whose file upload is rejected stays out of the combined upload, which
  still goes up with its other files, with errors and an error record naming the file
  left out, and the run exits RESULTS_NOT_SENT_TO_CLOUD; a contract whose file upload is
  rejected exits the same way;
* when nothing went up, a managed scan is marked failed once, with the records of every
  file, since the launcher commands that verify do not mark it; an ad-hoc run sends no
  mark;
* a scan is never marked failed after an insert that reached it or may have: a 5xx or a
  timeout can follow an insert Soda Cloud stored, and a mark would replace that scan's
  state and logs; only a 4xx rejection of the insert leads to a mark;
* a run where every collection succeeds sends what it always sent.

The first part drives the executor with a combine-upload test kind and no data source.
The second runs the same triggers with contracts on the test data source, in combined
mode and on the per-file path of ``soda contract verify``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from unittest.mock import patch

import pytest
import requests
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from helpers.test_functions import dedent_and_strip
from helpers.test_table import TestTable, TestTableSpecification
from soda_core.check_collections.base import CheckCollectionImpl, CheckCollectionResult, CheckCollectionYaml
from soda_core.check_collections.session import execute_check_collections
from soda_core.cli.exit_codes import ExitCode, session_result_to_exit_code
from soda_core.cli.handlers.contract import handle_verify_contract, interpret_contract_verification_result
from soda_core.cli.handlers.dependencies import resolve_soda_cloud_for_failure_report
from soda_core.cli.handlers.scan import run_scan
from soda_core.common import logs as logs_module
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Location, Logs
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import (
    Check,
    CheckCollectionStatus,
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationSession,
    ContractVerificationSessionResult,
    DataSource,
    YamlFileContentInfo,
)
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_core.contracts.impl.contract_verification_impl import ContractImpl, ContractVerificationHandlerRegistry

_KIND = "publish-outcome-test"
_RAISING_KIND = "publish-outcome-raise-test"
_WIRE_SOURCE = "outcome-collection"
_SCAN_ID = "scan-under-test"


class _SodaCloud(MockSodaCloud):
    """Accepts every command, except the file uploads whose contents hold
    ``reject_file_upload_containing`` and, when asked, the insert and the mark."""

    def __init__(
        self,
        reject_file_upload_containing: Optional[str] = None,
        reject_insert_with: Optional[int] = None,
        insert_times_out: bool = False,
        reject_mark: bool = False,
    ):
        super().__init__()
        self._reject_file_upload_containing = reject_file_upload_containing
        self._reject_insert_with = reject_insert_with
        self._insert_times_out = insert_times_out
        self._reject_mark = reject_mark

    def _http_handle(self, method, url, headers, json, data):
        super()._http_handle(method=method, url=url, headers=headers, json=json, data=data)
        command_type: Optional[str] = json.get("type") if isinstance(json, dict) else None
        if command_type == "sodaCoreUploadContractFile":
            if self._reject_file_upload_containing and self._reject_file_upload_containing in json["contents"]:
                return MockResponse(status_code=500, json_object={"message": "rejected"})
            return MockResponse(json_object={"fileId": f"file-{len(self.requests)}"})
        if command_type == "sodaCoreInsertScanResults":
            if self._insert_times_out:
                raise requests.exceptions.ReadTimeout("Soda Cloud did not answer in time")
            if self._reject_insert_with:
                return MockResponse(status_code=self._reject_insert_with, json_object={"message": "rejected"})
            return MockResponse(json_object={"scanId": _SCAN_ID})
        if command_type == "sodaCoreMarkScanFailed" and self._reject_mark:
            return MockResponse(status_code=500, json_object={"message": "rejected"})
        return MockResponse(json_object={})

    def requests_of_type(self, command_type: str) -> list[dict]:
        return [
            request.json
            for request in self.requests
            if isinstance(request.json, dict) and request.json.get("type") == command_type
        ]


def _log_messages(command: dict, level: Optional[str] = None) -> list[str]:
    return [log["message"] for log in command.get("logs") or [] if level is None or log["level"] == level]


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """No post-processing handler runs against the test kind, the active capture
    target is reset around each test, and no scan definition leaks in from the env."""
    monkeypatch.delenv("SODA_SCAN_DEFINITION", raising=False)
    registry_before = list(ContractVerificationHandlerRegistry.contract_verification_handlers)
    ContractVerificationHandlerRegistry.contract_verification_handlers[:] = []
    logs_module._active_logs.set(None)
    try:
        yield
    finally:
        ContractVerificationHandlerRegistry.contract_verification_handlers[:] = registry_before
        logs_module._active_logs.set(None)


def _set_scan_id(monkeypatch, managed: bool) -> None:
    if managed:
        monkeypatch.setenv("SODA_SCAN_ID", _SCAN_ID)
    else:
        monkeypatch.delenv("SODA_SCAN_ID", raising=False)


# ---------------------------------------------------------------------------------------
# A combine-upload test kind
# ---------------------------------------------------------------------------------------


class _YamlObject:
    def __init__(self, kind: str):
        self._kind = kind

    def read_string_opt(self, key: str, env_var=None, default_value=None):
        return self._kind if key == "kind" else default_value


class _Source:
    """A yaml source whose label says how its collection behaves:

    * ``healthy-*`` evaluates one check, which passes;
    * ``excluded-*`` has one check, which a check filter left out;
    * ``unparseable-*`` logs an error and has no check results;
    * ``unbuildable-*`` raises while its collection is built;
    * ``broken-yaml`` raises when parsed, before its kind is known.
    """

    def __init__(self, label: str):
        self.label = label
        self.file_path = f"{label}.yml"
        self.yaml_str_original = f"# {label}"

    def parse(self) -> _YamlObject:
        if self.label == "broken-yaml":
            raise ValueError("broken-yaml.yml is not valid YAML")
        return _YamlObject(_RAISING_KIND if self.label.startswith("unbuildable") else _KIND)


class _Yaml(CheckCollectionYaml):
    pass


class _Result(CheckCollectionResult):
    pass


def _check_result(label: str, outcome: CheckOutcome) -> CheckResult:
    path = f"checks.{label}"
    return CheckResult(
        check=Check(
            column_name=None,
            type="row_count",
            qualifier=None,
            name=f"{label} has rows",
            relative_path=path,
            check_path=path,
            identity=f"identity-{label}",
            definition="row_count:",
            contract_file_line=1,
            contract_file_column=1,
            threshold=None,
            attributes={},
            location=Location(file_path=f"{label}.yml", line=1, column=1),
        ),
        outcome=outcome,
        diagnostic_metric_values={"check_rows_tested": 1, "dataset_rows_tested": 1},
    )


class _OutcomeImpl(CheckCollectionImpl):
    """Combine-upload test kind. verify() uploads its file the way the base verify()
    does but, like a subtype with its own verify(), leaves a rejected upload unflagged:
    the session must catch that itself."""

    kind = _KIND
    wire_source = _WIRE_SOURCE
    display_name = "outcome collection"
    yaml_class = _Yaml
    result_class = _Result
    combine_uploads = True

    def __init__(self, yaml, logs=None, soda_cloud_impl=None, publish_results=False, **kwargs):
        self.logs = logs if logs is not None else Logs()
        self.yaml = yaml
        self.label = yaml.yaml_source.label
        self.logs.label = self.thread_label
        self.soda_cloud = soda_cloud_impl
        self.publish_results = publish_results
        self.data_source_impl = None
        soda_logger.info(f"Built {self.label}")

    @property
    def collection_id(self) -> Optional[str]:
        return self.label

    def verify(self) -> _Result:
        check_results: list[CheckResult] = []
        if self.label.startswith("unparseable"):
            soda_logger.error(f"{self.label} does not parse")
        elif self.label.startswith("excluded"):
            check_results = [_check_result(self.label, CheckOutcome.EXCLUDED)]
        else:
            check_results = [_check_result(self.label, CheckOutcome.PASSED)]
        status = CheckCollectionStatus.ERROR if self.logs.has_errors else CheckCollectionStatus.PASSED
        file_id: Optional[str] = (
            self.soda_cloud._upload_contract_yaml_file(self.yaml.yaml_source.yaml_str_original)
            if self.soda_cloud and self.publish_results
            else None
        )
        now = datetime.now(tz=timezone.utc)
        return _Result(
            check_collection=Contract(
                data_source_name="fake_ds",
                dataset_prefix=["main"],
                dataset_name="customers",
                soda_qualified_dataset_name="fake_ds/main/customers",
                source=YamlFileContentInfo(
                    source_content_str=self.yaml.yaml_source.yaml_str_original,
                    local_file_path=self.yaml.yaml_source.file_path,
                    soda_cloud_file_id=file_id,
                ),
            ),
            data_source=DataSource(name="fake_ds", type="duckdb"),
            data_timestamp=now,
            started_timestamp=now,
            ended_timestamp=now,
            status=status,
            measurements=[],
            check_results=check_results,
            sending_results_to_soda_cloud_failed=False,
            log_records=self.logs.get_log_records(),
            post_processing_stages=[],
        )


class _UnbuildableImpl(_OutcomeImpl):
    kind = _RAISING_KIND

    def __init__(self, yaml, logs=None, **kwargs):
        self.logs = logs if logs is not None else Logs()
        raise ValueError(f"{yaml.yaml_source.label}.yml names no dataset")


def _verify(
    monkeypatch,
    labels: list[str],
    managed: bool,
    soda_cloud: Optional[_SodaCloud] = None,
    logs: Optional[Logs] = None,
) -> tuple[list[CheckCollectionResult], ExitCode, _SodaCloud]:
    _set_scan_id(monkeypatch, managed)
    soda_cloud = soda_cloud if soda_cloud is not None else _SodaCloud()
    session_result = execute_check_collections(
        yaml_sources=[_Source(label) for label in labels],
        data_source_impl=None,
        soda_cloud_impl=soda_cloud,
        publish_results=True,
        default_impl_class=_OutcomeImpl,
        logs=logs,
    )
    return session_result.results, session_result_to_exit_code(session_result), soda_cloud


_MANAGED = pytest.mark.parametrize("managed", [True, False], ids=["managed", "ad_hoc"])

# How an insert can fail, and whether Soda Cloud may still have stored it. A 4xx rejects
# the insert. A 5xx or a timeout can come after Soda Cloud committed it.
_INSERT_FAILURES = pytest.mark.parametrize(
    "insert_failure, insert_may_have_landed",
    [
        (dict(reject_insert_with=400), False),
        (dict(reject_insert_with=504), True),
        (dict(insert_times_out=True), True),
    ],
    ids=["4xx", "5xx", "timeout"],
)


@_MANAGED
@pytest.mark.parametrize(
    "labels",
    [["healthy-a"], ["healthy-a", "healthy-b"], ["healthy-a", "excluded-b"], ["excluded-a", "excluded-b"]],
    ids=["one", "two", "one_excluded", "all_excluded"],
)
def test_every_collection_succeeding_sends_all_of_them_in_one_insert(monkeypatch, managed: bool, labels: list[str]):
    """Every result goes up in one insert, in session order, and nothing else reaches Soda Cloud."""
    results, exit_code, soda_cloud = _verify(monkeypatch, labels, managed)

    [insert] = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    assert [check["checkPath"] for check in insert["checks"]] == [f"checks.{label}" for label in labels]
    assert {check["source"] for check in insert["checks"]} == {_WIRE_SOURCE}
    assert insert["definitionName"] == "fake_ds/main/customers"
    assert insert["hasErrors"] is False
    assert soda_cloud.requests_of_type("sodaCoreMarkScanFailed") == []
    assert exit_code == ExitCode.OK
    assert all(result.scan_id == _SCAN_ID for result in results)


@_MANAGED
@pytest.mark.parametrize(
    "labels",
    [["unparseable-a", "healthy-b"], ["healthy-a", "unparseable-b"]],
    ids=["errored_first", "errored_last"],
)
def test_collection_erroring_before_results_goes_up_with_its_evaluated_sibling(
    monkeypatch, managed: bool, labels: list[str]
):
    results, exit_code, soda_cloud = _verify(monkeypatch, labels, managed)

    [insert] = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    assert insert["hasErrors"] is True
    [healthy_label] = [label for label in labels if label.startswith("healthy")]
    [unparseable_label] = [label for label in labels if label.startswith("unparseable")]
    assert [check["checkPath"] for check in insert["checks"]] == [f"checks.{healthy_label}"]
    assert f"{unparseable_label} does not parse" in _log_messages(insert, level="error")
    assert soda_cloud.requests_of_type("sodaCoreMarkScanFailed") == []
    assert exit_code == ExitCode.LOG_ERRORS
    assert all(result.scan_id == _SCAN_ID for result in results)


@_MANAGED
@pytest.mark.parametrize(
    "failing_label, error_message",
    [
        ("unbuildable-a", "unbuildable-a.yml names no dataset"),
        ("broken-yaml", "broken-yaml.yml is not valid YAML"),
    ],
    ids=["build_fails", "yaml_fails"],
)
def test_collection_failing_to_build_goes_up_with_its_evaluated_sibling(
    monkeypatch, managed: bool, failing_label: str, error_message: str
):
    """A file that never became a collection has no file of its own on Soda Cloud. It
    still counts: its error rides along in the sibling's upload, which leads it."""
    results, exit_code, soda_cloud = _verify(monkeypatch, [failing_label, "healthy-b"], managed)

    [insert] = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    assert insert["hasErrors"] is True
    assert insert["definitionName"] == "fake_ds/main/customers"
    assert insert["defaultDataSource"] == "fake_ds"
    assert [check["checkPath"] for check in insert["checks"]] == ["checks.healthy-b"]
    assert any(error_message in message for message in _log_messages(insert, level="error"))
    assert soda_cloud.requests_of_type("sodaCoreMarkScanFailed") == []
    assert exit_code == ExitCode.LOG_ERRORS


@pytest.mark.parametrize(
    "labels, error_message",
    [
        (["unparseable-a", "excluded-b"], "unparseable-a does not parse"),
        (["excluded-a", "unparseable-b"], "unparseable-b does not parse"),
        (["unparseable-a"], "unparseable-a does not parse"),
        (["unbuildable-a"], "unbuildable-a.yml names no dataset"),
        (["broken-yaml"], "broken-yaml.yml is not valid YAML"),
        (["broken-yaml", "excluded-b"], "broken-yaml.yml is not valid YAML"),
    ],
    ids=[
        "filter_selects_only_the_errored_one",
        "filter_selects_only_the_errored_last_one",
        "lone_errored",
        "lone_unbuildable",
        "lone_broken_yaml",
        "broken_yaml_next_to_excluded",
    ],
)
def test_errors_with_nothing_evaluated_mark_a_managed_scan_failed(monkeypatch, labels: list[str], error_message: str):
    """Nothing in the session evaluated a check, so an upload could only carry excluded
    checks next to the error. The scan is marked failed once instead, with every
    collection's records, and nothing is inserted."""
    results, exit_code, soda_cloud = _verify(monkeypatch, labels, managed=True)

    assert soda_cloud.requests_of_type("sodaCoreInsertScanResults") == []
    [mark] = soda_cloud.requests_of_type("sodaCoreMarkScanFailed")
    assert mark["scanId"] == _SCAN_ID
    assert any(error_message in message for message in _log_messages(mark, level="error"))
    for label in labels:
        if label.startswith("excluded"):
            assert f"Built {label}" in _log_messages(mark)
    assert exit_code == ExitCode.LOG_ERRORS
    assert not any(result.sending_results_to_soda_cloud_failed for result in results)


@pytest.mark.parametrize(
    "labels, command_type",
    [
        (["healthy-a", "healthy-b"], "sodaCoreInsertScanResults"),
        (["unparseable-a", "excluded-b"], "sodaCoreMarkScanFailed"),
    ],
    ids=["combined_insert", "failure_mark"],
)
def test_a_record_logged_before_the_session_goes_up_once(monkeypatch, labels: list[str], command_type: str):
    """What the caller logged before a multi-file session belongs to no one file. The combined
    insert and the failure mark each carry it once, ahead of the files' own records."""
    caller_logs = Logs()
    soda_logger.warning("before-session")
    _, _, soda_cloud = _verify(monkeypatch, labels, managed=True, logs=caller_logs)

    [command] = soda_cloud.requests_of_type(command_type)
    assert _log_messages(command)[0] == "before-session"
    assert _log_messages(command).count("before-session") == 1


def test_rejected_mark_of_errors_with_nothing_evaluated_exits_results_not_sent(monkeypatch):
    results, exit_code, soda_cloud = _verify(
        monkeypatch, ["unparseable-a", "excluded-b"], managed=True, soda_cloud=_SodaCloud(reject_mark=True)
    )

    assert soda_cloud.requests_of_type("sodaCoreInsertScanResults") == []
    assert len(soda_cloud.requests_of_type("sodaCoreMarkScanFailed")) == 1
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_ad_hoc_errors_with_nothing_evaluated_go_up_as_an_errored_upload(monkeypatch):
    """An ad-hoc run has no scan to mark: the upload creates it, with errors."""
    results, exit_code, soda_cloud = _verify(monkeypatch, ["unparseable-a", "excluded-b"], managed=False)

    [insert] = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    assert insert["hasErrors"] is True
    assert insert["resultsIngestionMode"] == "partial"
    assert "unparseable-a does not parse" in _log_messages(insert, level="error")
    assert soda_cloud.requests_of_type("sodaCoreMarkScanFailed") == []
    assert exit_code == ExitCode.LOG_ERRORS


def test_ad_hoc_file_that_never_became_a_collection_sends_nothing_and_exits_3(monkeypatch):
    """No collection to build a scan from and no scan to mark. The error is on the
    console, and the run exits 3 as an ad-hoc run that fails before its results does."""
    results, exit_code, soda_cloud = _verify(monkeypatch, ["broken-yaml"], managed=False)

    assert soda_cloud.requests == []
    assert exit_code == ExitCode.LOG_ERRORS


_REJECTED_NEXT_TO_ERRORED = pytest.mark.parametrize(
    "labels, rejected_label",
    [
        (["healthy-a", "unparseable-b"], "healthy-a"),
        (["excluded-a", "unparseable-b"], "excluded-a"),
        (["healthy-a", "unparseable-b", "healthy-c"], "healthy-c"),
        (["broken-yaml", "healthy-b", "healthy-c"], "healthy-c"),
    ],
    ids=["next_to_errored", "next_to_errored_nothing_evaluated", "next_to_errored_and_healthy", "next_to_broken_yaml"],
)


def _not_sent_errors(insert: dict, label: str) -> list[str]:
    return [
        message
        for message in _log_messages(insert, level="error")
        if f"{label}.yml" in message and "is not part of this upload" in message
    ]


@_MANAGED
def test_lone_rejected_file_upload_sends_nothing_and_exits_results_not_sent(monkeypatch, managed: bool):
    """Soda Cloud rejected the only file, so there is nothing to build an upload from. A
    managed scan is marked failed once, with the file's records, since no launcher command
    that verifies marks it. An ad-hoc run has no scan to mark."""
    results, exit_code, soda_cloud = _verify(
        monkeypatch, ["healthy-a"], managed, soda_cloud=_SodaCloud(reject_file_upload_containing="healthy-a")
    )

    assert soda_cloud.requests_of_type("sodaCoreInsertScanResults") == []
    marks = soda_cloud.requests_of_type("sodaCoreMarkScanFailed")
    if managed:
        [mark] = marks
        assert mark["scanId"] == _SCAN_ID
        assert any("did not upload to Soda Cloud" in message for message in _log_messages(mark, level="error"))
        assert "Built healthy-a" in _log_messages(mark)
    else:
        assert marks == []
    assert all(result.sending_results_to_soda_cloud_failed for result in results)
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


@_MANAGED
@pytest.mark.parametrize(
    "labels, rejected_label",
    [
        (["healthy-a", "healthy-b"], "healthy-b"),
        (["healthy-a", "healthy-b"], "healthy-a"),
    ],
    ids=["second_of_two", "first_of_two"],
)
def test_rejected_file_upload_stays_out_of_an_upload_that_names_it_and_has_errors(
    monkeypatch, managed: bool, labels: list[str], rejected_label: str
):
    """Soda Cloud rejected one file, so its collection cannot be part of the scan. The
    others still go up, in an insert with errors and an error record that names the file
    left out, so it never reads as a clean run. Nothing is marked after that insert, and
    the run exits RESULTS_NOT_SENT_TO_CLOUD for the file left out."""
    results, exit_code, soda_cloud = _verify(
        monkeypatch, labels, managed, soda_cloud=_SodaCloud(reject_file_upload_containing=rejected_label)
    )

    [sent_label] = [label for label in labels if label != rejected_label]
    [insert] = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    assert insert["hasErrors"] is True
    assert [check["checkPath"] for check in insert["checks"]] == [f"checks.{sent_label}"]
    assert insert["definitionName"] == "fake_ds/main/customers"
    assert _not_sent_errors(insert, rejected_label)
    assert any("did not upload to Soda Cloud" in message for message in _log_messages(insert, level="error"))
    assert soda_cloud.requests_of_type("sodaCoreMarkScanFailed") == []
    assert [result.sending_results_to_soda_cloud_failed for result in results] == [
        label == rejected_label for label in labels
    ]
    assert [result.scan_id for result in results] == [None if label == rejected_label else _SCAN_ID for label in labels]
    # The record lives in the upload, not in any file's results.
    assert not any("is not part of this upload" in error for result in results for error in result.get_errors())
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


@_MANAGED
@_REJECTED_NEXT_TO_ERRORED
def test_group_with_errors_goes_up_without_its_rejected_file(
    monkeypatch, managed: bool, labels: list[str], rejected_label: str
):
    """The other files go up in an insert with errors, which names the file left out, and
    the run exits RESULTS_NOT_SENT_TO_CLOUD for it. When none of the files that can go up
    evaluated a check, a managed scan is marked failed instead, with every file's records,
    and nothing is inserted."""
    results, exit_code, soda_cloud = _verify(
        monkeypatch, labels, managed, soda_cloud=_SodaCloud(reject_file_upload_containing=rejected_label)
    )

    sent_healthy_labels = [label for label in labels if label.startswith("healthy") and label != rejected_label]
    inserts = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    marks = soda_cloud.requests_of_type("sodaCoreMarkScanFailed")
    if managed and not sent_healthy_labels:
        assert inserts == []
        [mark] = marks
        assert mark["scanId"] == _SCAN_ID
        cloud_command = mark
        assert all(result.sending_results_to_soda_cloud_failed for result in results)
    else:
        [insert] = inserts
        assert insert["hasErrors"] is True
        assert [check["checkPath"] for check in insert.get("checks") or []] == [
            f"checks.{label}" for label in sent_healthy_labels
        ]
        assert _not_sent_errors(insert, rejected_label)
        assert marks == []
        cloud_command = insert
        assert [result.sending_results_to_soda_cloud_failed for result in results] == [
            label == rejected_label for label in labels
        ]
    errors = _log_messages(cloud_command, level="error")
    assert any("did not upload to Soda Cloud" in message for message in errors)
    for label in labels:
        if label.startswith("unparseable"):
            assert f"{label} does not parse" in errors
        elif label == "broken-yaml":
            assert any("broken-yaml.yml is not valid YAML" in message for message in errors)
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


@pytest.mark.parametrize("rejected_label", ["healthy-b", "excluded-b"], ids=["rejected_evaluated", "rejected_excluded"])
def test_managed_group_left_with_only_excluded_checks_and_no_error_goes_up(monkeypatch, rejected_label: str):
    """No file errored before its check results, so the group is not one that errored and
    evaluated no check. The file that can go up holds only an excluded check, and goes up
    with the stand-in that names the rejected file, instead of a mark."""
    labels = ["excluded-a", rejected_label]
    results, exit_code, soda_cloud = _verify(
        monkeypatch, labels, managed=True, soda_cloud=_SodaCloud(reject_file_upload_containing=rejected_label)
    )

    [insert] = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    assert insert["hasErrors"] is True
    assert [check["checkPath"] for check in insert["checks"]] == ["checks.excluded-a"]
    assert _not_sent_errors(insert, rejected_label)
    assert soda_cloud.requests_of_type("sodaCoreMarkScanFailed") == []
    assert [result.sending_results_to_soda_cloud_failed for result in results] == [False, True]
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_ad_hoc_group_left_with_only_a_file_that_never_became_a_collection_stays_held_back(monkeypatch):
    """Nothing is left to lead an upload, so nothing goes up and every result is flagged."""
    results, exit_code, soda_cloud = _verify(
        monkeypatch,
        ["broken-yaml", "healthy-b"],
        managed=False,
        soda_cloud=_SodaCloud(reject_file_upload_containing="healthy-b"),
    )

    assert soda_cloud.requests_of_type("sodaCoreInsertScanResults") == []
    assert soda_cloud.requests_of_type("sodaCoreMarkScanFailed") == []
    assert all(result.sending_results_to_soda_cloud_failed for result in results)
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


@_MANAGED
@_INSERT_FAILURES
def test_failed_insert_marks_a_managed_scan_failed_only_when_soda_cloud_rejected_it(
    monkeypatch, managed: bool, insert_failure: dict, insert_may_have_landed: bool
):
    """A 4xx means Soda Cloud did not store the insert, so a managed scan is marked failed
    with every file's records. After a 5xx or a timeout Soda Cloud may have stored it, and
    a mark would turn that completed scan FAILED and replace its logs, so there is none.
    Either way the run exits RESULTS_NOT_SENT_TO_CLOUD."""
    results, exit_code, soda_cloud = _verify(
        monkeypatch, ["healthy-a", "unparseable-b"], managed, soda_cloud=_SodaCloud(**insert_failure)
    )

    assert len(soda_cloud.requests_of_type("sodaCoreInsertScanResults")) == 1
    marks = soda_cloud.requests_of_type("sodaCoreMarkScanFailed")
    if managed and not insert_may_have_landed:
        [mark] = marks
        assert "unparseable-b does not parse" in _log_messages(mark, level="error")
        assert "Built healthy-a" in _log_messages(mark)
    else:
        assert marks == []
    assert all(result.sending_results_to_soda_cloud_failed for result in results)
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_held_back_group_is_marked_failed_once_even_when_the_mark_is_rejected(monkeypatch):
    results, exit_code, soda_cloud = _verify(
        monkeypatch,
        ["unparseable-a", "healthy-b"],
        managed=True,
        soda_cloud=_SodaCloud(reject_file_upload_containing="healthy-b", reject_mark=True),
    )

    assert soda_cloud.requests_of_type("sodaCoreInsertScanResults") == []
    assert len(soda_cloud.requests_of_type("sodaCoreMarkScanFailed")) == 1
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


# ---------------------------------------------------------------------------------------
# The same triggers with contracts
# ---------------------------------------------------------------------------------------

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("publish_outcome")
    .column_varchar("id")
    .rows(rows=[("1",), ("2",)])
    .build()
)

_HEALTHY_CONTRACT = """
    columns:
      - name: id
    checks:
      - row_count:
"""

# Errors while it is parsed, so it has no check results.
_UNPARSEABLE_CONTRACT = """
    columns:
      - name: id
    checks:
      - not_a_check_type:
"""

_REJECTED_UPLOAD_MARKER = "# Soda Cloud rejects the upload of this file"

_REJECTED_HEALTHY_CONTRACT = f"""
    {_REJECTED_UPLOAD_MARKER}
    columns:
      - name: id
    checks:
      - row_count:
"""


def _contract_source(
    data_source_test_helper: DataSourceTestHelper, test_table: TestTable, checks_yaml: str
) -> ContractYamlSource:
    return ContractYamlSource.from_str(
        f"dataset: {data_source_test_helper.build_dqn(test_table)}\n" + dedent_and_strip(checks_yaml)
    )


def _verify_contracts(
    data_source_test_helper: DataSourceTestHelper,
    monkeypatch,
    checks_yamls: list[str],
    managed: bool,
    soda_cloud: Optional[_SodaCloud] = None,
    check_selectors: Optional[list[CheckSelector]] = None,
) -> tuple[ContractVerificationSessionResult, ExitCode, _SodaCloud]:
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    _set_scan_id(monkeypatch, managed)
    soda_cloud = soda_cloud if soda_cloud is not None else _SodaCloud()
    session_result = ContractVerificationSession.execute(
        contract_yaml_sources=[
            _contract_source(data_source_test_helper, test_table, checks_yaml) for checks_yaml in checks_yamls
        ],
        data_source_impls=[data_source_test_helper.data_source_impl],
        soda_cloud_impl=soda_cloud,
        soda_cloud_publish_results=True,
        check_selectors=check_selectors,
    )
    return session_result, interpret_contract_verification_result(session_result), soda_cloud


@pytest.fixture
def combined_contracts(monkeypatch):
    """Contracts upload once per file. Combining them sends them down the path a
    combine-upload subtype takes."""
    monkeypatch.setattr(ContractImpl, "combine_uploads", True)


@_MANAGED
@pytest.mark.parametrize(
    "checks_yamls",
    [[_UNPARSEABLE_CONTRACT, _HEALTHY_CONTRACT], [_HEALTHY_CONTRACT, _UNPARSEABLE_CONTRACT]],
    ids=["errored_first", "errored_last"],
)
def test_combined_contract_erroring_before_results_goes_up_with_its_evaluated_sibling(
    data_source_test_helper: DataSourceTestHelper, monkeypatch, combined_contracts, managed: bool, checks_yamls
):
    session_result, exit_code, soda_cloud = _verify_contracts(
        data_source_test_helper, monkeypatch, checks_yamls, managed
    )

    [insert] = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    assert insert["hasErrors"] is True
    assert [check["checkPath"] for check in insert["checks"]] == ["checks.row_count"]
    assert any("not_a_check_type" in message for message in _log_messages(insert, level="error"))
    assert soda_cloud.requests_of_type("sodaCoreMarkScanFailed") == []
    assert exit_code == ExitCode.LOG_ERRORS


def test_combined_contract_filter_selecting_only_the_unparseable_one_marks_the_scan_failed(
    data_source_test_helper: DataSourceTestHelper, monkeypatch, combined_contracts
):
    """The filter excludes every check of the healthy contract, and the one it selects
    cannot be read. The scan is marked failed, not uploaded as a clean partial run."""
    session_result, exit_code, soda_cloud = _verify_contracts(
        data_source_test_helper,
        monkeypatch,
        [_UNPARSEABLE_CONTRACT, _HEALTHY_CONTRACT],
        managed=True,
        check_selectors=CheckSelector.parse_all(["type=missing"]),
    )

    assert soda_cloud.requests_of_type("sodaCoreInsertScanResults") == []
    [mark] = soda_cloud.requests_of_type("sodaCoreMarkScanFailed")
    assert mark["scanId"] == _SCAN_ID
    assert any("not_a_check_type" in message for message in _log_messages(mark, level="error"))
    assert exit_code == ExitCode.LOG_ERRORS


@_MANAGED
def test_lone_combined_contract_with_a_rejected_file_upload_sends_no_results_and_exits_results_not_sent(
    data_source_test_helper: DataSourceTestHelper, monkeypatch, combined_contracts, managed: bool
):
    session_result, exit_code, soda_cloud = _verify_contracts(
        data_source_test_helper,
        monkeypatch,
        [_REJECTED_HEALTHY_CONTRACT],
        managed,
        soda_cloud=_SodaCloud(reject_file_upload_containing=_REJECTED_UPLOAD_MARKER),
    )

    assert soda_cloud.requests_of_type("sodaCoreInsertScanResults") == []
    marks = soda_cloud.requests_of_type("sodaCoreMarkScanFailed")
    if managed:
        [mark] = marks
        assert any("did not upload to Soda Cloud" in message for message in _log_messages(mark, level="error"))
    else:
        assert marks == []
    assert all(result.sending_results_to_soda_cloud_failed for result in session_result.contract_verification_results)
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


@_MANAGED
def test_combined_contract_with_a_rejected_file_upload_goes_up_without_it_and_with_errors(
    data_source_test_helper: DataSourceTestHelper, monkeypatch, combined_contracts, managed: bool
):
    session_result, exit_code, soda_cloud = _verify_contracts(
        data_source_test_helper,
        monkeypatch,
        [_HEALTHY_CONTRACT, _REJECTED_HEALTHY_CONTRACT],
        managed,
        soda_cloud=_SodaCloud(reject_file_upload_containing=_REJECTED_UPLOAD_MARKER),
    )

    [insert] = soda_cloud.requests_of_type("sodaCoreInsertScanResults")
    assert insert["hasErrors"] is True
    assert [check["checkPath"] for check in insert["checks"]] == ["checks.row_count"]
    assert any("is not part of this upload" in message for message in _log_messages(insert, level="error"))
    assert soda_cloud.requests_of_type("sodaCoreMarkScanFailed") == []
    assert [r.sending_results_to_soda_cloud_failed for r in session_result.contract_verification_results] == [
        False,
        True,
    ]
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


@_MANAGED
def test_contract_with_a_rejected_file_upload_exits_results_not_sent(
    data_source_test_helper: DataSourceTestHelper, monkeypatch, managed: bool
):
    """The per-file path of a single contract: no results reach Soda Cloud, so the run
    must not exit 0, the reason is in the results, and a managed scan is marked failed
    with the contract's records."""
    session_result, exit_code, soda_cloud = _verify_contracts(
        data_source_test_helper,
        monkeypatch,
        [_REJECTED_HEALTHY_CONTRACT],
        managed,
        soda_cloud=_SodaCloud(reject_file_upload_containing=_REJECTED_UPLOAD_MARKER),
    )

    [result] = session_result.contract_verification_results
    assert result.status is CheckCollectionStatus.PASSED
    assert result.sending_results_to_soda_cloud_failed is True
    assert any("Not sending results to Soda Cloud" in error for error in result.get_errors())
    assert soda_cloud.requests_of_type("sodaCoreInsertScanResults") == []
    marks = soda_cloud.requests_of_type("sodaCoreMarkScanFailed")
    if managed:
        [mark] = marks
        assert mark["scanId"] == _SCAN_ID
        assert any("did not upload to Soda Cloud" in message for message in _log_messages(mark, level="error"))
        assert result.scan_id is None
    else:
        assert marks == []
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


@_MANAGED
@_INSERT_FAILURES
def test_contract_with_a_failed_insert_exits_results_not_sent_and_marks_only_a_rejected_one(
    data_source_test_helper: DataSourceTestHelper,
    monkeypatch,
    managed: bool,
    insert_failure: dict,
    insert_may_have_landed: bool,
):
    session_result, exit_code, soda_cloud = _verify_contracts(
        data_source_test_helper,
        monkeypatch,
        [_HEALTHY_CONTRACT],
        managed,
        soda_cloud=_SodaCloud(**insert_failure),
    )

    assert len(soda_cloud.requests_of_type("sodaCoreInsertScanResults")) == 1
    marks = soda_cloud.requests_of_type("sodaCoreMarkScanFailed")
    assert len(marks) == (1 if managed and not insert_may_have_landed else 0)
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_contract_whose_mark_is_rejected_is_marked_once(data_source_test_helper: DataSourceTestHelper, monkeypatch):
    """The contract errored before its results, so its managed scan is marked failed. Soda
    Cloud rejects the mark and the run exits RESULTS_NOT_SENT_TO_CLOUD, with no second mark."""
    session_result, exit_code, soda_cloud = _verify_contracts(
        data_source_test_helper,
        monkeypatch,
        [_UNPARSEABLE_CONTRACT],
        managed=True,
        soda_cloud=_SodaCloud(reject_mark=True),
    )

    assert soda_cloud.requests_of_type("sodaCoreInsertScanResults") == []
    assert len(soda_cloud.requests_of_type("sodaCoreMarkScanFailed")) == 1
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


@_MANAGED
def test_contract_verify_cli_with_a_rejected_file_upload_exits_results_not_sent(monkeypatch, tmp_path, managed: bool):
    """soda contract verify end to end through its failure boundary: the file upload is
    rejected, no results reach Soda Cloud, a managed scan is marked failed once, and the
    CLI exits RESULTS_NOT_SENT_TO_CLOUD."""
    import duckdb

    _set_scan_id(monkeypatch, managed)
    db_path = tmp_path / "test.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute("CREATE TABLE my_table (id VARCHAR)")
    connection.execute("INSERT INTO my_table VALUES ('a')")
    connection.close()
    data_source_path = tmp_path / "ds.yaml"
    data_source_path.write_text(
        "type: duckdb\nname: test_ds\nconnection:\n" f'    database: "{db_path}"\n' "    schema: main\n"
    )
    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text(
        f"{_REJECTED_UPLOAD_MARKER}\ndataset: test_ds/main/my_table\ncolumns:\n  - name: id\nchecks:\n  - row_count:\n"
    )
    soda_cloud = _SodaCloud(reject_file_upload_containing=_REJECTED_UPLOAD_MARKER)

    with patch("soda_core.common.soda_cloud.SodaCloud.from_config", return_value=soda_cloud):
        exit_code = run_scan(
            resolve_soda_cloud_for_failure_report("sc.yaml", {}),
            lambda logs: handle_verify_contract(
                contract_file_path=str(contract_path),
                dataset_identifier=None,
                data_source_file_paths=[str(data_source_path)],
                soda_cloud_file_path="sc.yaml",
                variables={},
                publish=True,
                verbose=False,
                logs=logs,
            ),
        )

    assert len(soda_cloud.requests_of_type("sodaCoreUploadContractFile")) == 1
    assert soda_cloud.requests_of_type("sodaCoreInsertScanResults") == []
    marks = soda_cloud.requests_of_type("sodaCoreMarkScanFailed")
    if managed:
        [mark] = marks
        assert any("did not upload to Soda Cloud" in message for message in _log_messages(mark, level="error"))
    else:
        assert marks == []
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
