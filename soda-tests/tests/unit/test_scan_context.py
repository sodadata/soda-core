import json
import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
import soda_core.common.logs_queue as logs_queue_module
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.scan import run_scan
from soda_core.common.exceptions import ScanExecutionFailedException
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.common.logs_collector import LogsCollector
from soda_core.common.logs_queue import LogsQueue
from soda_core.common.scan_context import AtomicScanContext, BatchedScanContext, get_scan_context, using_scan_context

# run_scan: the bracket every CLI results-publishing command runs under. The flow sources the
# installed ScanContext with get_scan_context() and opens the scan itself via start_scan once its
# dependencies resolve. These tests drive the REAL two-channel pipeline — a real LogsQueue streaming
# into a Cloud double that records the full request sequence — because the interesting properties
# are cross-channel: what the failure report carries relative to what the stream already took, and
# in what order the commands land.

DATA_TIMESTAMP = datetime(2026, 7, 13, 8, 30, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _no_retry_backoff(monkeypatch):
    monkeypatch.setattr(logs_queue_module, "RETRY_DELAY_SECONDS", 0)


@pytest.fixture(autouse=True)
def _deterministic_flush_cadence(monkeypatch):
    # The worker's cadence flush must never interleave with the exact-sequence assertions below:
    # batches ship only on the explicit flushes and the close. (The queue reads the module constant
    # at construction, which happens inside start_scan during each test.)
    monkeypatch.setattr(logs_queue_module, "DEFAULT_FLUSH_INTERVAL", 3600)


def _payload() -> dict:
    return {"type": "sodaCoreInsertScanResults", "definitionName": "my_scan"}


class _ScanLifecycleSodaCloud(MockSodaCloud):
    """MockSodaCloud answering by request kind instead of positionally, so interleaved log uploads
    never consume a response meant for a scan-lifecycle command."""

    def __init__(
        self,
        scan_start_status: int = 200,
        insert_status: int = 200,
        end_status: int = 200,
        log_upload_status: int = 200,
        scan_reference: str = "org/ref-1",
    ):
        super().__init__()
        self.scan_start_status = scan_start_status
        self.insert_status = insert_status
        self.end_status = end_status
        self.log_upload_status = log_upload_status
        self.scan_reference = scan_reference

    def _http_handle(self, method, url, headers, json, data):
        if data is not None and hasattr(data, "read"):
            data = data.read()
        from helpers.mock_soda_cloud import MockRequest

        self.requests.append(MockRequest(url=url, headers=headers, json=json, data=data))
        if url and "batchV4" in url:
            return MockResponse(status_code=self.log_upload_status, json_object={})
        command_type = json.get("type") if isinstance(json, dict) else None
        if command_type == "sodaCoreScanStart":
            return MockResponse(status_code=self.scan_start_status, json_object={"scanReference": self.scan_reference})
        if command_type in ("sodaCoreInsertScanDataBatch", "sodaCoreInsertScanResults"):
            return MockResponse(status_code=self.insert_status, json_object={})
        if command_type == "sodaCoreScanEndAsync":
            return MockResponse(status_code=self.end_status, json_object={})
        return MockResponse(status_code=200, json_object={})


def _request_kinds(mock_cloud: MockSodaCloud) -> list[str]:
    kinds = []
    for request in mock_cloud.requests:
        if request.url and "batchV4" in request.url:
            kinds.append("logsBatchV4")
        elif isinstance(request.json, dict) and request.json.get("type"):
            kinds.append(request.json["type"])
    return kinds


def _command_json(mock_cloud: MockSodaCloud, command_type: str) -> dict:
    return next(
        request.json
        for request in mock_cloud.requests
        if isinstance(request.json, dict) and request.json.get("type") == command_type
    )


def _streamed_messages(mock_cloud: MockSodaCloud) -> list[str]:
    messages = []
    for request in mock_cloud.requests:
        if request.url and "batchV4" in request.url and request.data:
            body = request.data.decode() if isinstance(request.data, bytes) else request.data
            messages.extend(json.loads(line)["message"] for line in body.splitlines())
    return messages


# ---------------------------------------------------------------------------
# The context variants: routing, the start_scan policies, sourcing.
# ---------------------------------------------------------------------------


def test_get_scan_context_outside_any_bracket_is_an_inert_atomic_context():
    context = get_scan_context()

    assert isinstance(context, AtomicScanContext)
    assert not context.is_batched
    with pytest.raises(AssertionError, match="run_scan"):
        context.insert_results(_payload())


def test_using_scan_context_restores_the_previous_context():
    outer = AtomicScanContext(soda_cloud=MagicMock())
    inner = AtomicScanContext(soda_cloud=MagicMock())

    with using_scan_context(outer):
        with using_scan_context(inner):
            assert get_scan_context() is inner
        assert get_scan_context() is outer


def test_atomic_context_inserts_sync_and_stamps_the_command_type():
    soda_cloud = MagicMock()
    context = AtomicScanContext(soda_cloud)
    payload = {"definitionName": "my_scan"}

    context.insert_results(payload)

    # The context owns the command-type stamp (on a copy), so flows that build a typeless payload
    # (metric monitoring) never branch on the mode; the caller's dict is untouched.
    soda_cloud.insert_scan_results.assert_called_once_with(
        {"definitionName": "my_scan", "type": "sodaCoreInsertScanResults"}
    )
    assert "type" not in payload


def test_atomic_context_start_and_end_are_no_ops():
    soda_cloud = MagicMock()
    context = AtomicScanContext(soda_cloud)
    context.logs = Logs()
    try:
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)

        soda_cloud.scan_start.assert_not_called()
        assert isinstance(context.logs.gatherer, LogsCollector)
        assert context.end_scan() is True
        soda_cloud.scan_end_async.assert_not_called()
    finally:
        context.logs.close()


def test_batched_context_insert_results_batches_when_scan_reference_set():
    soda_cloud = MagicMock()
    context = BatchedScanContext(soda_cloud, scan_id="scan-123")
    context.scan_reference = "org/ref-1"

    context.insert_results(_payload())

    soda_cloud.insert_scan_data_batch.assert_called_once_with(_payload(), "org/ref-1")
    soda_cloud.insert_scan_results.assert_not_called()


def test_batched_context_insert_before_start_fails_loudly():
    # No atomic fallback: a batched run without a scan_reference is a flow bug (inserting before
    # start_scan) — a failed start already failed the run inside start_scan.
    soda_cloud = MagicMock()
    context = BatchedScanContext(soda_cloud, scan_id="scan-123")

    with pytest.raises(AssertionError, match="start_scan"):
        context.insert_results(_payload())

    soda_cloud.insert_scan_results.assert_not_called()
    soda_cloud.insert_scan_data_batch.assert_not_called()


def test_batched_context_records_delivered_and_rejected_uploads():
    soda_cloud = MagicMock()
    context = BatchedScanContext(soda_cloud, scan_id="scan-123")
    context.scan_reference = "org/ref-1"
    soda_cloud.insert_scan_data_batch.side_effect = [True, False]

    assert context.insert_results(_payload()) is True
    assert context.results_delivered and not context.results_rejected

    assert context.insert_results(_payload()) is False
    assert context.results_delivered and context.results_rejected


def test_start_scan_switches_to_streaming_and_replays_captured_records():
    mock_cloud = _ScanLifecycleSodaCloud()
    logs = Logs()
    try:
        soda_logger.info("captured before the scan started")
        context = BatchedScanContext(mock_cloud, scan_id="scan-123")
        context.logs = logs

        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)

        assert context.scan_reference == "org/ref-1"
        assert isinstance(logs.gatherer, LogsQueue)
        assert logs.gatherer.scan_id == "scan-123"
        # From now on the payload log fill sites see no records: the stream is the single log channel.
        assert logs.get_log_records() == []
    finally:
        logs.close()

    start_command = _command_json(mock_cloud, "sodaCoreScanStart")
    assert start_command["scanId"] == "scan-123"
    assert start_command["definitionName"] == "my_scan"
    assert start_command["defaultDataSource"] == "postgres"
    assert start_command["dataTimestamp"] == "2026-07-13T08:30:00+00:00"
    # The pre-start history was replayed into the stream (shipped on the close-time flush).
    assert "captured before the scan started" in _streamed_messages(mock_cloud)


def test_start_scan_is_idempotent():
    mock_cloud = _ScanLifecycleSodaCloud()
    context = BatchedScanContext(mock_cloud, scan_id="scan-123")
    context.logs = Logs()
    try:
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)

        assert _request_kinds(mock_cloud).count("sodaCoreScanStart") == 1
    finally:
        context.logs.close()


def test_start_scan_rejected_fails_the_run():
    mock_cloud = _ScanLifecycleSodaCloud(scan_start_status=400)
    logs = Logs()
    context = BatchedScanContext(mock_cloud, scan_id="scan-123")
    context.logs = logs
    try:
        with pytest.raises(ScanExecutionFailedException, match="did not accept sodaCoreScanStart"):
            context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)

        assert context.scan_reference is None
        # Nothing streamed: the failure report attaches the full in-memory record list.
        assert isinstance(logs.gatherer, LogsCollector)
        assert _request_kinds(mock_cloud) == ["sodaCoreScanStart"]
    finally:
        logs.close()


def test_start_scan_raising_client_call_fails_the_run_the_same_way():
    # A raising scan_start (e.g. an authentication failure escaping the client) is the same
    # fail-the-scan path as a rejection — same clean-message exception, cause preserved.
    soda_cloud = MagicMock()
    original = ConnectionError("network down")
    soda_cloud.scan_start.side_effect = original
    context = BatchedScanContext(soda_cloud, scan_id="scan-123")

    with pytest.raises(ScanExecutionFailedException, match="network down") as excinfo:
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)

    assert excinfo.value.__cause__ is original


# ---------------------------------------------------------------------------
# run_scan: the two channels driven together — a real LogsQueue against the recording Cloud
# double — plus the terminal-state discipline.
# ---------------------------------------------------------------------------


def test_run_scan_without_scan_id_installs_an_atomic_context(monkeypatch):
    # Even for a wiring that opted into batched ingestion: no SODA_SCAN_ID, no batched run.
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    mock_cloud = _ScanLifecycleSodaCloud()
    seen = {}

    def command(logs: Logs) -> ExitCode:
        context = get_scan_context()
        seen["context"], seen["logs"] = context, logs
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        context.insert_results(_payload())
        return ExitCode.OK

    exit_code = run_scan(mock_cloud, command, batched=True)

    assert exit_code == ExitCode.OK
    # The ad-hoc regression: today's single sync command, nothing of the async pipeline.
    assert _request_kinds(mock_cloud) == ["sodaCoreInsertScanResults"]
    assert isinstance(seen["context"], AtomicScanContext)
    assert seen["context"].logs is seen["logs"]
    assert isinstance(seen["logs"].gatherer, LogsCollector)


def test_run_scan_managed_installs_a_batched_context(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    seen = {}

    def command(logs: Logs) -> ExitCode:
        seen["context"] = get_scan_context()
        return ExitCode.OK

    exit_code = run_scan(_ScanLifecycleSodaCloud(), command, batched=True)

    assert exit_code == ExitCode.OK
    assert isinstance(seen["context"], BatchedScanContext)
    assert seen["context"].scan_id == "scan-123"


def test_run_scan_happy_path_command_order(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_cloud = _ScanLifecycleSodaCloud()

    def command(logs: Logs) -> ExitCode:
        context = get_scan_context()
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        soda_logger.info("engine work under way")
        logs.gatherer.flush()  # a mid-run cadence flush, forced for determinism
        assert context.insert_results(_payload()) is True
        soda_logger.info("after the results were sent")
        return ExitCode.OK

    exit_code = run_scan(mock_cloud, command, batched=True)

    assert exit_code == ExitCode.OK
    # The whole contract in one sequence: start gates the stream, batches ride between, the final
    # flush lands before the end command promotes the streamed logs.
    assert _request_kinds(mock_cloud) == [
        "sodaCoreScanStart",
        "logsBatchV4",
        "sodaCoreInsertScanDataBatch",
        "logsBatchV4",
        "sodaCoreScanEndAsync",
    ]
    batch_command = _command_json(mock_cloud, "sodaCoreInsertScanDataBatch")
    assert batch_command["scanReference"] == "org/ref-1"
    assert _command_json(mock_cloud, "sodaCoreScanEndAsync")["scanReference"] == "org/ref-1"
    assert "after the results were sent" in _streamed_messages(mock_cloud)


def test_run_scan_without_batched_opt_in_stays_atomic_even_when_managed(monkeypatch):
    # The contract-verify shape: the wiring declares whether the flow participates in batched
    # ingestion; SODA_SCAN_ID alone must not flip a sync-ingesting flow's context to batched
    # (an engine consumer sourcing the context mid-session would see the wrong ingestion mode).
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_cloud = _ScanLifecycleSodaCloud()
    seen = {}

    def command(logs: Logs) -> ExitCode:
        seen["context"] = get_scan_context()
        return ExitCode.OK

    exit_code = run_scan(mock_cloud, command)

    assert exit_code == ExitCode.OK
    assert isinstance(seen["context"], AtomicScanContext)
    assert not seen["context"].is_batched
    assert _request_kinds(mock_cloud) == []


def test_run_scan_failed_start_marks_the_scan_failed(monkeypatch):
    # A failed start takes the standard failure mapping: no results insert, no scan end — the
    # failure report (with the full in-memory record list) owns the scan's terminal state.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_cloud = _ScanLifecycleSodaCloud(scan_start_status=400)

    def command(logs: Logs) -> ExitCode:
        soda_logger.info("resolution before the start")
        context = get_scan_context()
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        raise AssertionError("unreachable: start_scan must raise")

    exit_code = run_scan(mock_cloud, command, batched=True)

    assert exit_code == ExitCode.LOG_ERRORS
    assert _request_kinds(mock_cloud) == ["sodaCoreScanStart", "sodaCoreMarkScanFailed"]
    reported = _command_json(mock_cloud, "sodaCoreMarkScanFailed")["logs"]
    assert any("resolution before the start" in entry["message"] for entry in reported)
    assert any("did not accept sodaCoreScanStart" in entry["message"] for entry in reported)


def test_run_scan_failure_with_a_healthy_stream_reports_no_logs(monkeypatch):
    # THE failure-path property: sodaCoreMarkScanFailed REPLACES a scan's stored logs, so on a
    # healthy stream the report must go out empty — flush first, attach nothing — leaving the
    # streamed history authoritative in Soda Cloud.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_cloud = _ScanLifecycleSodaCloud()

    def command(logs: Logs) -> ExitCode:
        get_scan_context().start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        soda_logger.info("progress before the crash")
        raise ValueError("boom")

    exit_code = run_scan(mock_cloud, command, batched=True)

    assert exit_code == ExitCode.LOG_ERRORS
    kinds = _request_kinds(mock_cloud)
    # The flush precedes the report — that ordering is what keeps the report empty.
    assert kinds.index("logsBatchV4") < kinds.index("sodaCoreMarkScanFailed")
    assert _command_json(mock_cloud, "sodaCoreMarkScanFailed")["logs"] == []
    # The failure line itself reached the stream instead.
    assert any("boom" in message for message in _streamed_messages(mock_cloud))
    # No results were delivered: the scan is NOT ended — the failure report owns its terminal state.
    assert "sodaCoreScanEndAsync" not in kinds


def test_run_scan_failure_with_a_broken_stream_attaches_the_unsent_errors(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_cloud = _ScanLifecycleSodaCloud(log_upload_status=400)

    def command(logs: Logs) -> ExitCode:
        get_scan_context().start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        raise ValueError("boom")

    exit_code = run_scan(mock_cloud, command, batched=True)

    assert exit_code == ExitCode.LOG_ERRORS
    # The stream could deliver nothing, so the error records ride the report — the one case where
    # attaching them is right, because they reached Cloud through no other channel.
    reported = _command_json(mock_cloud, "sodaCoreMarkScanFailed")["logs"]
    assert reported and all(entry["level"] == "error" for entry in reported)
    assert any("boom" in entry["message"] for entry in reported)
    assert "sodaCoreScanEndAsync" not in _request_kinds(mock_cloud)


def test_run_scan_failure_before_start_reports_the_full_record_list(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_cloud = _ScanLifecycleSodaCloud()

    def command(logs: Logs) -> ExitCode:
        soda_logger.info("resolution progress")
        raise ValueError("resolution failed")

    exit_code = run_scan(mock_cloud, command, batched=True)

    assert exit_code == ExitCode.LOG_ERRORS
    # Nothing was streamed: today's behavior — the full record list rides the report.
    reported = _command_json(mock_cloud, "sodaCoreMarkScanFailed")["logs"]
    assert any("resolution progress" in entry["message"] for entry in reported)
    assert _request_kinds(mock_cloud) == ["sodaCoreMarkScanFailed"]


def test_run_scan_failure_after_a_delivered_insert_does_not_also_end_the_scan(monkeypatch):
    # One run, one terminal transition: the failure report already marked the scan FAILED, and
    # sodaCoreScanEndAsync on top of it would ingest the batch into a failed scan.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_cloud = _ScanLifecycleSodaCloud()

    def command(logs: Logs) -> ExitCode:
        context = get_scan_context()
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        assert context.insert_results(_payload()) is True
        raise ValueError("post-processing exploded")

    exit_code = run_scan(mock_cloud, command, batched=True)

    assert exit_code == ExitCode.LOG_ERRORS
    kinds = _request_kinds(mock_cloud)
    assert "sodaCoreMarkScanFailed" in kinds
    assert "sodaCoreScanEndAsync" not in kinds


def test_run_scan_cancellation_neither_ends_nor_marks_the_scan(monkeypatch):
    # SIGTERM / pod eviction: the run's terminal state belongs to the launcher fallback. The logs
    # are still flushed on the way out.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_cloud = _ScanLifecycleSodaCloud()
    seen = {}

    def command(logs: Logs) -> ExitCode:
        seen["logs"] = logs
        context = get_scan_context()
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        assert context.insert_results(_payload()) is True
        soda_logger.info("interrupted mid-run")
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_scan(mock_cloud, command, batched=True)

    kinds = _request_kinds(mock_cloud)
    assert "sodaCoreScanEndAsync" not in kinds
    assert "sodaCoreMarkScanFailed" not in kinds
    assert "interrupted mid-run" in _streamed_messages(mock_cloud)
    # The finally-close released the stream: no leaked worker thread.
    assert not seen["logs"].gatherer.worker_thread.is_alive()


def test_run_scan_rejected_results_leave_the_scan_unended(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_cloud = _ScanLifecycleSodaCloud(insert_status=400)

    def command(logs: Logs) -> ExitCode:
        context = get_scan_context()
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        return ExitCode.OK if context.insert_results(_payload()) else ExitCode.RESULTS_NOT_SENT_TO_CLOUD

    exit_code = run_scan(mock_cloud, command, batched=True)

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    # Undelivered results: ending the scan would close it "cleanly" with nothing in it — the
    # exit-code fallback (launcher) owns the terminal state instead.
    assert "sodaCoreScanEndAsync" not in _request_kinds(mock_cloud)


def test_run_scan_partly_rejected_session_leaves_the_scan_unended(monkeypatch):
    # results_delivered latches on the first accepted upload; the rejected flag must still veto the
    # end, or a multi-collection session would close "cleanly" with a collection missing.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_cloud = _ScanLifecycleSodaCloud()

    def command(logs: Logs) -> ExitCode:
        context = get_scan_context()
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        assert context.insert_results(_payload()) is True
        mock_cloud.insert_status = 400
        assert context.insert_results(_payload()) is False
        return ExitCode.RESULTS_NOT_SENT_TO_CLOUD

    exit_code = run_scan(mock_cloud, command, batched=True)

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    assert "sodaCoreScanEndAsync" not in _request_kinds(mock_cloud)


def test_run_scan_failure_report_supersedes_the_stream(monkeypatch, caplog):
    # Once sodaCoreMarkScanFailed lands, the backend rejects every further batchV4 upload for the
    # scan. The report's own confirmation line (and anything logged after it) must therefore stay
    # console-only — streaming it would end the run with a false "records could not be delivered"
    # alarm immediately after the failure was reported successfully.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_cloud = _ScanLifecycleSodaCloud()
    original_handle = mock_cloud._http_handle

    def terminal_aware(method, url, headers, json, data):
        response = original_handle(method, url, headers, json, data)
        if isinstance(json, dict) and json.get("type") == "sodaCoreMarkScanFailed":
            # The real backend: the mark moves the scan out of its log-accepting state.
            mock_cloud.log_upload_status = 400
        return response

    mock_cloud._http_handle = terminal_aware

    def command(logs: Logs) -> ExitCode:
        get_scan_context().start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        raise ValueError("boom")

    with caplog.at_level(logging.INFO):
        exit_code = run_scan(mock_cloud, command, batched=True)

    assert exit_code == ExitCode.LOG_ERRORS
    kinds = _request_kinds(mock_cloud)
    # Nothing is uploaded after the terminal command — the report retired the stream.
    assert "logsBatchV4" not in kinds[kinds.index("sodaCoreMarkScanFailed") + 1 :]
    assert "could not be delivered" not in caplog.text


def test_run_scan_rejected_end_is_fatal_and_not_retried(monkeypatch):
    # Batch uploads sit inert in object storage until the end command triggers reassembly, and no
    # backend sweeper does it later: a lost end loses the whole run's results, so it cannot be a
    # warning-and-exit-0. Exactly one attempt: Cloud-command retries are a global concern tracked
    # separately, not a per-command loop here.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_cloud = _ScanLifecycleSodaCloud(end_status=500)

    def command(logs: Logs) -> ExitCode:
        context = get_scan_context()
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        assert context.insert_results(_payload()) is True
        return ExitCode.OK

    exit_code = run_scan(mock_cloud, command, batched=True)

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    assert _request_kinds(mock_cloud).count("sodaCoreScanEndAsync") == 1


def test_run_scan_raising_end_never_escapes(monkeypatch):
    # This runs after the CLI's failure boundary has closed: an escaping raise would exit 1, which
    # the launcher reads as "checks failed" rather than "results never ingested".
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = "org/ref-1"
    soda_cloud.insert_scan_data_batch.return_value = True
    soda_cloud.scan_end_async.side_effect = ConnectionError("network down")

    def command(logs: Logs) -> ExitCode:
        context = get_scan_context()
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        assert context.insert_results(_payload()) is True
        return ExitCode.OK

    exit_code = run_scan(soda_cloud, command, batched=True)

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    assert soda_cloud.scan_end_async.call_count == 1
