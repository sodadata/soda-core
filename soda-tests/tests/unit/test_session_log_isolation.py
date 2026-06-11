"""Per-collection log isolation in the check-collections executor.

When N collections run in one ``execute_check_collections`` session on one
thread, each collection's log records must land in — and be labelled for — only
that collection, with no duplicates in the combined upload payload.

Capture is routed through a single root-logger handler to the *active* ``Logs``
(a contextvar), and a ``Logs`` becomes active the moment it is constructed, so
each impl's construction logs are captured by its own gatherer; the executor
re-activates each impl's ``Logs`` around its verify(), and the default per-item
``handle_session`` does the same around post-processing handlers. Each record's
``thread`` is stamped at emit time (via the active
Logs' ``label``) with the per-collection value ``{wire_source}.{collection_id}``,
so there is no after-the-fact relabel and only one gatherer is ever active
(siblings cannot cross-capture).

The CLI console output is unaffected — it prints via a separate
``SodaConsoleHandler`` at emit time, not from these per-result buffers.

These tests drive the executor with a sentinel ``_LoggingImpl`` that mirrors the
real construct/verify log lifecycle without needing a data source.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import pytest
from soda_core.check_collections.base import (
    CheckCollectionImpl,
    CheckCollectionResult,
    CheckCollectionYaml,
)
from soda_core.check_collections.session import execute_check_collections
from soda_core.common import logs as logs_module
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import _build_check_collection_results_json_dict
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    Contract,
    YamlFileContentInfo,
)
from soda_core.contracts.impl.contract_verification_impl import (
    ContractVerificationHandlerRegistry,
)

_LOGGING_KIND = "logging-isolation-test"
_RAISING_KIND = "logging-isolation-raise-test"
_WIRE_SOURCE = "logging-collection"


class _FakeYamlObject:
    """Minimal YamlObject stand-in — the executor reads ``kind`` off it."""

    def __init__(self, kind: Optional[str]):
        self._kind = kind

    def read_string_opt(self, key: str, env_var=None, default_value=None):
        return self._kind if key == "kind" else default_value


class _LabelledSource:
    """``CheckCollectionYamlSource`` stand-in carrying a label + kind."""

    def __init__(self, label: str, kind: str = _LOGGING_KIND):
        self._label = label
        self._kind = kind
        self.file_path = f"{label}.yml"
        self.yaml_str_original = f"# {label}"

    def parse(self):
        return _FakeYamlObject(kind=self._kind)


class _FakeYaml(CheckCollectionYaml):
    """Inherits the base __init__/parse so ``data_timestamp`` /
    ``execution_timestamp`` are populated for the executor's post-parse reads."""


class _FakeResult(CheckCollectionResult):
    """Sentinel result subclass — same shape as ``CheckCollectionResult``."""


def _make_result(label: str, log_records: list) -> _FakeResult:
    now = datetime.now(tz=timezone.utc)
    return _FakeResult(
        check_collection=Contract(
            data_source_name="fake_ds",
            dataset_prefix=[],
            dataset_name=label,
            soda_qualified_dataset_name=f"fake_ds/{label}",
            source=YamlFileContentInfo(
                source_content_str=f"# {label}",
                local_file_path=f"/fake/{label}.yml",
                soda_cloud_file_id=f"file-{label}",
            ),
        ),
        data_source=None,
        data_timestamp=now,
        started_timestamp=now,
        ended_timestamp=now,
        status=CheckCollectionStatus.PASSED,
        measurements=[],
        check_results=[],
        sending_results_to_soda_cloud_failed=False,
        log_records=log_records,
        post_processing_stages=[],
    )


class _LoggingImpl(CheckCollectionImpl):
    """Sentinel combine-upload subtype that exercises the log lifecycle.

    Mirrors the parts of ``CheckCollectionImpl.__init__`` / ``verify()`` that
    matter for capture, without a data source: it owns its ``Logs`` (when
    ``logs=None``), labels that Logs (so records are stamped at emit), emits a
    construction line, and on verify() emits a line and pops the records.
    """

    kind = _LOGGING_KIND
    wire_source = _WIRE_SOURCE
    display_name = "logging-collection"
    yaml_class = _FakeYaml
    result_class = _FakeResult
    combine_uploads = True

    def __init__(self, yaml, logs=None, **kwargs):
        # Own a Logs per impl exactly like CheckCollectionImpl.__init__ does
        # when the caller passes logs=None (the combine-upload path); a freshly
        # constructed Logs is the active capture target.
        self.logs = logs if logs is not None else Logs()
        self.yaml = yaml
        self._label = yaml.yaml_source._label
        # Label the capture target so this impl's records are stamped at emit
        # time (mirrors base.__init__'s ``self.logs.label = self.thread_label``).
        self.logs.label = self.thread_label
        soda_logger.info(f"construct-{self._label}")

    @property
    def collection_id(self) -> Optional[str]:
        return self._label

    def verify(self) -> _FakeResult:
        soda_logger.info(f"verify-{self._label}")
        return _make_result(self._label, self.logs.get_log_records())


class _RaisingConstructImpl(CheckCollectionImpl):
    """Combine-upload subtype whose __init__ creates its own ``Logs`` and then
    raises — modelling a per-file construction failure (bad YAML, unparseable
    dataset, ...). Must not leave the root logger in a degraded state."""

    kind = _RAISING_KIND
    wire_source = _WIRE_SOURCE
    display_name = "logging-collection-raising"
    yaml_class = _FakeYaml
    result_class = _FakeResult
    combine_uploads = True

    def __init__(self, yaml, logs=None, **kwargs):
        self.logs = logs if logs is not None else Logs()
        raise RuntimeError("construction blew up")


@pytest.fixture()
def _isolate_logging():
    """Isolate this module's logging side effects from the rest of the suite.

    - Force ``soda_logger`` to DEBUG so the INFO lines the fakes emit are
      captured (without depending on whatever level prior tests left).
    - Clear the post-processing handler registry for the duration of the test
      so phase 3 can't invoke a registered handler against our minimal fakes
      (which don't model ``data_source_impl`` etc.); restore it afterwards.
    - Reset the active-capture contextvar before and after so a Logs built here
      (or by a prior test) doesn't leak as the active target. The single root
      capturer is process-permanent and intentionally left installed.
    """
    prev_level = soda_logger.level
    soda_logger.setLevel(logging.DEBUG)
    registry_before = list(ContractVerificationHandlerRegistry.contract_verification_handlers)
    ContractVerificationHandlerRegistry.contract_verification_handlers[:] = []
    logs_module._active_logs.set(None)
    try:
        yield
    finally:
        soda_logger.setLevel(prev_level)
        ContractVerificationHandlerRegistry.contract_verification_handlers[:] = registry_before
        logs_module._active_logs.set(None)


def _run_three_collections() -> list[_FakeResult]:
    sources = [_LabelledSource("a"), _LabelledSource("b"), _LabelledSource("c")]
    session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)
    return session_result.results


def _messages(result: _FakeResult) -> list[str]:
    return [r.getMessage() for r in (result.log_records or [])]


def test_each_collection_result_only_contains_its_own_log_lines(_isolate_logging):
    """A collection's result must carry only the log lines it emitted —
    never lines from sibling collections in the same session."""
    results = _run_three_collections()

    by_label = {r.check_collection.dataset_name: _messages(r) for r in results}
    assert by_label["a"] == ["construct-a", "verify-a"]
    assert by_label["b"] == ["construct-b", "verify-b"]
    assert by_label["c"] == ["construct-c", "verify-c"]


def test_no_log_record_is_shared_across_results(_isolate_logging):
    """The same ``LogRecord`` object must not appear in more than one result —
    shared objects are what duplicate into the combined Cloud payload."""
    results = _run_three_collections()

    seen_ids: set[int] = set()
    duplicated: list[str] = []
    for result in results:
        for record in result.log_records or []:
            if id(record) in seen_ids:
                duplicated.append(record.getMessage())
            seen_ids.add(id(record))

    assert duplicated == []


def test_thread_matches_the_emitting_collection(_isolate_logging):
    """Every record of a collection shares one ``thread`` value, stamped at emit
    time, that names the collection (``{wire_source}.{collection_id}``)."""
    results = _run_three_collections()

    for result in results:
        label = result.check_collection.dataset_name
        threads = {record.thread for record in (result.log_records or [])}
        assert threads == {f"{_WIRE_SOURCE}.{label}"}, f"collection {label!r} carried {threads}"


def test_combined_payload_has_no_duplicate_log_entries(_isolate_logging):
    """The combined ``sodaCoreInsertScanResults`` payload must list each emitted
    log line exactly once, each stamped with its collection's thread."""
    results = _run_three_collections()

    payload = _build_check_collection_results_json_dict(results, wire_source=_WIRE_SOURCE)
    entries = [(log["message"], log["thread"]) for log in payload["logs"]]

    assert sorted(entries) == [
        ("construct-a", f"{_WIRE_SOURCE}.a"),
        ("construct-b", f"{_WIRE_SOURCE}.b"),
        ("construct-c", f"{_WIRE_SOURCE}.c"),
        ("verify-a", f"{_WIRE_SOURCE}.a"),
        ("verify-b", f"{_WIRE_SOURCE}.b"),
        ("verify-c", f"{_WIRE_SOURCE}.c"),
    ]


def test_constructing_logs_auto_captures_without_an_explicit_scope(_isolate_logging):
    """Capture is a side effect of constructing a ``Logs`` — there is no opt-in
    scope a caller can forget. This guards against regressing to an opt-in model
    where a missing scope would silently drop records."""
    logs = Logs()
    soda_logger.info("ping-unscoped")
    assert "ping-unscoped" in [r.getMessage() for r in logs.get_log_records()]


def test_failed_construction_leaves_capture_intact_for_siblings(_isolate_logging):
    """A construction that raises must not break capture for the other
    collections or accumulate handlers on the root logger (there is one
    process-permanent root capturer; a failed construct adds none)."""
    # Install the process-wide capturer before sampling so the count is not
    # order-dependent (running this test alone would otherwise install it
    # mid-run and legitimately grow the count by one).
    logs_module._ensure_root_capturer()
    handler_count_before = len(logging.root.handlers)

    sources = [
        _LabelledSource("ok-1"),
        _LabelledSource("bad", kind=_RAISING_KIND),
        _LabelledSource("ok-2"),
    ]
    session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)

    # The failed file still yields an ERROR placeholder; siblings still run and
    # capture their own lines, correctly labelled.
    assert [r.status for r in session_result.results] == [
        CheckCollectionStatus.PASSED,
        CheckCollectionStatus.ERROR,
        CheckCollectionStatus.PASSED,
    ]
    assert _messages(session_result.results[0]) == ["construct-ok-1", "verify-ok-1"]
    assert _messages(session_result.results[2]) == ["construct-ok-2", "verify-ok-2"]
    assert {r.thread for r in session_result.results[2].log_records} == {f"{_WIRE_SOURCE}.ok-2"}
    # No per-construction handler accumulation on the root logger.
    assert len(logging.root.handlers) == handler_count_before
