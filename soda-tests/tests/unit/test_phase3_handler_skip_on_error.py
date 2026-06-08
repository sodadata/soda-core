"""Phase 3 of execute_check_collections: session-level post-processing dispatch.

Combine-upload subtypes are post-processed AFTER the single combined upload, in
phase 3 of ``execute_check_collections`` (outside ``verify()``). Handlers run
ONCE per wire-source group via ``ContractVerificationHandler.handle_session``
(not once per file), so a session-scoped handler can fetch config once per
dataset, reuse one connection, and post a single aggregated stage update.

These tests lock the dispatch contract:
- ERROR placeholders (a file whose ``verify()`` raised) are excluded from the
  session items, mirroring the non-combine path where an exception in verify()
  bypasses handler invocation.
- ``handle_session`` is called ONCE per wire source with all surviving files.
- The default ``handle_session`` falls back to per-file ``handle`` calls.
- A handler raising in ``handle_session`` is isolated: other handlers still run
  and the session completes normally.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from soda_core.check_collections.base import (
    CheckCollectionImpl,
    CheckCollectionResult,
    CheckCollectionYaml,
)
from soda_core.check_collections.session import execute_check_collections
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.soda_cloud import SodaCloud
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    Contract,
    ContractVerificationResult,
    PostProcessingStage,
    YamlFileContentInfo,
)
from soda_core.contracts.impl.contract_verification_impl import (
    ContractImpl,
    ContractVerificationHandler,
    ContractVerificationHandlerRegistry,
    PostProcessingSessionItem,
)
from soda_core.contracts.impl.diagnostics_warehouse_files import (
    DiagnosticsWarehouseFiles,
)

_HANDLER_SKIP_KIND = "phase3-handler-skip-stub"


@contextmanager
def _registered(*handlers: ContractVerificationHandler):
    """Register handlers for the duration of the block, then restore the
    global registry — so handler registration never leaks across tests."""
    reg = ContractVerificationHandlerRegistry
    saved_handlers = list(reg.contract_verification_handlers)
    saved_stages = dict(reg.post_processing_stages)
    try:
        for handler in handlers:
            reg.register(handler)
        yield
    finally:
        reg.contract_verification_handlers = saved_handlers
        reg.post_processing_stages = saved_stages


class _StubYaml(CheckCollectionYaml):
    @classmethod
    def parse(cls, yaml_source, **kwargs):
        return cls(yaml_source=yaml_source, yaml_object=kwargs.get("yaml_object"))


class _StubResult(CheckCollectionResult):
    pass


def _make_passed_result(label: str) -> _StubResult:
    now = datetime.now(tz=timezone.utc)
    return _StubResult(
        check_collection=Contract(
            data_source_name="fake_ds",
            dataset_prefix=[],
            dataset_name=label,
            soda_qualified_dataset_name=f"fake_ds/{label}",
            source=YamlFileContentInfo(source_content_str=None, local_file_path=None),
        ),
        data_source=None,
        data_timestamp=None,
        started_timestamp=now,
        ended_timestamp=now,
        status=CheckCollectionStatus.PASSED,
        measurements=[],
        check_results=[],
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
        post_processing_stages=[],
    )


class _StubImpl(CheckCollectionImpl):
    """Combine-upload stub. ``verify()`` raises iff the yaml source label is
    ``"broken"`` so we can force an ERROR placeholder for one file."""

    kind = _HANDLER_SKIP_KIND
    wire_source = "stub-handler-wire"
    yaml_class = _StubYaml
    result_class = _StubResult
    combine_uploads = True
    requires_collection_id = False

    def __init__(self, yaml, **kwargs):
        # Skip the heavy base ``__init__``; the executor only needs the yaml,
        # a ``data_source_impl`` attribute (read when building session items),
        # and a reachable ``_apply_thread_label_to_log_records``.
        self.yaml = yaml
        self.data_source_impl = None

    @property
    def collection_id(self) -> Optional[str]:
        return None

    def verify(self) -> _StubResult:
        label = getattr(self.yaml.yaml_source, "_label", "anon")
        if label == "broken":
            raise RuntimeError("yaml unhappy")
        return _make_passed_result(label=label)


class _RecordingHandler(ContractVerificationHandler):
    """Overrides ``handle_session`` to record, per call, the file labels it
    received and the shared group response_json."""

    def __init__(self) -> None:
        self.session_calls: list[tuple[list[str], Optional[dict]]] = []

    def handle(self, *args, **kwargs):  # required abstract; unused (we override handle_session)
        raise AssertionError("handle() should not be called when handle_session is overridden")

    def provides_post_processing_stages(self) -> list[PostProcessingStage]:
        return []

    def handle_session(
        self,
        items: list[PostProcessingSessionItem],
        soda_cloud: SodaCloud,
        soda_cloud_send_results_response_json: Optional[dict] = None,
        dwh_files: Optional[DiagnosticsWarehouseFiles] = None,
    ) -> None:
        labels = [
            item.verification_result.check_collection.dataset_name
            if item.verification_result.check_collection
            else None
            for item in items
        ]
        self.session_calls.append((labels, soda_cloud_send_results_response_json))


class _DefaultPathHandler(ContractVerificationHandler):
    """Does NOT override ``handle_session`` — exercises the default per-file
    fallback by recording each ``handle`` call."""

    def __init__(self) -> None:
        self.handle_labels: list[Optional[str]] = []

    def handle(
        self,
        contract_impl: ContractImpl,
        data_source_impl: Optional[DataSourceImpl],
        contract_verification_result: ContractVerificationResult,
        soda_cloud: SodaCloud,
        soda_cloud_send_results_response_json: dict,
        dwh_files: Optional[DiagnosticsWarehouseFiles] = None,
    ):
        cc = contract_verification_result.check_collection
        self.handle_labels.append(cc.dataset_name if cc else None)

    def provides_post_processing_stages(self) -> list[PostProcessingStage]:
        return []


class _ExplodingHandler(ContractVerificationHandler):
    """Raises inside ``handle_session`` to verify session-level isolation."""

    def handle(self, *args, **kwargs):
        raise AssertionError("unused")

    def provides_post_processing_stages(self) -> list[PostProcessingStage]:
        return []

    def handle_session(self, *args, **kwargs) -> None:
        raise RuntimeError("handler boom")


class _StubYamlObject:
    def __init__(self, kind: Optional[str]):
        self._kind = kind

    def read_string_opt(self, key: str, env_var: Optional[str] = None, default_value: Optional[str] = None):
        if key == "kind":
            return self._kind
        return default_value


class _StubSource:
    def __init__(self, label: str):
        self._label = label
        self.file_path = f"/fake/{label}.yml"
        self.yaml_str_original = f"# {label}"

    def parse(self):
        return _StubYamlObject(kind=_HANDLER_SKIP_KIND)


def test_handle_session_called_once_with_surviving_files_only():
    """One yaml's verify() raises → ``handle_session`` is invoked ONCE for the
    wire source, with the two PASSED files only (ERROR placeholder excluded)."""
    handler = _RecordingHandler()
    sources = [_StubSource("alpha"), _StubSource("broken"), _StubSource("gamma")]

    with _registered(handler):
        session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)

    # Sanity: three results, one of which is the ERROR placeholder.
    assert len(session_result.results) == 3
    assert any(r.status == CheckCollectionStatus.ERROR for r in session_result.results)

    # handle_session called exactly once (per wire source), with alpha+gamma only.
    assert len(handler.session_calls) == 1
    labels, group_response = handler.session_calls[0]
    assert labels == ["alpha", "gamma"]
    assert group_response is None  # no cloud / no publish in this test


def test_default_handle_session_falls_back_to_per_file_handle():
    """A handler that does NOT override handle_session gets ``handle`` called
    once per surviving file via the default fallback."""
    handler = _DefaultPathHandler()
    sources = [_StubSource("alpha"), _StubSource("broken"), _StubSource("gamma")]

    with _registered(handler):
        execute_check_collections(yaml_sources=sources, data_source_impl=None)

    assert handler.handle_labels == ["alpha", "gamma"]


def test_handler_exception_in_handle_session_is_isolated():
    """A handler raising in handle_session must not break sibling handlers nor
    the session — execute_check_collections still returns all results."""
    exploding = _ExplodingHandler()
    recording = _RecordingHandler()
    sources = [_StubSource("alpha"), _StubSource("gamma")]

    with _registered(exploding, recording):
        session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)

    # Session completed and the surviving handler still ran with both files.
    assert len(session_result.results) == 2
    assert len(recording.session_calls) == 1
    assert recording.session_calls[0][0] == ["alpha", "gamma"]


def test_default_handle_session_isolates_per_item_handle_failure():
    """The default handle_session must isolate a per-item ``handle`` failure: a raise on one
    file does not stop ``handle`` running for the others, and the session still completes."""

    class _FailingDefaultHandler(ContractVerificationHandler):
        def __init__(self) -> None:
            self.labels: list[Optional[str]] = []

        def handle(
            self,
            contract_impl,
            data_source_impl,
            contract_verification_result,
            soda_cloud,
            soda_cloud_send_results_response_json,
            dwh_files=None,
        ):
            cc = contract_verification_result.check_collection
            label = cc.dataset_name if cc else None
            self.labels.append(label)
            if label == "boom_in_handle":
                raise RuntimeError("handle exploded")

        def provides_post_processing_stages(self) -> list[PostProcessingStage]:
            return []

    handler = _FailingDefaultHandler()
    # "boom_in_handle" verifies fine (verify() only raises on label "broken"); its handle() raises.
    sources = [_StubSource("alpha"), _StubSource("boom_in_handle"), _StubSource("gamma")]

    with _registered(handler):
        session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)

    # handle() ran for all three surviving files; the raise on the middle one was isolated.
    assert handler.labels == ["alpha", "boom_in_handle", "gamma"]
    assert len(session_result.results) == 3


def test_session_relabels_thread_labels_once_per_surviving_file(monkeypatch):
    """The executor re-stamps per-file thread labels after session handlers run — once per
    surviving (non-ERROR) file, mirroring run_post_processing_handlers' final relabel pass."""
    relabelled: list[str] = []
    original = _StubImpl._apply_thread_label_to_log_records

    def counting(self, log_records):
        relabelled.append(getattr(self.yaml.yaml_source, "_label", "anon"))
        return original(self, log_records)

    monkeypatch.setattr(_StubImpl, "_apply_thread_label_to_log_records", counting)

    handler = _RecordingHandler()
    sources = [_StubSource("alpha"), _StubSource("broken"), _StubSource("gamma")]
    with _registered(handler):
        execute_check_collections(yaml_sources=sources, data_source_impl=None)

    assert relabelled == ["alpha", "gamma"]  # once per surviving file, never the ERROR placeholder
