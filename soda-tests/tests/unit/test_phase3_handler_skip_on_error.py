"""Phase 3 of execute_check_collections: post-processing handler skip on ERROR.

When ``impl.verify()`` raises in phase 2 the executor builds an ERROR
placeholder via ``build_error_result``. In the non-combine path, handlers
live INSIDE ``verify()`` so an exception bypasses them entirely. The
combine-upload path's phase-3 handler loop runs OUTSIDE verify(), so it
must mirror the non-combine semantics by skipping results whose
``error is not None``.

This regression test locks that behaviour: when one of two files raises,
``run_post_processing_handlers`` is called for the surviving file ONLY,
not for the ERROR-placeholder file.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from soda_core.check_collections.base import (
    CheckCollectionImpl,
    CheckCollectionResult,
    CheckCollectionYaml,
)
from soda_core.check_collections.session import execute_check_collections
from soda_core.common.logs import Logs
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    Contract,
    YamlFileContentInfo,
)

_HANDLER_SKIP_KIND = "phase3-handler-skip-stub"


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
    """Combine-upload stub that records every call to
    ``run_post_processing_handlers``.

    ``verify()`` raises iff the yaml source label is ``"broken"`` so we
    can force an ERROR placeholder for one file in the session.
    """

    kind = _HANDLER_SKIP_KIND
    wire_source = "stub-handler-wire"
    yaml_class = _StubYaml
    result_class = _StubResult
    combine_uploads = True
    requires_collection_id = False

    handler_calls: list[str] = []

    def __init__(self, yaml, **kwargs):
        # Skip the heavy base ``__init__``; the executor only needs the
        # yaml + a reachable run_post_processing_handlers method.
        self.yaml = yaml
        self.logs = Logs()

    @property
    def collection_id(self) -> Optional[str]:
        return None

    def verify(self) -> _StubResult:
        label = getattr(self.yaml.yaml_source, "_label", "anon")
        if label == "broken":
            raise RuntimeError("yaml unhappy")
        return _make_passed_result(label=label)

    def run_post_processing_handlers(self, result, response_json) -> None:
        # Tag by the result's dataset_name so the test can assert which
        # files got handlers (and which didn't).
        label = result.check_collection.dataset_name if result.check_collection else "<no-dataset>"
        type(self).handler_calls.append(label)


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


def test_phase3_handlers_skipped_for_error_placeholder():
    """One yaml's verify() raises → phase-3 handlers run for the surviving
    file ONLY. The ERROR placeholder must NOT invoke handlers, matching
    the non-combine path where an exception in verify() bypasses
    handler invocation."""
    _StubImpl.handler_calls = []

    sources = [_StubSource("alpha"), _StubSource("broken"), _StubSource("gamma")]
    session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)

    # Sanity: three results, one of which is the ERROR placeholder.
    assert len(session_result.results) == 3
    labels_by_status = {
        r.status: r.check_collection.dataset_name if r.check_collection else None for r in session_result.results
    }
    assert CheckCollectionStatus.ERROR in labels_by_status

    # Handlers ran only for the two PASSED files.
    assert _StubImpl.handler_calls == ["alpha", "gamma"]
