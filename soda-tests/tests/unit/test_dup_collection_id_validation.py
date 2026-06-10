"""Phase 1.5 of execute_check_collections: duplicate collection_id detection.

Two yamls whose constructed impls share ``(wire_source, collection_id)``
inside a ``combine_uploads = True`` subtype must abort the session before
any ``verify()`` runs. The error type is ``InvalidArgumentException`` so
CLI handlers (e.g. ``handle_verify_data_standards``) surface it as a clean
user-facing message via ExitCode.LOG_ERRORS without a stack trace.

Tests use sentinel impl/yaml classes that bypass the heavy engine — they
exercise the executor's structural invariants without spinning up a real
data source or running queries.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest
from soda_core.check_collections.base import (
    CheckCollectionImpl,
    CheckCollectionResult,
    CheckCollectionYaml,
)
from soda_core.check_collections.session import execute_check_collections
from soda_core.common.exceptions import InvalidArgumentException
from soda_core.common.logs import Logs
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    Contract,
    YamlFileContentInfo,
)

# Unique kinds per stub so registrations don't collide with other test
# modules' sentinel impls or with the real subtypes.
_DUP_KIND = "dup-check-collection-id-stub"
_DUP_OTHER_WIRE_KIND = "dup-other-wire-source-stub"
_NO_COMBINE_KIND = "dup-no-combine-uploads-stub"


class _StubYaml(CheckCollectionYaml):
    """Yaml stub that records its source for verify()-time inspection."""

    @classmethod
    def parse(cls, yaml_source, **kwargs):
        # Delegate to base init so first-class fields (``yaml_object``,
        # ``kind``, ``data_timestamp``, ``execution_timestamp``) get
        # populated — the executor reads them post-parse.
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
    """Combine-uploads stub. ``collection_id`` is read from the yaml source label."""

    kind = _DUP_KIND
    wire_source = "stub-wire"
    yaml_class = _StubYaml
    result_class = _StubResult
    combine_uploads = True
    requires_collection_id = False  # bypass the verify()-time guard; tests control collection_id directly

    def __init__(self, yaml, **kwargs):
        # Skip the heavy base ``__init__``; tests control state directly.
        self.yaml = yaml
        self.logs = Logs()
        self._collection_id = getattr(yaml.yaml_source, "collection_id_override", None)
        # Track verify() invocations so tests can assert it didn't run.
        type(self).verify_call_count = getattr(type(self), "verify_call_count", 0)

    @property
    def collection_id(self) -> Optional[str]:
        return self._collection_id

    def verify(self) -> _StubResult:
        type(self).verify_call_count += 1
        return _make_passed_result(label=getattr(self.yaml.yaml_source, "_label", "anon"))


class _StubYamlObject:
    """Minimal YamlObject stand-in carrying the test-supplied kind."""

    def __init__(self, kind: Optional[str]):
        self._kind = kind

    def read_string_opt(self, key: str, env_var: Optional[str] = None, default_value: Optional[str] = None):
        if key == "kind":
            return self._kind
        return default_value


class _StubSource:
    """Stand-in for ``CheckCollectionYamlSource``."""

    def __init__(self, label: str, collection_id_override: Optional[str], kind: str = _DUP_KIND):
        self._label = label
        self.collection_id_override = collection_id_override
        self._kind = kind
        self.file_path = f"/fake/{label}.yml"
        self.yaml_str_original = f"# {label}"

    def parse(self):
        return _StubYamlObject(kind=self._kind)

    @classmethod
    def from_str(cls, *_args, **_kwargs):
        raise NotImplementedError


@pytest.fixture(autouse=True)
def _reset_verify_counter():
    _StubImpl.verify_call_count = 0
    yield


def test_two_impls_same_collection_id_raises_invalid_argument():
    """Phase 1.5 raises on a single duplicate pair; phase 2 never runs."""
    sources = [
        _StubSource(label="a", collection_id_override="pii_std"),
        _StubSource(label="b", collection_id_override="pii_std"),
    ]

    with pytest.raises(InvalidArgumentException) as exc_info:
        execute_check_collections(yaml_sources=sources, data_source_impl=None)

    # Both file paths appear in the error message.
    msg = str(exc_info.value)
    assert "stub-wire 'pii_std'" in msg
    assert "/fake/a.yml" in msg
    assert "/fake/b.yml" in msg

    # Phase 2 (verify) never ran for either file.
    assert _StubImpl.verify_call_count == 0


def test_error_message_lists_all_dup_groups():
    """Multiple dup groups in one session → one error listing every group."""
    sources = [
        _StubSource(label="a1", collection_id_override="std_a"),
        _StubSource(label="a2", collection_id_override="std_a"),
        _StubSource(label="a3", collection_id_override="std_a"),
        _StubSource(label="b1", collection_id_override="std_b"),
        _StubSource(label="b2", collection_id_override="std_b"),
        _StubSource(label="unique", collection_id_override="std_unique"),
    ]

    with pytest.raises(InvalidArgumentException) as exc_info:
        execute_check_collections(yaml_sources=sources, data_source_impl=None)

    msg = str(exc_info.value)
    # Both dup groups appear in the message.
    assert "'std_a':" in msg
    assert "'std_b':" in msg
    # All three a-file paths appear.
    assert "/fake/a1.yml" in msg
    assert "/fake/a2.yml" in msg
    assert "/fake/a3.yml" in msg
    # Both b-file paths appear.
    assert "/fake/b1.yml" in msg
    assert "/fake/b2.yml" in msg
    # The unique file is NOT mentioned.
    assert "/fake/unique.yml" not in msg


# ---- Stub subtypes that exercise other branches of the dup check. ----


class _StubResultOtherWire(_StubResult):
    pass


class _StubImplOtherWire(_StubImpl):
    """Stub subtype with a different wire_source — same collection_id is OK across subtypes."""

    kind = _DUP_OTHER_WIRE_KIND
    wire_source = "other-stub-wire"
    result_class = _StubResultOtherWire


class _StubResultNoCombine(_StubResult):
    pass


class _StubImplNoCombine(_StubImpl):
    """Stub subtype that opts OUT of combined uploads."""

    kind = _NO_COMBINE_KIND
    wire_source = "no-combine-wire"
    combine_uploads = False
    result_class = _StubResultNoCombine


def test_contracts_not_subject_to_dup_check():
    """Real contract path: combine_uploads is False, no dup check applies."""
    # Two contract yamls with identical content would still produce
    # collection_id=None at the impl level. Phase 1.5 skips them. We
    # piggyback on _StubImplNoCombine here as a stand-in for any
    # combine_uploads=False subtype — the test asserts the executor
    # doesn't raise even with identical "collection ids" on a subtype
    # that opted out of combining.
    sources = [
        _StubSource(label="c1", collection_id_override="same", kind=_NO_COMBINE_KIND),
        _StubSource(label="c2", collection_id_override="same", kind=_NO_COMBINE_KIND),
    ]
    session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)

    # Both verified successfully — no error raised.
    assert len(session_result.results) == 2
    assert _StubImplNoCombine.verify_call_count == 2


def test_missing_collection_id_skipped_in_dup_check():
    """impl.collection_id is None → file excluded from dedup."""
    sources = [
        _StubSource(label="no_id_1", collection_id_override=None),
        _StubSource(label="no_id_2", collection_id_override=None),
    ]
    session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)

    # No dup error: both files have ``collection_id = None`` so neither
    # participates in the dedup. (The ``requires_collection_id`` guard
    # is bypassed on this stub via the class attribute; in real subtypes
    # the guard inside ``verify()`` would still fire — but that's a
    # per-file ERROR result, not a session-level raise.)
    assert len(session_result.results) == 2


def test_construct_failure_one_yaml_does_not_break_dup_check():
    """Phase 1 failures are skipped in phase 1.5; remaining files still dedup."""
    # Use a kind that doesn't exist → for_kind() raises → phase 1 records
    # ``(None, None, exc, yaml_source)`` for that index. The other two files
    # dup each other and should still trigger the raise.
    sources = [
        _StubSource(label="broken", collection_id_override=None, kind="kind-that-does-not-exist"),
        _StubSource(label="dup1", collection_id_override="shared"),
        _StubSource(label="dup2", collection_id_override="shared"),
    ]

    with pytest.raises(InvalidArgumentException) as exc_info:
        execute_check_collections(yaml_sources=sources, data_source_impl=None)

    msg = str(exc_info.value)
    assert "'shared':" in msg
    assert "/fake/dup1.yml" in msg
    assert "/fake/dup2.yml" in msg
    # The broken file isn't in any dup group.
    assert "/fake/broken.yml" not in msg


def test_same_collection_id_across_different_wire_sources_allowed():
    """Same collection_id on two different subtypes (different wire_source) is OK."""
    sources = [
        _StubSource(label="a", collection_id_override="shared_name", kind=_DUP_KIND),
        _StubSource(label="b", collection_id_override="shared_name", kind=_DUP_OTHER_WIRE_KIND),
    ]
    session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)

    # No raise — different identity-prefix spaces.
    assert len(session_result.results) == 2


def test_subtype_without_combine_uploads_skipped():
    """combine_uploads=False subtypes don't participate even with non-None collection_id."""
    sources = [
        _StubSource(label="a", collection_id_override="same_id", kind=_NO_COMBINE_KIND),
        _StubSource(label="b", collection_id_override="same_id", kind=_NO_COMBINE_KIND),
    ]
    session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)

    # No raise — combine_uploads is False on this stub subtype.
    assert len(session_result.results) == 2


def test_phase_2_isolation_preserved_when_no_dups():
    """Control case: unique ids, one file raises during verify() → ERROR placeholder."""

    class _StubImplRaisingOnVerify(_StubImpl):
        kind = "dup-raising-stub"
        wire_source = "raising-wire"

        def verify(self):
            if self.yaml.yaml_source._label == "broken":
                raise RuntimeError("boom")
            return _make_passed_result(label=self.yaml.yaml_source._label)

    sources = [
        _StubSource(label="alpha", collection_id_override="id_alpha", kind="dup-raising-stub"),
        _StubSource(label="broken", collection_id_override="id_broken", kind="dup-raising-stub"),
        _StubSource(label="gamma", collection_id_override="id_gamma", kind="dup-raising-stub"),
    ]
    session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)

    assert len(session_result.results) == 3
    assert session_result.results[0].status is CheckCollectionStatus.PASSED
    assert session_result.results[1].status is CheckCollectionStatus.ERROR
    assert isinstance(session_result.results[1].error, RuntimeError)
    assert session_result.results[2].status is CheckCollectionStatus.PASSED
