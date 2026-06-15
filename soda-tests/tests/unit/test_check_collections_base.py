"""Unit tests for the ``check_collections`` package base + executor.

Covers ``CheckCollectionImpl`` defaults, kind-based dispatch via
``CheckCollectionImpl.for_kind`` / ``__init_subclass__``, and
``execute_check_collections`` per-item isolation / abort semantics.
Uses in-test sentinel ``FakeImpl`` so we don't depend on contract YAML
parsing or data sources.
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
from soda_core.common.logs import Logs
from soda_core.common.yaml import CheckCollectionYamlSource
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    Contract,
    YamlFileContentInfo,
)


class _FakeYaml(CheckCollectionYaml):
    """Sentinel yaml that records the yaml_source it was parsed from.

    Inherits the base ``__init__`` so the executor's post-parse reads of
    ``yaml.data_timestamp`` / ``yaml.execution_timestamp`` see real
    values (set by ``CheckCollectionYaml.__init__``).
    """


class _FakeResult(CheckCollectionResult):
    """Sentinel result subclass — same shape as ``CheckCollectionResult``."""


def _make_passed_result(label: str) -> _FakeResult:
    now = datetime.now(tz=timezone.utc)
    result = _FakeResult(
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
    return result


# Unique kinds per stub so registrations don't collide with siblings.
_FAKE_KIND = "fake-check-collections-base"
_RAISING_KIND = "raising-check-collections-base"


class _FakeImpl(CheckCollectionImpl):
    """Sentinel subtype that bypasses the real engine.

    Overrides ``__init__`` so we don't run YAML parsing, metric resolution,
    or query building. ``verify()`` returns a hardcoded result (or raises
    if the yaml records that it should).

    Declares a non-empty ``kind`` so it registers with
    ``CheckCollectionImpl._REGISTRY`` and is discoverable by the executor's
    per-yaml dispatch.
    """

    kind = _FAKE_KIND
    wire_source = "fake"
    display_name = "fake"
    yaml_class = _FakeYaml
    result_class = _FakeResult

    def __init__(
        self,
        yaml,
        data_source_impl=None,
        soda_cloud_impl=None,
        publish_results=False,
        only_validate_without_execute=False,
        check_selectors=None,
        execution_timestamp=None,
        data_timestamp=None,
        all_data_source_impls=None,
        dwh_files=None,
        logs=None,
        **kwargs,
    ):
        # Skip ``CheckCollectionImpl.__init__`` (which wants a real YAML);
        # stash the few attributes the test cares about. ``collection_id``
        # is inherited from the base class as a ``@property`` returning
        # ``None`` — no instance attribute needed.
        self.yaml = yaml
        self.logs = Logs()
        # ``raise_on_verify`` lives on the yaml_source for per-instance control;
        # the FakeYaml wrapper carries the source reference through.
        source = getattr(yaml, "yaml_source", None)
        self._raise_on_verify: Optional[BaseException] = getattr(source, "raise_on_verify", None)

    def verify(self) -> _FakeResult:
        if self._raise_on_verify is not None:
            raise self._raise_on_verify
        return _make_passed_result(label=getattr(self.yaml.yaml_source, "_label", "anon"))


class _FakeYamlObject:
    """Minimal YamlObject stand-in for sentinel sources.

    The executor calls ``yaml_source.parse()`` and reads ``kind`` via
    ``read_string_opt("kind")``. This stub returns the test-supplied kind.
    """

    def __init__(self, kind: Optional[str]):
        self._kind = kind

    def read_string_opt(self, key: str, env_var: Optional[str] = None, default_value: Optional[str] = None):
        if key == "kind":
            return self._kind
        return default_value


class _LabelledSource:
    """Tiny stand-in for ``CheckCollectionYamlSource`` in tests — carries a label.

    Exposes a ``parse()`` method returning a minimal yaml-object stub whose
    ``read_string_opt("kind")`` returns the configured kind so the executor's
    dispatch can route to the right impl class via ``CheckCollectionImpl.for_kind``.
    """

    def __init__(
        self,
        label: str,
        raise_on_verify: Optional[BaseException] = None,
        kind: Optional[str] = _FAKE_KIND,
    ):
        self._label = label
        self.raise_on_verify = raise_on_verify
        self._kind = kind
        self.file_path = f"{label}.yml"
        self.yaml_str_original = f"# {label}"

    def parse(self):
        return _FakeYamlObject(kind=self._kind)

    @classmethod
    def from_str(cls, *_args, **_kwargs):
        raise NotImplementedError


class _RaisingYaml(CheckCollectionYaml):
    """Yaml class whose ``parse(...)`` raises — exercises the parse-failure path."""

    @classmethod
    def parse(cls, yaml_source, **_kwargs):
        raise RuntimeError(f"parse failure for {yaml_source._label}")


class _RaisingImpl(CheckCollectionImpl):
    kind = _RAISING_KIND
    wire_source = "raising"
    display_name = "raising"
    yaml_class = _RaisingYaml
    result_class = _FakeResult


def test_execute_check_collections_runs_each_item_in_order():
    sources = [
        _LabelledSource("a"),
        _LabelledSource("b"),
        _LabelledSource("c"),
    ]
    session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)

    labels = [r.check_collection.dataset_name for r in session_result.results]
    assert labels == ["a", "b", "c"]
    assert all(r.status is CheckCollectionStatus.PASSED for r in session_result.results)


def test_execute_check_collections_isolates_per_item_errors_by_default():
    exc = RuntimeError("middle exploded")
    sources = [
        _LabelledSource("a"),
        _LabelledSource("b", raise_on_verify=exc),
        _LabelledSource("c"),
    ]
    session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)

    assert len(session_result.results) == 3
    assert session_result.results[0].status is CheckCollectionStatus.PASSED
    assert session_result.results[1].status is CheckCollectionStatus.ERROR
    assert session_result.results[1].error is exc
    assert session_result.results[2].status is CheckCollectionStatus.PASSED


def test_execute_check_collections_abort_on_first_error_reraises_verbatim():
    exc = RuntimeError("first exploded")
    sources = [
        _LabelledSource("a", raise_on_verify=exc),
        _LabelledSource("b"),
    ]
    with pytest.raises(RuntimeError) as exc_info:
        execute_check_collections(yaml_sources=sources, data_source_impl=None, abort_on_first_error=True)
    assert exc_info.value is exc


def test_execute_check_collections_handles_parse_failures():
    sources = [_LabelledSource("bad", kind=_RAISING_KIND)]
    session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)
    assert len(session_result.results) == 1
    assert session_result.results[0].status is CheckCollectionStatus.ERROR
    assert isinstance(session_result.results[0].error, RuntimeError)


def test_execute_check_collections_records_error_for_unknown_kind():
    """An unknown ``kind:`` produces an ERROR-status placeholder result;
    other items in the batch still run.
    """
    sources = [
        _LabelledSource("ok"),
        _LabelledSource("nope", kind="this-kind-does-not-exist"),
        _LabelledSource("also-ok"),
    ]
    session_result = execute_check_collections(yaml_sources=sources, data_source_impl=None)
    assert len(session_result.results) == 3
    assert session_result.results[0].status is CheckCollectionStatus.PASSED
    assert session_result.results[1].status is CheckCollectionStatus.ERROR
    assert isinstance(session_result.results[1].error, ValueError)
    assert "Unknown check-collection kind" in str(session_result.results[1].error)
    assert session_result.results[2].status is CheckCollectionStatus.PASSED


def test_execute_check_collections_unknown_kind_fallback_uses_default_impl_class():
    """N5: when kind dispatch fails, the ERROR placeholder result must be
    built by the caller-supplied ``default_impl_class``, not by the base
    ``CheckCollectionImpl``.

    The single-contract public surface
    (``ContractVerificationSessionImpl.execute``) returns
    ``list[ContractVerificationResult]``. If we fall back to
    ``CheckCollectionImpl.build_error_result``, the placeholder is a base
    ``CheckCollectionResult`` instance — violating the typed return.
    Passing ``default_impl_class=ContractImpl`` (or any subtype) ensures
    the fallback's result type matches the caller's declared subtype.
    """
    sources = [_LabelledSource("nope", kind="not-registered-anywhere")]
    session_result = execute_check_collections(
        yaml_sources=sources,
        data_source_impl=None,
        default_impl_class=_FakeImpl,
    )
    assert len(session_result.results) == 1
    assert session_result.results[0].status is CheckCollectionStatus.ERROR
    # Without the kwarg the fallback returned a plain ``CheckCollectionResult``;
    # with it the result must be the subtype's ``result_class``.
    assert isinstance(session_result.results[0], _FakeResult)


def test_check_collection_impl_default_verify_on_runner_raises_not_implemented():
    impl = _FakeImpl(yaml=_FakeYaml(yaml_source=_LabelledSource("a")))
    with pytest.raises(NotImplementedError) as exc_info:
        impl.verify_on_runner(
            soda_cloud_impl=None,
            variables={},
            blocking_timeout_in_minutes=60,
            publish_results=False,
            verbose=False,
        )
    assert "fake" in str(exc_info.value)
    impl.logs.close()  # release the active-capture slot taken at construction


def test_for_kind_returns_registered_impl_class():
    assert CheckCollectionImpl.for_kind(_FAKE_KIND) is _FakeImpl
    assert CheckCollectionImpl.for_kind(_RAISING_KIND) is _RaisingImpl


def test_for_kind_raises_value_error_for_unknown_kind():
    with pytest.raises(ValueError, match="Unknown check-collection kind"):
        CheckCollectionImpl.for_kind("definitely-not-registered")


def test_init_subclass_does_not_register_empty_kind():
    """A subclass with empty (default) ``kind`` must not register —
    test stubs rely on this so they can subclass without leaking into
    the global registry.
    """

    class _NoKindImpl(CheckCollectionImpl):
        # Inherits ``kind = ""`` from the base.
        wire_source = "no-kind"
        display_name = "no-kind"

    # _REGISTRY only contains entries with non-empty kinds.
    assert "" not in CheckCollectionImpl._REGISTRY
    assert _NoKindImpl not in CheckCollectionImpl._REGISTRY.values()


def test_build_error_result_returns_subtype_result_class():
    exc = ValueError("boom")
    result = _FakeImpl.build_error_result(yaml_source=_LabelledSource("bad"), exception=exc)

    assert isinstance(result, _FakeResult)
    assert result.status is CheckCollectionStatus.ERROR
    assert result.error is exc
    assert result.measurements == []
    assert result.check_results == []


def test_verify_raises_when_wire_source_is_empty():
    """A subclass that forgets to declare ``wire_source`` must fail loudly in ``verify()``."""

    class _NoWireSourceImpl(CheckCollectionImpl):
        # Inherits ``wire_source = ""`` from the base.
        display_name = "no-wire-source"
        yaml_class = _FakeYaml
        result_class = _FakeResult

        def __init__(self):
            # Bypass the heavy engine init — the guard runs at the top of ``verify()``.
            pass

    with pytest.raises(ValueError, match="wire_source"):
        _NoWireSourceImpl().verify()


class _FakeImplWithCollectionId(_FakeImpl):
    """Sentinel with a collection id, like a data standard's DQN."""

    kind = _FAKE_KIND + "-with-id"

    @property
    def collection_id(self) -> Optional[str]:
        return "dqn://fake/standard-1"


def test_source_description_prefers_the_file_path():
    """A file-sourced collection is identified by its path in console lines —
    even when a collection id is also available."""
    source = CheckCollectionYamlSource.from_str("kind: data_standard", file_path="/path/to/standard.yml")
    impl = _FakeImplWithCollectionId(yaml=_FakeYaml(yaml_source=source))
    assert impl.source_description == "/path/to/standard.yml"


def test_source_description_falls_back_to_collection_id_for_string_sources():
    """A Cloud-fetched yaml has no file path; console lines must show the
    collection id (e.g. a data standard's DQN) instead of 'None'."""
    source = CheckCollectionYamlSource.from_str("kind: data_standard")
    impl = _FakeImplWithCollectionId(yaml=_FakeYaml(yaml_source=source))
    assert impl.source_description == "dqn://fake/standard-1"


def test_source_description_last_resort_is_the_yaml_source_description():
    """No file path and no collection id (e.g. a contract string on the agent
    path) still yields a human-readable identifier, never 'None'."""
    source = CheckCollectionYamlSource.from_str("kind: contract")
    impl = _FakeImpl(yaml=_FakeYaml(yaml_source=source))  # collection_id is None
    assert impl.source_description == source.description
    assert "None" not in impl.source_description
