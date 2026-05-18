"""Unit tests for the ``check_collections`` package base + executor.

Covers ``CheckCollectionImpl`` defaults, ``CheckCollectionItem`` immutability,
and ``execute_check_collections`` per-item isolation / abort semantics.
Uses an in-test sentinel ``FakeImpl`` so we don't depend on contract YAML
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
from soda_core.check_collections.session import (
    CheckCollectionItem,
    execute_check_collections,
)
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    Contract,
    YamlFileContentInfo,
)


class _FakeYaml(CheckCollectionYaml):
    """Sentinel yaml that records the yaml_source it was parsed from."""

    def __init__(self, yaml_source):
        self.yaml_source = yaml_source

    @classmethod
    def parse(cls, yaml_source, provided_variable_values=None, data_timestamp=None, **kwargs):
        return cls(yaml_source=yaml_source)


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


class _FakeImpl(CheckCollectionImpl):
    """Sentinel subtype that bypasses the real engine.

    Overrides ``__init__`` so we don't run YAML parsing, metric resolution,
    or query building. ``verify()`` returns a hardcoded result (or raises
    if the yaml records that it should).
    """

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
        collection_id=None,
        only_validate_without_execute=False,
        check_selectors=None,
        execution_timestamp=None,
        data_timestamp=None,
        all_data_source_impls=None,
        dwh_data_source_file_path=None,
        logs=None,
    ):
        # Skip ``CheckCollectionImpl.__init__`` (which wants a real YAML);
        # stash the few attributes the test cares about.
        self.yaml = yaml
        self.collection_id = collection_id
        # ``raise_on_verify`` lives on the yaml_source for per-instance control;
        # the FakeYaml wrapper carries the source reference through.
        source = getattr(yaml, "yaml_source", None)
        self._raise_on_verify: Optional[BaseException] = getattr(source, "raise_on_verify", None)

    def verify(self) -> _FakeResult:
        if self._raise_on_verify is not None:
            raise self._raise_on_verify
        return _make_passed_result(label=getattr(self.yaml.yaml_source, "_label", "anon"))


class _LabelledSource:
    """Tiny stand-in for ``CheckCollectionYamlSource`` in tests — carries a label."""

    def __init__(self, label: str, raise_on_verify: Optional[BaseException] = None):
        self._label = label
        self.raise_on_verify = raise_on_verify
        self.file_path = f"{label}.yml"
        self.yaml_str_original = f"# {label}"

    @classmethod
    def from_str(cls, *_args, **_kwargs):
        raise NotImplementedError


class _RaisingYaml(CheckCollectionYaml):
    """Yaml class whose ``parse(...)`` raises — exercises the parse-failure path."""

    @classmethod
    def parse(cls, yaml_source, **_kwargs):
        raise RuntimeError(f"parse failure for {yaml_source._label}")


class _RaisingImpl(CheckCollectionImpl):
    wire_source = "raising"
    display_name = "raising"
    yaml_class = _RaisingYaml
    result_class = _FakeResult


def test_execute_check_collections_runs_each_item_in_order():
    items = [
        CheckCollectionItem(impl_class=_FakeImpl, yaml_source=_LabelledSource("a")),
        CheckCollectionItem(impl_class=_FakeImpl, yaml_source=_LabelledSource("b")),
        CheckCollectionItem(impl_class=_FakeImpl, yaml_source=_LabelledSource("c")),
    ]
    session_result = execute_check_collections(items=items, data_source_impl=None)

    labels = [r.check_collection.dataset_name for r in session_result.results]
    assert labels == ["a", "b", "c"]
    assert all(r.status is CheckCollectionStatus.PASSED for r in session_result.results)


def test_execute_check_collections_isolates_per_item_errors_by_default():
    exc = RuntimeError("middle exploded")
    items = [
        CheckCollectionItem(impl_class=_FakeImpl, yaml_source=_LabelledSource("a")),
        CheckCollectionItem(impl_class=_FakeImpl, yaml_source=_LabelledSource("b", raise_on_verify=exc)),
        CheckCollectionItem(impl_class=_FakeImpl, yaml_source=_LabelledSource("c")),
    ]
    session_result = execute_check_collections(items=items, data_source_impl=None)

    assert len(session_result.results) == 3
    assert session_result.results[0].status is CheckCollectionStatus.PASSED
    assert session_result.results[1].status is CheckCollectionStatus.ERROR
    assert session_result.results[1].error is exc
    assert session_result.results[2].status is CheckCollectionStatus.PASSED


def test_execute_check_collections_abort_on_first_error_reraises_verbatim():
    exc = RuntimeError("first exploded")
    items = [
        CheckCollectionItem(impl_class=_FakeImpl, yaml_source=_LabelledSource("a", raise_on_verify=exc)),
        CheckCollectionItem(impl_class=_FakeImpl, yaml_source=_LabelledSource("b")),
    ]
    with pytest.raises(RuntimeError) as exc_info:
        execute_check_collections(items=items, data_source_impl=None, abort_on_first_error=True)
    assert exc_info.value is exc


def test_execute_check_collections_handles_parse_failures():
    items = [
        CheckCollectionItem(impl_class=_RaisingImpl, yaml_source=_LabelledSource("bad")),
    ]
    session_result = execute_check_collections(items=items, data_source_impl=None)
    assert len(session_result.results) == 1
    assert session_result.results[0].status is CheckCollectionStatus.ERROR
    assert isinstance(session_result.results[0].error, RuntimeError)


def test_check_collection_impl_default_verify_on_agent_raises_not_implemented():
    impl = _FakeImpl(yaml=_FakeYaml(yaml_source=_LabelledSource("a")))
    with pytest.raises(NotImplementedError) as exc_info:
        impl.verify_on_agent(
            soda_cloud_impl=None,
            variables={},
            blocking_timeout_in_minutes=60,
            publish_results=False,
            verbose=False,
        )
    assert "fake" in str(exc_info.value)


def test_check_collection_item_is_frozen():
    item = CheckCollectionItem(impl_class=_FakeImpl, yaml_source=_LabelledSource("a"), collection_id="cid-1")
    with pytest.raises(Exception):
        item.collection_id = "cid-2"  # type: ignore[misc]


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
