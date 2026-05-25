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
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    Contract,
    YamlFileContentInfo,
)


# Unique kinds per stub so registrations don't collide with other test
# modules' sentinel impls or with the real subtypes.
_DUP_KIND = "dup-check-collection-id-stub"
# Reserved for Task 3 tests (cross-wire-source and combine-uploads opt-out cases).
_DUP_OTHER_WIRE_KIND = "dup-other-wire-source-stub"  # noqa: F841
_NO_COMBINE_KIND = "dup-no-combine-uploads-stub"  # noqa: F841


class _StubYaml(CheckCollectionYaml):
    """Yaml stub that records its source for verify()-time inspection."""

    def __init__(self, yaml_source):
        self.yaml_source = yaml_source

    @classmethod
    def parse(cls, yaml_source, **_kwargs):
        return cls(yaml_source=yaml_source)


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
