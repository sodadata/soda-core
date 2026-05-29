"""``expected_kinds`` enforcement in execute_check_collections.

Subtype-narrowed public APIs (``verify_data_standards``, future
``verify_reconciliations``, ...) pin the executor to a single permitted
top-level ``kind:`` via ``expected_kinds={"<their kind>"}``. The
executor reads each yaml's kind exactly once (the same parse that
drives ``CheckCollectionImpl.for_kind(...)``); offenders are collected
during phase 1 and raised eagerly before any ``verify()`` runs.

These tests exercise the structural invariant on sentinel impl/yaml
classes — no real engine, no data source.
"""

from __future__ import annotations

from typing import Optional

import pytest
from soda_core.check_collections.base import (
    CheckCollectionImpl,
    CheckCollectionResult,
    CheckCollectionYaml,
)
from soda_core.check_collections.session import execute_check_collections
from soda_core.common.exceptions import InvalidArgumentException

_KIND_A = "expected-kinds-stub-a"
_KIND_B = "expected-kinds-stub-b"


class _StubYaml(CheckCollectionYaml):
    @classmethod
    def parse(cls, yaml_source, **kwargs):
        return cls(yaml_source=yaml_source, yaml_object=kwargs.get("yaml_object"))


class _StubResultA(CheckCollectionResult):
    pass


class _StubResultB(CheckCollectionResult):
    pass


class _StubImplA(CheckCollectionImpl):
    kind = _KIND_A
    wire_source = "stub-wire-a"
    yaml_class = _StubYaml
    result_class = _StubResultA

    def __init__(self, yaml, **kwargs):
        self.yaml = yaml
        type(self).verify_call_count = getattr(type(self), "verify_call_count", 0)

    def verify(self) -> _StubResultA:
        type(self).verify_call_count += 1
        # Return a sentinel — we never assert on the value; the test
        # asserts on the raise BEFORE verify() runs.
        raise AssertionError("verify() should never run when expected_kinds rejects the session")


class _StubImplB(_StubImplA):
    kind = _KIND_B
    wire_source = "stub-wire-b"
    result_class = _StubResultB


class _StubYamlObject:
    def __init__(self, kind: Optional[str]):
        self._kind = kind

    def read_string_opt(self, key: str, env_var: Optional[str] = None, default_value: Optional[str] = None):
        if key == "kind":
            return self._kind
        return default_value


class _StubSource:
    def __init__(self, label: str, kind: Optional[str]):
        self._label = label
        self._kind = kind
        self.file_path = f"/fake/{label}.yml"
        self.yaml_str_original = f"# {label}"

    def parse(self):
        return _StubYamlObject(kind=self._kind)


@pytest.fixture(autouse=True)
def _reset_verify_counter():
    _StubImplA.verify_call_count = 0
    _StubImplB.verify_call_count = 0
    yield


def test_expected_kinds_rejects_yaml_with_wrong_kind():
    """One yaml outside the expected set → raise lists it, verify() never runs."""
    sources = [
        _StubSource(label="ok", kind=_KIND_A),
        _StubSource(label="wrong", kind=_KIND_B),
    ]
    with pytest.raises(InvalidArgumentException) as exc_info:
        execute_check_collections(
            yaml_sources=sources,
            data_source_impl=None,
            expected_kinds={_KIND_A},
        )
    msg = str(exc_info.value)
    assert _KIND_A in msg, "Expected kind set should appear in the error"
    assert "/fake/wrong.yml" in msg
    assert f"kind='{_KIND_B}'" in msg
    assert "/fake/ok.yml" not in msg, "Compliant yaml should not be in offender list"
    assert _StubImplA.verify_call_count == 0


def test_expected_kinds_rejects_yaml_missing_kind():
    """A yaml without ``kind:`` is rejected — and the offender report
    surfaces the raw ``None`` (NOT the ``"contract"`` dispatch default)
    so the user can tell their yaml is missing the field, not declaring
    the wrong value."""
    sources = [
        _StubSource(label="no_kind", kind=None),
    ]
    with pytest.raises(InvalidArgumentException) as exc_info:
        execute_check_collections(
            yaml_sources=sources,
            data_source_impl=None,
            expected_kinds={_KIND_A},
        )
    msg = str(exc_info.value)
    assert "/fake/no_kind.yml" in msg
    # Missing kind reads as ``None`` in the offender list — not the
    # dispatch default ``"contract"``, which would mislead the user.
    assert "kind=None" in msg
    assert "kind='contract'" not in msg


def test_expected_kinds_none_accepts_everything():
    """Default ``expected_kinds=None`` runs every registered kind without filtering."""
    sources = [
        _StubSource(label="a", kind=_KIND_A),
        _StubSource(label="b", kind=_KIND_B),
    ]
    # Both stubs raise on verify() — but with expected_kinds=None the
    # phase-1 kind check passes, both impls construct, and phase 2 runs.
    # We only care that NO InvalidArgumentException is raised at phase 1.25;
    # the per-file verify() failure isolation is exercised by other tests.
    try:
        execute_check_collections(yaml_sources=sources, data_source_impl=None)
    except InvalidArgumentException:
        pytest.fail("expected_kinds=None should not raise InvalidArgumentException")


def test_expected_kinds_multiple_allowed_kinds():
    """expected_kinds={KIND_A, KIND_B} accepts both; a third kind is rejected."""
    sources = [
        _StubSource(label="a", kind=_KIND_A),
        _StubSource(label="b", kind=_KIND_B),
        _StubSource(label="other", kind="some-third-kind"),
    ]
    with pytest.raises(InvalidArgumentException) as exc_info:
        execute_check_collections(
            yaml_sources=sources,
            data_source_impl=None,
            expected_kinds={_KIND_A, _KIND_B},
        )
    msg = str(exc_info.value)
    assert "/fake/other.yml" in msg
    assert "kind='some-third-kind'" in msg
    # The two allowed yamls should NOT appear in the offender list.
    assert "/fake/a.yml" not in msg
    assert "/fake/b.yml" not in msg


def test_expected_kinds_collects_all_offenders_in_one_raise():
    """Multiple wrong-kind yamls → one raise listing every offender."""
    sources = [
        _StubSource(label="ok", kind=_KIND_A),
        _StubSource(label="wrong_1", kind=_KIND_B),
        _StubSource(label="wrong_2", kind="another-kind"),
    ]
    with pytest.raises(InvalidArgumentException) as exc_info:
        execute_check_collections(
            yaml_sources=sources,
            data_source_impl=None,
            expected_kinds={_KIND_A},
        )
    msg = str(exc_info.value)
    assert "/fake/wrong_1.yml" in msg
    assert "/fake/wrong_2.yml" in msg
