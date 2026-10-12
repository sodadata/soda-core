"""The session check on ``scope`` check filters.

Every ``scope`` value in the check filters must be ``base`` or a key that a collection declares somewhere in the
session. Otherwise ``execute_check_collections`` raises ``InvalidArgumentException`` after
constructing the collections and before any ``verify()``, so nothing is queried or uploaded. A value with a wildcard
is a pattern that must match at least one known key. A collection that does not declare a known key needs nothing
special: its checks
fail the filter and go up as EXCLUDED.

The stub impls skip the base ``__init__``, as several other test stubs do, so the check reads the class defaults
``CheckCollectionImpl`` declares for ``scopes``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock

import pytest
from soda_core.check_collections.base import CheckCollectionImpl, CheckCollectionResult, CheckCollectionYaml
from soda_core.check_collections.session import execute_check_collections, raise_if_unknown_scope_keys
from soda_core.common.exceptions import InvalidArgumentException, YamlParserException
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.contracts.contract_verification import CheckCollectionStatus, Contract, YamlFileContentInfo
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_core.contracts.impl.scope import Scope

# Unique kinds, so the stubs never collide with other test modules or the real kinds.
_SCOPED_KIND = "scope-key-validation-scoped-stub"

# Labels of the sources whose collection ran verify(), in order.
_verified: list[str] = []


class _StubYamlObject:
    def __init__(self, kind: str):
        self._kind = kind

    def read_string_opt(self, key: str, env_var: Optional[str] = None, default_value: Optional[str] = None):
        return self._kind if key == "kind" else default_value


class _StubSource:
    """A yaml source whose collection declares ``declared_keys``, or leaves ``scopes`` unset when it is None."""

    def __init__(
        self,
        label: str,
        declared_keys: Optional[list[str]] = None,
        kind: str = _SCOPED_KIND,
        parse_error: Optional[Exception] = None,
    ):
        self.label = label
        self.declared_keys = declared_keys
        self._kind = kind
        self._parse_error = parse_error
        self.file_path = f"/fake/{label}.yml"
        self.yaml_str_original = f"# {label}"

    def parse(self):
        if self._parse_error is not None:
            raise self._parse_error
        return _StubYamlObject(kind=self._kind)


class _StubYaml(CheckCollectionYaml):
    @classmethod
    def parse(cls, yaml_source, **kwargs):
        return cls(yaml_source=yaml_source, yaml_object=kwargs.get("yaml_object"))


class _StubResult(CheckCollectionResult):
    pass


class _ScopedStubImpl(CheckCollectionImpl):
    kind = _SCOPED_KIND
    wire_source = "scope-key-stub"
    yaml_class = _StubYaml
    result_class = _StubResult
    requires_collection_id = False
    combine_uploads = True

    def __init__(self, yaml, **kwargs):
        # Skips the base ``__init__``.
        self.yaml = yaml
        self.logs = Logs()
        declared_keys: Optional[list[str]] = yaml.yaml_source.declared_keys
        if declared_keys is not None:
            self.scopes = {key: Scope(key=key) for key in declared_keys}

    def verify(self) -> CheckCollectionResult:
        label: str = self.yaml.yaml_source.label
        _verified.append(label)
        now = datetime.now(tz=timezone.utc)
        return self.result_class(
            check_collection=Contract(
                data_source_name="fake_ds",
                dataset_prefix=[],
                dataset_name=label,
                soda_qualified_dataset_name=None,
                source=YamlFileContentInfo(source_content_str=None, local_file_path=None, soda_cloud_file_id="file-id"),
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


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    _verified.clear()
    yield


def _execute(sources: list[_StubSource], check_filters: Optional[list[str]], soda_cloud_impl=None):
    return execute_check_collections(
        yaml_sources=sources,
        data_source_impl=None,
        soda_cloud_impl=soda_cloud_impl,
        publish_results=soda_cloud_impl is not None,
        check_selectors=CheckSelector.parse_all(check_filters),
    )


@pytest.mark.parametrize("check_filter", ["scope=apac", "scope!=apac"])
def test_unknown_key_raises_before_verify_naming_the_key(check_filter: str):
    soda_cloud_impl = MagicMock(spec=SodaCloud)

    with pytest.raises(InvalidArgumentException) as exc_info:
        _execute([_StubSource("a", ["eu", "us"])], [check_filter], soda_cloud_impl)

    assert "'apac'" in str(exc_info.value)
    assert _verified == []
    soda_cloud_impl.send_check_collection_results.assert_not_called()


def test_every_unknown_key_is_named_once():
    with pytest.raises(InvalidArgumentException) as exc_info:
        _execute([_StubSource("a", ["eu"])], ["scope=apac", "scope!=latam", "scope=eu", "scope=apac"])

    message = str(exc_info.value)
    assert message.count("'apac'") == 1
    assert "'latam'" in message
    assert _verified == []


def test_known_keys_run_and_upload():
    soda_cloud_impl = MagicMock(spec=SodaCloud)

    session_result = _execute(
        [_StubSource("a", ["eu"]), _StubSource("b", ["us"])], ["scope=eu", "scope!=us"], soda_cloud_impl
    )

    assert len(session_result.results) == 2
    assert _verified == ["a", "b"]
    soda_cloud_impl.send_check_collection_results.assert_called_once()


@pytest.mark.parametrize("check_filter", ["scope=base", "scope!=base"])
def test_base_is_never_unknown(check_filter: str):
    session_result = _execute([_StubSource("a", [])], [check_filter])

    assert len(session_result.results) == 1
    assert _verified == ["a"]


@pytest.mark.parametrize("check_filter", ["scope=e*", "scope=ba?e"])
def test_a_wildcard_value_that_matches_a_known_key_runs(check_filter: str):
    session_result = _execute([_StubSource("a", ["eu"])], [check_filter])

    assert len(session_result.results) == 1
    assert _verified == ["a"]


@pytest.mark.parametrize("check_filter", ["scope=ap*", "scope!=a?ac"])
def test_a_wildcard_value_that_matches_no_known_key_fails_the_run(check_filter: str):
    # The known keys are those of every file in the session, so one file without the key is not enough to pass.
    with pytest.raises(InvalidArgumentException) as exc_info:
        _execute([_StubSource("a", ["eu"]), _StubSource("b", [])], [check_filter])

    assert repr(check_filter.split("=", 1)[1]) in str(exc_info.value)
    assert _verified == []


def test_a_collection_that_does_not_declare_a_known_key_still_runs():
    session_result = _execute(
        [_StubSource("a", ["eu"]), _StubSource("b", [])],
        ["scope=eu"],
    )

    assert len(session_result.results) == 2
    assert _verified == ["a", "b"]


class _ScopesNeverRead(_ScopedStubImpl):
    kind = "scope-key-validation-scopes-never-read-stub"

    @property
    def scopes(self):
        raise AssertionError("scopes read without a scope check filter")


@pytest.mark.parametrize("check_filters", [None, [], ["type=missing", "column!=id"]])
def test_no_scope_filter_means_no_check(check_filters: Optional[list[str]]):
    session_result = _execute([_StubSource("a", kind=_ScopesNeverRead.kind)], check_filters)

    assert len(session_result.results) == 1
    assert _verified == ["a"]


def test_list_syntax_for_a_scope_gets_a_hint():
    with pytest.raises(InvalidArgumentException, match=r"'\[eu,us\]'.*one scope filter per key, as in scope=eu and"):
        _execute([_StubSource("a")], ["scope=[eu,us]"])


def test_one_file_can_be_checked_on_its_own():
    # One file checked on its own, outside a session.
    class _OneFileImpl:
        def __init__(self):
            self.scopes = {"eu": Scope(key="eu")}

    constructed = [(_OneFileImpl(), _OneFileImpl, None, _StubSource("one"))]

    raise_if_unknown_scope_keys(constructed, CheckSelector.parse_all(["scope!=eu", "scope=base"]))
    with pytest.raises(InvalidArgumentException, match="'us'"):
        raise_if_unknown_scope_keys(constructed, CheckSelector.parse_all(["scope=us"]))


def test_a_key_declared_only_by_a_file_that_failed_to_parse_is_unknown(caplog):
    broken = _StubSource(
        "broken", ["apac"], parse_error=YamlParserException("YAML syntax error", "/fake/broken.yml[3,1]")
    )

    with caplog.at_level(logging.ERROR, logger="soda"):
        with pytest.raises(InvalidArgumentException, match="'apac'"):
            _execute([_StubSource("a", ["eu"]), broken], ["scope=apac"])

    # The parse error is logged before the session raises, since no ERROR placeholder is built for it.
    [record] = caplog.records
    assert "/fake/broken.yml" in record.getMessage()
    assert _verified == []


def test_a_failed_file_is_logged_once_when_the_keys_are_known(caplog):
    broken = _StubSource("broken", parse_error=YamlParserException("YAML syntax error", "/fake/broken.yml[3,1]"))

    with caplog.at_level(logging.ERROR, logger="soda"):
        session_result = _execute([_StubSource("a", ["eu"]), broken], ["scope=eu"])

    assert [result.status for result in session_result.results] == [
        CheckCollectionStatus.PASSED,
        CheckCollectionStatus.ERROR,
    ]
    [record] = caplog.records
    assert "/fake/broken.yml" in record.getMessage()
