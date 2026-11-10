"""execute_check_collections raises ResultsRejectedBySodaCloudException when Soda Cloud rejected
every failed results upload of the session for a customer-caused reason.

The stub's ``verify()`` stands in for the per-file upload: it returns a result already flagged the
way ``SodaCloud.send_check_collection_results`` flags it. The yaml source label picks the outcome:
``"rejected"`` (customer-caused rejection), ``"unsent"`` (any other send failure), anything else
(sent).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest
from soda_core.check_collections.base import CheckCollectionImpl, CheckCollectionResult, CheckCollectionYaml
from soda_core.check_collections.session import execute_check_collections
from soda_core.common.exceptions import ResultsRejectedBySodaCloudException, ScanExecutionFailedException
from soda_core.common.logs import Logs
from soda_core.common.redaction import RedactionPolicy
from soda_core.contracts.contract_verification import CheckCollectionStatus, Contract, YamlFileContentInfo

_KIND = "rejection-stub"


class _StubYaml(CheckCollectionYaml):
    @classmethod
    def parse(cls, yaml_source, **kwargs):
        return cls(yaml_source=yaml_source, yaml_object=kwargs.get("yaml_object"))


class _StubResult(CheckCollectionResult):
    pass


class _StubImpl(CheckCollectionImpl):
    kind = _KIND
    wire_source = "rejection-stub-wire"
    yaml_class = _StubYaml
    result_class = _StubResult
    requires_collection_id = False

    def __init__(self, yaml, **kwargs):
        # Skip the heavy base ``__init__``: the executor only needs these attributes.
        self.yaml = yaml
        self.logs = Logs()
        self.data_source_impl = None
        self.redaction_policy = RedactionPolicy(frozenset())

    @property
    def collection_id(self) -> Optional[str]:
        return None

    def verify(self) -> _StubResult:
        label: str = self.yaml.yaml_source._label
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
            sending_results_to_soda_cloud_failed=label in ("rejected", "unsent"),
            log_records=[],
            post_processing_stages=[],
            soda_cloud_rejection_code="resource_attribute_not_found" if label == "rejected" else None,
        )


class _StubYamlObject:
    def read_string_opt(self, key: str, env_var: Optional[str] = None, default_value: Optional[str] = None):
        return _KIND if key == "kind" else default_value


class _StubSource:
    def __init__(self, label: str):
        self._label = label
        self.file_path = f"/fake/{label}.yml"
        self.yaml_str_original = f"# {label}"

    def parse(self):
        return _StubYamlObject()


@pytest.mark.parametrize("labels", [["rejected"], ["rejected", "sent"]])
def test_session_raises_when_every_failed_upload_was_rejected(labels):
    with pytest.raises(ResultsRejectedBySodaCloudException) as exc_info:
        execute_check_collections(yaml_sources=[_StubSource(label) for label in labels], data_source_impl=None)

    exc = exc_info.value
    # The CLI failure boundary handles it like any other expected failure.
    assert isinstance(exc, ScanExecutionFailedException)
    assert exc.rejection_codes == ["resource_attribute_not_found"]
    assert "resource_attribute_not_found" in str(exc)
    # Every file was still verified; the results stay available to programmatic callers.
    assert [r.check_collection.dataset_name for r in exc.session_result.results] == labels


@pytest.mark.parametrize("labels", [["sent"], ["unsent"], ["rejected", "unsent"]])
def test_session_returns_normally_otherwise(labels):
    """No send failure, or any send failure that is not a customer-caused rejection: the session
    returns its result, and the send failure keeps exiting 4."""
    session_result = execute_check_collections(
        yaml_sources=[_StubSource(label) for label in labels], data_source_impl=None
    )

    assert len(session_result.results) == len(labels)
    assert session_result.sending_results_to_soda_cloud_failed is ("unsent" in labels)
