"""Unit tests for R-657 task 2 (INFO hygiene) fixes on ``SodaCloud``.

- ``fetch_dataset_configuration`` / ``fetch_datasets_configurations`` interpolate
  ``DatasetIdentifier`` via ``.to_string()`` rather than the object directly — the
  object has no ``__str__``, so bare interpolation falls back to ``__repr__`` and
  leaks the internal ``DatasetIdentifier(data_source=..., prefixes=..., dataset=...)``
  shape into user-facing log/exception text.
- ``post_processing_update`` logs one INFO line per transition (the completed
  form); the pre-announcement is DEBUG-only.
"""

import logging

import pytest
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.exceptions import SodaCloudException
from soda_core.common.soda_cloud import SodaCloud
from soda_core.contracts.contract_verification import PostProcessingStageState


def _soda_cloud() -> SodaCloud:
    return SodaCloud(
        host="cloud.soda.io", api_key_id="id", api_key_secret="secret", token="preset-token", port=None, scheme="https"
    )


class _ConfigsResponse:
    ok = True
    status_code = 200

    def __init__(self, results):
        self._results = results

    def json(self):
        return {"results": self._results}


class _FailedConfigsResponse:
    ok = False
    status_code = 400

    def json(self):
        return {"message": "boom"}


def test_fetch_datasets_configurations_info_line_uses_to_string_not_repr(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="soda")
    soda_cloud = _soda_cloud()
    identifier = DatasetIdentifier(data_source_name="ds", prefixes=["schema"], dataset_name="table")
    monkeypatch.setattr(soda_cloud, "_execute_query", lambda *args, **kwargs: _ConfigsResponse([]))

    soda_cloud.fetch_datasets_configurations([identifier])

    info_records = [
        r for r in caplog.records if r.name == "soda" and "Fetching datasets configurations" in r.getMessage()
    ]
    assert len(info_records) == 1
    message = info_records[0].getMessage()
    assert "ds/schema/table" in message
    assert "DatasetIdentifier(" not in message


def test_fetch_datasets_configurations_failure_message_uses_to_string_not_repr(monkeypatch):
    soda_cloud = _soda_cloud()
    identifier = DatasetIdentifier(data_source_name="ds", prefixes=[], dataset_name="table")
    monkeypatch.setattr(soda_cloud, "_execute_query", lambda *args, **kwargs: _FailedConfigsResponse())

    with pytest.raises(SodaCloudException) as exc_info:
        soda_cloud.fetch_datasets_configurations([identifier])

    assert "ds/table" in str(exc_info.value)
    assert "DatasetIdentifier(" not in str(exc_info.value)


def test_fetch_dataset_configuration_mismatch_message_uses_to_string_not_repr(monkeypatch):
    soda_cloud = _soda_cloud()
    identifier = DatasetIdentifier(data_source_name="ds", prefixes=[], dataset_name="table")
    # Backend returns a configuration for a different dataset than requested,
    # forcing the mismatch branch that raises with the identifier interpolated.
    monkeypatch.setattr(
        soda_cloud,
        "_execute_query",
        lambda *args, **kwargs: _ConfigsResponse([{"datasetQualifiedName": "ds/other-table"}]),
    )

    with pytest.raises(SodaCloudException) as exc_info:
        soda_cloud.fetch_dataset_configuration(identifier)

    assert "ds/table" in str(exc_info.value)
    assert "DatasetIdentifier(" not in str(exc_info.value)


class _PostProcessingOkResponse:
    ok = True
    status_code = 200

    def json(self):
        return {}


def test_post_processing_update_logs_updating_at_debug_and_updated_at_info(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="soda")
    soda_cloud = _soda_cloud()
    monkeypatch.setattr(soda_cloud, "_execute_command", lambda *args, **kwargs: _PostProcessingOkResponse())

    soda_cloud.post_processing_update(stage="my-stage", scan_id="scan-1", state=PostProcessingStageState.COMPLETED)

    updating_records = [
        r for r in caplog.records if r.name == "soda" and "Updating post processing stage" in r.getMessage()
    ]
    updated_records = [
        r for r in caplog.records if r.name == "soda" and "Updated post processing stage" in r.getMessage()
    ]
    assert len(updating_records) == 1
    assert updating_records[0].levelno == logging.DEBUG
    assert len(updated_records) == 1
    assert updated_records[0].levelno == logging.INFO


def test_post_processing_update_updating_line_absent_above_debug(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="soda")
    soda_cloud = _soda_cloud()
    monkeypatch.setattr(soda_cloud, "_execute_command", lambda *args, **kwargs: _PostProcessingOkResponse())

    soda_cloud.post_processing_update(stage="my-stage", scan_id="scan-1", state=PostProcessingStageState.COMPLETED)

    updating_records = [
        r for r in caplog.records if r.name == "soda" and "Updating post processing stage" in r.getMessage()
    ]
    updated_records = [
        r for r in caplog.records if r.name == "soda" and "Updated post processing stage" in r.getMessage()
    ]
    assert updating_records == []
    assert len(updated_records) == 1
