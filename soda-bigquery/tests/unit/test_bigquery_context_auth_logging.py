import logging

from soda_bigquery.common.data_sources.bigquery_data_source_connection import (
    BigQueryContextAuth,
    BigQueryDataSourceConnection,
)


def _fake_default():
    return ("fake-credentials", "fake-project-id")


def test_logs_info_when_account_info_json_provided_under_context_auth(caplog, monkeypatch):
    monkeypatch.setattr(
        "soda_bigquery.common.data_sources.bigquery_data_source_connection.default",
        _fake_default,
    )
    config = BigQueryContextAuth(
        use_context_auth=True,
        account_info_json="ignored-fallback-json",
    )
    connection = BigQueryDataSourceConnection.__new__(BigQueryDataSourceConnection)

    with caplog.at_level(logging.INFO, logger="soda"):
        connection._load_project_id_and_credentials(config)

    assert any(
        "account_info_json" in record.message and "ignored" in record.message for record in caplog.records
    ), f"Expected a warning about ignored account_info_json. Records: {[r.message for r in caplog.records]}"


def test_no_log_when_only_use_context_auth_is_provided(caplog, monkeypatch):
    monkeypatch.setattr(
        "soda_bigquery.common.data_sources.bigquery_data_source_connection.default",
        _fake_default,
    )
    config = BigQueryContextAuth(use_context_auth=True)
    connection = BigQueryDataSourceConnection.__new__(BigQueryDataSourceConnection)

    with caplog.at_level(logging.INFO, logger="soda"):
        connection._load_project_id_and_credentials(config)

    assert not any("account_info_json" in record.message for record in caplog.records)
