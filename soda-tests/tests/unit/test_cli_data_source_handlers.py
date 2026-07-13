import logging
from unittest.mock import MagicMock, mock_open, patch

import pytest
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.data_source import (
    handle_create_data_source,
    handle_discover_data_source,
    handle_discover_data_source_locally,
    handle_test_data_source,
)
from soda_core.cli.handlers.dependencies import run_with_failure_reporting


@patch("soda_core.cli.handlers.data_source.exists", return_value=True)
def test_create_data_source_when_file_exists(mock_exists):
    exit_code = handle_create_data_source("existing.yaml", "postgres")
    assert exit_code == ExitCode.LOG_ERRORS


@patch("soda_core.cli.handlers.data_source.exists", return_value=False)
def test_create_data_source_when_type_is_not_supported(mock_exists):
    exit_code = handle_create_data_source("new.yaml", "mysql")
    assert exit_code == ExitCode.LOG_ERRORS


@patch("builtins.open", new_callable=mock_open)
@patch("soda_core.cli.handlers.data_source.Path.mkdir")
@patch("soda_core.cli.handlers.data_source.exists", return_value=False)
def test_create_data_source_success(mock_exists, mock_mkdir, mock_file):
    exit_code = handle_create_data_source("new.yaml", "postgres")
    assert exit_code == ExitCode.OK
    mock_file.assert_called_once_with("new.yaml", "w")
    mock_mkdir.assert_called()


@patch("soda_core.cli.handlers.data_source.exists", return_value=False)
@patch("soda_core.cli.handlers.data_source.Path.mkdir", side_effect=Exception("mkdir error"))
def test_create_data_source_unexpected_exception(mock_mkdir, mock_exists):
    exit_code = handle_create_data_source("new.yaml", "postgres")
    assert exit_code == ExitCode.LOG_ERRORS


@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_test_data_source_success(mock_data_source_impl_cls):
    mock_instance = MagicMock()
    mock_instance.test_connection_error_message.return_value = None
    mock_data_source_impl_cls.from_yaml_source.return_value = mock_instance

    exit_code = handle_test_data_source("ds.yaml")
    assert exit_code == ExitCode.OK


@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_test_data_source_connection_error(mock_data_source_impl_cls):
    mock_instance = MagicMock()
    mock_instance.test_connection_error_message.return_value = "Connection failed"
    mock_data_source_impl_cls.from_yaml_source.return_value = mock_instance

    exit_code = handle_test_data_source("ds.yaml")
    assert exit_code == ExitCode.LOG_ERRORS


@patch("soda_core.common.data_source_impl.DataSourceImpl.from_yaml_source", return_value=None)
def test_test_data_source_returns_none(mock_from_file):
    exit_code = handle_test_data_source("ds.yaml")
    assert exit_code == ExitCode.LOG_ERRORS


@patch("soda_core.cli.handlers.data_source.EnvConfigHelper")
@patch("soda_core.cli.handlers.data_source.Logs")
@patch("soda_core.cli.handlers.data_source.LogsQueue")
@patch("soda_core.cli.handlers.data_source.SodaCloud")
@patch("soda_core.cli.handlers.data_source.SodaCloudYamlSource")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_test_data_source_uploads_logs_when_scan_id_provided(
    mock_data_source_impl_cls,
    mock_yaml_source_cls,
    mock_soda_cloud_cls,
    mock_logs_queue_cls,
    mock_logs_cls,
    mock_env_config_helper_cls,
):
    mock_instance = MagicMock()
    mock_instance.test_connection_error_message.return_value = None
    mock_data_source_impl_cls.from_yaml_source.return_value = mock_instance

    mock_env_config_helper_cls.return_value.soda_scan_id = "scan-id-123"
    mock_soda_cloud = MagicMock()
    mock_soda_cloud_cls.from_yaml_source.return_value = mock_soda_cloud
    mock_logs_queue = MagicMock()
    mock_logs_queue_cls.return_value = mock_logs_queue
    mock_logs = MagicMock()
    mock_logs_cls.return_value = mock_logs

    exit_code = handle_test_data_source("ds.yaml", soda_cloud_file_path="sc.yaml")

    assert exit_code == ExitCode.OK
    mock_logs_queue_cls.assert_called_once_with(
        soda_cloud=mock_soda_cloud,
        stage="test_connection",
        scan_id="scan-id-123",
        dataset="",
    )
    mock_logs_cls.assert_called_once_with(gatherer=mock_logs_queue)
    mock_logs.close.assert_called_once()


@patch("soda_core.cli.handlers.data_source.EnvConfigHelper")
@patch("soda_core.cli.handlers.data_source.LogsQueue")
@patch("soda_core.cli.handlers.data_source.SodaCloud")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_test_data_source_skips_log_upload_when_scan_id_missing(
    mock_data_source_impl_cls,
    mock_soda_cloud_cls,
    mock_logs_queue_cls,
    mock_env_config_helper_cls,
):
    mock_instance = MagicMock()
    mock_instance.test_connection_error_message.return_value = None
    mock_data_source_impl_cls.from_yaml_source.return_value = mock_instance

    mock_env_config_helper_cls.return_value.soda_scan_id = None

    exit_code = handle_test_data_source("ds.yaml", soda_cloud_file_path="sc.yaml")

    assert exit_code == ExitCode.OK
    mock_soda_cloud_cls.from_yaml_source.assert_not_called()
    mock_logs_queue_cls.assert_not_called()


@patch("soda_core.cli.handlers.data_source.EnvConfigHelper")
@patch("soda_core.cli.handlers.data_source.LogsQueue")
@patch("soda_core.cli.handlers.data_source.SodaCloud")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_test_data_source_skips_log_upload_when_soda_cloud_missing(
    mock_data_source_impl_cls,
    mock_soda_cloud_cls,
    mock_logs_queue_cls,
    mock_env_config_helper_cls,
):
    mock_instance = MagicMock()
    mock_instance.test_connection_error_message.return_value = None
    mock_data_source_impl_cls.from_yaml_source.return_value = mock_instance

    mock_env_config_helper_cls.return_value.soda_scan_id = "scan-id-123"

    exit_code = handle_test_data_source("ds.yaml", soda_cloud_file_path=None)

    assert exit_code == ExitCode.OK
    mock_soda_cloud_cls.from_yaml_source.assert_not_called()
    mock_logs_queue_cls.assert_not_called()


@patch("soda_core.cli.handlers.data_source.EnvConfigHelper")
@patch("soda_core.cli.handlers.data_source.Logs")
@patch("soda_core.cli.handlers.data_source.LogsQueue")
@patch("soda_core.cli.handlers.data_source.SodaCloud")
@patch("soda_core.cli.handlers.data_source.SodaCloudYamlSource")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_test_data_source_uploader_captures_and_flushes_when_parsing_raises(
    mock_data_source_impl_cls,
    mock_yaml_source_cls,
    mock_soda_cloud_cls,
    mock_logs_queue_cls,
    mock_logs_cls,
    mock_env_config_helper_cls,
):
    mock_env_config_helper_cls.return_value.soda_scan_id = "scan-id-123"
    mock_logs_queue = MagicMock()
    mock_logs_queue_cls.return_value = mock_logs_queue
    mock_logs = MagicMock()
    mock_logs_cls.return_value = mock_logs
    mock_data_source_impl_cls.from_yaml_source.side_effect = RuntimeError("bad data source yaml")

    with pytest.raises(RuntimeError):
        handle_test_data_source("ds.yaml", soda_cloud_file_path="sc.yaml")

    # Uploader is attached before parsing, so an early parse failure is still captured and flushed.
    mock_logs_cls.assert_called_once_with(gatherer=mock_logs_queue)
    mock_logs.close.assert_called_once()


# Discovery handler tests. The handler receives fully resolved dependencies
# (resolution + exit-code mapping are covered in test_cli_dependencies.py).
# Convention: engine failures propagate raw without logging — the CLI wiring
# is the single logging site; a rejected results upload is not an engine
# failure and returns RESULTS_NOT_SENT_TO_CLOUD directly.


def _data_source_impl_fake() -> MagicMock:
    data_source_impl = MagicMock()
    data_source_impl.name = "test_ds"
    return data_source_impl


@patch("soda_core.discovery.discovery_payload.send_discovery_results")
@patch("soda_core.discovery.discovery_run.DiscoveryRun")
def test_discover_success_sends_results_and_exits_ok(mock_discovery_run_cls, mock_send_discovery_results):
    data_source_impl = _data_source_impl_fake()
    mock_discovery_run_cls.execute.return_value = ["ds/schema/table"]
    mock_send_discovery_results.return_value = MagicMock(ok=True)

    exit_code = handle_discover_data_source(data_source_impl, soda_cloud=MagicMock(), scan_definition_name="my_scan")

    assert exit_code == ExitCode.OK
    # Resolution only parses YAML: the handler owns the connection lifecycle.
    data_source_impl.open_connection.assert_called_once()
    data_source_impl.close_connection.assert_called_once()


@patch("soda_core.discovery.discovery_run.DiscoveryRun")
def test_discover_query_failure_propagates_raw(mock_discovery_run_cls, caplog):
    data_source_impl = _data_source_impl_fake()
    mock_discovery_run_cls.execute.side_effect = Exception("query failed")

    with pytest.raises(Exception, match="query failed"):
        handle_discover_data_source(data_source_impl, soda_cloud=MagicMock(), scan_definition_name="my_scan")

    # Nothing is logged at the raise site: the CLI wiring logs once, with the traceback.
    assert not any("query failed" in record.getMessage() for record in caplog.records)
    # The connection is always released; the CLI wiring maps the raise to failure reporting.
    data_source_impl.close_connection.assert_called_once()


@patch("soda_core.discovery.discovery_payload.send_discovery_results")
@patch("soda_core.discovery.discovery_run.DiscoveryRun")
def test_discover_results_send_rejected_exits_results_not_sent(mock_discovery_run_cls, mock_send_discovery_results):
    soda_cloud = MagicMock()
    mock_discovery_run_cls.execute.return_value = ["ds/schema/table"]
    mock_send_discovery_results.return_value = MagicMock(ok=False)

    exit_code = handle_discover_data_source(_data_source_impl_fake(), soda_cloud, scan_definition_name="my_scan")

    # A rejected results upload is not an engine failure: no mark_scan_as_failed,
    # the exit code alone signals the undelivered results.
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    soda_cloud.mark_scan_as_failed.assert_not_called()


@patch("soda_core.discovery.discovery_payload.build_discovery_payload")
@patch("soda_core.discovery.discovery_run.DiscoveryRun")
def test_discover_unexpected_post_query_exception_propagates(mock_discovery_run_cls, mock_build_discovery_payload):
    mock_discovery_run_cls.execute.return_value = ["ds/schema/table"]
    mock_build_discovery_payload.side_effect = RuntimeError("payload build failed")

    # Unexpected exceptions propagate; the CLI wiring logs and reports them.
    with pytest.raises(RuntimeError):
        handle_discover_data_source(_data_source_impl_fake(), soda_cloud=MagicMock(), scan_definition_name="my_scan")


# Local discovery handler tests. Unlike the Cloud sibling there is no scan
# lifecycle and no failure reporting: failures are logged and map straight to
# LOG_ERRORS.


@patch("soda_core.discovery.discovery_run.DiscoveryRun")
def test_discover_locally_prints_dqns_and_exits_ok(mock_discovery_run_cls, caplog):
    data_source_impl = _data_source_impl_fake()
    mock_discovery_run_cls.execute.return_value = ["test_ds/schema/table_a", "test_ds/schema/table_b"]

    with caplog.at_level(logging.INFO, logger="soda"):
        exit_code = handle_discover_data_source_locally(data_source_impl)

    assert exit_code == ExitCode.OK
    messages = [record.getMessage() for record in caplog.records]
    # One DQN per line, followed by a summary count.
    assert "test_ds/schema/table_a" in messages
    assert "test_ds/schema/table_b" in messages
    assert any("Discovered 2 datasets" in message for message in messages)
    # Resolution only parses YAML: the handler owns the connection lifecycle.
    data_source_impl.open_connection.assert_called_once()
    data_source_impl.close_connection.assert_called_once()


@patch("soda_core.discovery.discovery_run.DiscoveryRun")
def test_discover_locally_passes_include_exclude_filters(mock_discovery_run_cls):
    data_source_impl = _data_source_impl_fake()
    mock_discovery_run_cls.execute.return_value = []

    exit_code = handle_discover_data_source_locally(data_source_impl, include=["cust%"], exclude=["tmp%"])

    assert exit_code == ExitCode.OK
    mock_discovery_run_cls.execute.assert_called_once_with(
        data_source_impl=data_source_impl,
        prefixes=[],
        include=["cust%"],
        exclude=["tmp%"],
    )


@patch("soda_core.discovery.discovery_run.DiscoveryRun")
def test_discover_locally_query_failure_exits_log_errors(mock_discovery_run_cls, caplog):
    data_source_impl = _data_source_impl_fake()
    mock_discovery_run_cls.execute.side_effect = Exception("query failed")

    exit_code = handle_discover_data_source_locally(data_source_impl)

    assert exit_code == ExitCode.LOG_ERRORS
    assert any("query failed" in record.getMessage() for record in caplog.records)
    # The connection is always released, also on failure.
    data_source_impl.close_connection.assert_called_once()


# Full discovery flow, wired as cli.py wires it: dependency resolution +
# centralized failure mapping around the handler. Managed scans (SODA_SCAN_ID
# set) report engine failures to Soda Cloud via mark_scan_as_failed with the
# captured log records and exit LOG_ERRORS (3). RESULTS_NOT_SENT_TO_CLOUD (4)
# means nothing reached Cloud: the managed launcher treats exit codes > 3 as
# undelivered and marks the scan failed itself.


def _run_discover_flow() -> ExitCode:
    return run_with_failure_reporting(
        data_source_file_path="ds.yaml",
        soda_cloud_file_path="sc.yaml",
        command=lambda data_source_impl, soda_cloud: handle_discover_data_source(
            data_source_impl, soda_cloud, scan_definition_name="my_scan"
        ),
    )


@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_discover_data_source_creation_failure_marks_scan_failed(
    mock_data_source_impl_cls, mock_soda_cloud_cls, mock_sc_yaml_source_cls, mock_ds_yaml_source_cls, monkeypatch
):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_soda_cloud = MagicMock()
    mock_soda_cloud.mark_scan_as_failed.return_value = True
    mock_soda_cloud_cls.from_yaml_source.return_value = mock_soda_cloud
    mock_data_source_impl_cls.from_yaml_source.return_value = None

    exit_code = _run_discover_flow()

    assert exit_code == ExitCode.LOG_ERRORS
    mock_soda_cloud.mark_scan_as_failed.assert_called_once()
    _, kwargs = mock_soda_cloud.mark_scan_as_failed.call_args
    assert kwargs["scan_id"] == "scan-123"
    assert any("Data source could not be created" in record.getMessage() for record in kwargs["logs"])


@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_discover_data_source_creation_failure_without_scan_id_exits_log_errors(
    mock_data_source_impl_cls, mock_soda_cloud_cls, mock_sc_yaml_source_cls, mock_ds_yaml_source_cls, monkeypatch
):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    mock_soda_cloud = MagicMock()
    mock_soda_cloud_cls.from_yaml_source.return_value = mock_soda_cloud
    mock_data_source_impl_cls.from_yaml_source.return_value = None

    exit_code = _run_discover_flow()

    assert exit_code == ExitCode.LOG_ERRORS
    mock_soda_cloud.mark_scan_as_failed.assert_not_called()


@patch("soda_core.discovery.discovery_run.DiscoveryRun")
@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_discover_execution_exception_marks_scan_failed(
    mock_data_source_impl_cls,
    mock_soda_cloud_cls,
    mock_sc_yaml_source_cls,
    mock_ds_yaml_source_cls,
    mock_discovery_run_cls,
    monkeypatch,
):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_soda_cloud = MagicMock()
    mock_soda_cloud.mark_scan_as_failed.return_value = True
    mock_soda_cloud_cls.from_yaml_source.return_value = mock_soda_cloud
    mock_data_source_impl_cls.from_yaml_source.return_value = MagicMock()
    mock_discovery_run_cls.execute.side_effect = Exception("query failed")

    exit_code = _run_discover_flow()

    assert exit_code == ExitCode.LOG_ERRORS
    mock_soda_cloud.mark_scan_as_failed.assert_called_once()
    _, kwargs = mock_soda_cloud.mark_scan_as_failed.call_args
    assert kwargs["scan_id"] == "scan-123"
    # The single failure line originates from the wiring (with traceback), and
    # is captured into the reported records.
    failure_records = [record for record in kwargs["logs"] if "query failed" in record.getMessage()]
    assert len(failure_records) == 1
    assert failure_records[0].exc_info is not None


@patch("soda_core.discovery.discovery_run.DiscoveryRun")
@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_discover_execution_exception_when_mark_scan_failed_rejected_exits_results_not_sent(
    mock_data_source_impl_cls,
    mock_soda_cloud_cls,
    mock_sc_yaml_source_cls,
    mock_ds_yaml_source_cls,
    mock_discovery_run_cls,
    monkeypatch,
):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_soda_cloud = MagicMock()
    mock_soda_cloud.mark_scan_as_failed.return_value = False
    mock_soda_cloud_cls.from_yaml_source.return_value = mock_soda_cloud
    mock_data_source_impl_cls.from_yaml_source.return_value = MagicMock()
    mock_discovery_run_cls.execute.side_effect = Exception("query failed")

    exit_code = _run_discover_flow()

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


@patch("soda_core.discovery.discovery_payload.build_discovery_payload")
@patch("soda_core.discovery.discovery_run.DiscoveryRun")
@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_discover_unexpected_post_query_exception_marks_scan_failed(
    mock_data_source_impl_cls,
    mock_soda_cloud_cls,
    mock_sc_yaml_source_cls,
    mock_ds_yaml_source_cls,
    mock_discovery_run_cls,
    mock_build_discovery_payload,
    monkeypatch,
):
    # An unexpected exception outside the query phase (here: payload build) must not
    # propagate out of the flow; it goes through the same failure reporting.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    mock_soda_cloud = MagicMock()
    mock_soda_cloud.mark_scan_as_failed.return_value = True
    mock_soda_cloud_cls.from_yaml_source.return_value = mock_soda_cloud
    mock_data_source_impl_cls.from_yaml_source.return_value = MagicMock()
    mock_discovery_run_cls.execute.return_value = ["ds/schema/table"]
    mock_build_discovery_payload.side_effect = RuntimeError("payload build failed")

    exit_code = _run_discover_flow()

    assert exit_code == ExitCode.LOG_ERRORS
    mock_soda_cloud.mark_scan_as_failed.assert_called_once()
    _, kwargs = mock_soda_cloud.mark_scan_as_failed.call_args
    assert kwargs["scan_id"] == "scan-123"
    assert any("payload build failed" in record.getMessage() for record in kwargs["logs"])
