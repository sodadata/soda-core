from unittest.mock import MagicMock, mock_open, patch

import pytest
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.data_source import (
    handle_create_data_source,
    handle_test_data_source,
)


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
