from unittest.mock import patch, mock_open, MagicMock
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.data_source import handle_create_data_source, handle_test_data_source


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
    mock_data_source_impl_cls.from_file.return_value = mock_instance

    exit_code = handle_test_data_source("ds.yaml")
    assert exit_code == ExitCode.OK


@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_test_data_source_connection_error(mock_data_source_impl_cls):
    mock_instance = MagicMock()
    mock_instance.test_connection_error_message.return_value = "Connection failed"
    mock_data_source_impl_cls.from_file.return_value = mock_instance

    exit_code = handle_test_data_source("ds.yaml")
    assert exit_code == ExitCode.LOG_ERRORS


@patch("soda_core.common.data_source_impl.DataSourceImpl.from_file", return_value=None)
def test_test_data_source_returns_none(mock_from_file):
    exit_code = handle_test_data_source("ds.yaml")
    assert exit_code == ExitCode.LOG_ERRORS
