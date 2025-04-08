from unittest.mock import MagicMock, mock_open, patch

from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.soda_cloud import (
    handle_create_soda_cloud,
    handle_test_soda_cloud,
)


@patch("soda_core.cli.handlers.soda_cloud.exists", return_value=True)
def test_create_soda_cloud_when_file_exists(mock_exists):
    exit_code = handle_create_soda_cloud("sc.yaml")
    assert exit_code == ExitCode.LOG_ERRORS


@patch("builtins.open", new_callable=mock_open)
@patch("soda_core.cli.handlers.soda_cloud.Path.mkdir")
@patch("soda_core.cli.handlers.soda_cloud.exists", return_value=False)
def test_create_soda_cloud_success(mock_exists, mock_mkdir, mock_file):
    exit_code = handle_create_soda_cloud("sc.yaml")
    assert exit_code == ExitCode.OK
    mock_file.assert_called_once_with("sc.yaml", "w")
    mock_mkdir.assert_called()


@patch("soda_core.cli.handlers.soda_cloud.exists", return_value=False)
@patch("soda_core.cli.handlers.soda_cloud.Path.mkdir", side_effect=Exception("fail"))
def test_create_soda_cloud_unexpected_exception(mock_mkdir, mock_exists):
    exit_code = handle_create_soda_cloud("sc.yaml")
    assert exit_code == ExitCode.LOG_ERRORS


@patch("soda_core.common.soda_cloud.SodaCloud")
def test_test_soda_cloud_success(mock_soda_cloud_cls):
    mock_soda_cloud = MagicMock()
    mock_soda_cloud.test_connection.return_value = None
    mock_soda_cloud_cls.from_file.return_value = mock_soda_cloud

    # Patch Logs.has_errors to return False (no errors)
    with patch("soda_core.common.soda_cloud.Logs.has_errors", return_value=False):
        exit_code = handle_test_soda_cloud("sc.yaml")

    assert exit_code == ExitCode.OK


@patch("soda_core.common.soda_cloud.SodaCloud")
def test_test_soda_cloud_connection_failure(mock_soda_cloud_cls):
    mock_soda_cloud = MagicMock()
    mock_soda_cloud.test_connection.return_value = None
    mock_soda_cloud_cls.from_file.return_value = mock_soda_cloud

    with patch("soda_core.cli.handlers.soda_cloud.Logs.has_errors", return_value=True), patch(
        "soda_core.cli.handlers.soda_cloud.Logs.get_errors_str", return_value="boom"
    ):
        exit_code = handle_test_soda_cloud("sc.yaml")

    assert exit_code == ExitCode.LOG_ERRORS


@patch("soda_core.cli.handlers.soda_cloud.YamlSource")
def test_test_soda_cloud_returns_none(mock_yaml_source_cls):
    mock_yaml_source = MagicMock()
    mock_yaml_source.parse_yaml_file_content.return_value = "parsed_yaml"
    mock_yaml_source_cls.from_file_path.return_value = mock_yaml_source

    # Simulate SodaCloud.from_file returning None
    with patch("soda_core.common.soda_cloud.SodaCloud.from_yaml_source", return_value=None):
        exit_code = handle_test_soda_cloud("sc.yaml")

    assert exit_code == ExitCode.LOG_ERRORS
