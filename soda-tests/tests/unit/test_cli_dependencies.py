from unittest.mock import MagicMock, patch

from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.dependencies import (
    resolve_data_source,
    resolve_soda_cloud,
    run_with_failure_reporting,
)
from soda_core.cli.handlers.failure_reporting import ScanExecutionFailedException
from soda_core.common.exceptions import InvalidSodaCloudConfigurationException
from soda_core.common.logging_constants import soda_logger

# resolve_soda_cloud: None means unusable (reason logged), whether the parse
# returns None or raises.


def test_resolve_soda_cloud_without_file_path_returns_none():
    assert resolve_soda_cloud(None) is None


@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
def test_resolve_soda_cloud_returns_parsed_instance(mock_soda_cloud_cls, mock_sc_yaml_source_cls):
    soda_cloud = MagicMock()
    mock_soda_cloud_cls.from_yaml_source.return_value = soda_cloud

    assert resolve_soda_cloud("sc.yaml") is soda_cloud


@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
def test_resolve_soda_cloud_with_unparseable_config_returns_none(mock_soda_cloud_cls, mock_sc_yaml_source_cls):
    mock_soda_cloud_cls.from_yaml_source.return_value = None

    assert resolve_soda_cloud("sc.yaml") is None


@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
def test_resolve_soda_cloud_with_raising_parse_returns_none(mock_soda_cloud_cls, mock_sc_yaml_source_cls):
    mock_soda_cloud_cls.from_yaml_source.side_effect = InvalidSodaCloudConfigurationException(
        "Missing required 'api_key_id' property"
    )

    assert resolve_soda_cloud("sc.yaml") is None


# resolve_data_source: None means the data source could not be created (reason
# logged), whether the parse returns None or raises.


def test_resolve_data_source_without_file_path_returns_none():
    assert resolve_data_source(None) is None


@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_resolve_data_source_returns_created_instance(mock_data_source_impl_cls, mock_ds_yaml_source_cls):
    data_source_impl = MagicMock()
    mock_data_source_impl_cls.from_yaml_source.return_value = data_source_impl

    assert resolve_data_source("ds.yaml") is data_source_impl


@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_resolve_data_source_with_unparseable_config_returns_none(mock_data_source_impl_cls, mock_ds_yaml_source_cls):
    mock_data_source_impl_cls.from_yaml_source.return_value = None

    assert resolve_data_source("ds.yaml") is None


@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_resolve_data_source_with_raising_creation_returns_none(mock_data_source_impl_cls, mock_ds_yaml_source_cls):
    mock_data_source_impl_cls.from_yaml_source.side_effect = ImportError("Data source type 'postgres' not available")

    assert resolve_data_source("ds.yaml") is None


# run_with_failure_reporting: the ONE place where resolution and execution
# failures map to exit codes and Soda Cloud failure reporting.


@patch("soda_core.cli.handlers.dependencies.resolve_data_source")
@patch("soda_core.cli.handlers.dependencies.resolve_soda_cloud", return_value=None)
def test_run_with_unusable_soda_cloud_exits_results_not_sent_before_data_source_resolution(
    mock_resolve_soda_cloud, mock_resolve_data_source
):
    command = MagicMock()

    exit_code = run_with_failure_reporting("ds.yaml", "sc.yaml", command)

    # Nothing can reach Cloud without a usable cloud config: exit > 3 so the
    # managed launcher's fallback marks the scan failed.
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    # Reorder proof: the Soda Cloud config is resolved first; the data source
    # is never resolved and the command never runs.
    mock_resolve_data_source.assert_not_called()
    command.assert_not_called()


@patch("soda_core.cli.handlers.dependencies.resolve_data_source")
@patch("soda_core.cli.handlers.dependencies.resolve_soda_cloud")
def test_run_with_unresolvable_data_source_marks_scan_failed_with_captured_records(
    mock_resolve_soda_cloud, mock_resolve_data_source, monkeypatch
):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.mark_scan_as_failed.return_value = True
    mock_resolve_soda_cloud.return_value = soda_cloud

    def log_and_fail(data_source_file_path):
        soda_logger.error("Data source could not be created")
        return None

    mock_resolve_data_source.side_effect = log_and_fail
    command = MagicMock()

    exit_code = run_with_failure_reporting("ds.yaml", "sc.yaml", command)

    assert exit_code == ExitCode.LOG_ERRORS
    command.assert_not_called()
    soda_cloud.mark_scan_as_failed.assert_called_once()
    _, kwargs = soda_cloud.mark_scan_as_failed.call_args
    assert kwargs["scan_id"] == "scan-123"
    # The Logs collector starts before resolution, so resolution failures are captured too.
    assert any("Data source could not be created" in record.getMessage() for record in kwargs["logs"])


@patch("soda_core.cli.handlers.dependencies.resolve_data_source", return_value=None)
@patch("soda_core.cli.handlers.dependencies.resolve_soda_cloud")
def test_run_with_unresolvable_data_source_without_scan_id_exits_log_errors(
    mock_resolve_soda_cloud, mock_resolve_data_source, monkeypatch
):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    soda_cloud = MagicMock()
    mock_resolve_soda_cloud.return_value = soda_cloud

    exit_code = run_with_failure_reporting("ds.yaml", "sc.yaml", MagicMock())

    # Ad-hoc run: no Cloud scan to update, errors stay on the console.
    assert exit_code == ExitCode.LOG_ERRORS
    soda_cloud.mark_scan_as_failed.assert_not_called()


@patch("soda_core.cli.handlers.dependencies.resolve_data_source")
@patch("soda_core.cli.handlers.dependencies.resolve_soda_cloud")
def test_run_with_command_raising_scan_execution_failed_marks_scan_failed(
    mock_resolve_soda_cloud, mock_resolve_data_source, monkeypatch
):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.mark_scan_as_failed.return_value = True
    mock_resolve_soda_cloud.return_value = soda_cloud
    mock_resolve_data_source.return_value = MagicMock()

    def command(data_source_impl, soda_cloud):
        # Convention: the handler logs the failure with its domain context, then raises.
        soda_logger.error("Discovery query failed: connection dropped")
        raise ScanExecutionFailedException("Discovery query failed: connection dropped")

    exit_code = run_with_failure_reporting("ds.yaml", "sc.yaml", command)

    assert exit_code == ExitCode.LOG_ERRORS
    soda_cloud.mark_scan_as_failed.assert_called_once()
    _, kwargs = soda_cloud.mark_scan_as_failed.call_args
    assert kwargs["scan_id"] == "scan-123"
    assert any("Discovery query failed" in record.getMessage() for record in kwargs["logs"])


@patch("soda_core.cli.handlers.dependencies.resolve_data_source")
@patch("soda_core.cli.handlers.dependencies.resolve_soda_cloud")
def test_run_with_command_raising_unexpected_exception_marks_scan_failed(
    mock_resolve_soda_cloud, mock_resolve_data_source, monkeypatch
):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.mark_scan_as_failed.return_value = True
    mock_resolve_soda_cloud.return_value = soda_cloud
    mock_resolve_data_source.return_value = MagicMock()
    command = MagicMock(side_effect=RuntimeError("payload build failed"))

    exit_code = run_with_failure_reporting("ds.yaml", "sc.yaml", command)

    assert exit_code == ExitCode.LOG_ERRORS
    soda_cloud.mark_scan_as_failed.assert_called_once()
    _, kwargs = soda_cloud.mark_scan_as_failed.call_args
    # Unexpected exceptions are logged here before reporting, so the records carry them.
    assert any("payload build failed" in record.getMessage() for record in kwargs["logs"])


@patch("soda_core.cli.handlers.dependencies.resolve_data_source")
@patch("soda_core.cli.handlers.dependencies.resolve_soda_cloud")
def test_run_with_rejected_mark_scan_as_failed_exits_results_not_sent(
    mock_resolve_soda_cloud, mock_resolve_data_source, monkeypatch
):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.mark_scan_as_failed.return_value = False
    mock_resolve_soda_cloud.return_value = soda_cloud
    mock_resolve_data_source.return_value = MagicMock()
    command = MagicMock(side_effect=RuntimeError("boom"))

    exit_code = run_with_failure_reporting("ds.yaml", "sc.yaml", command)

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


@patch("soda_core.cli.handlers.dependencies.resolve_data_source")
@patch("soda_core.cli.handlers.dependencies.resolve_soda_cloud")
def test_run_passes_resolved_dependencies_and_returns_command_exit_code(
    mock_resolve_soda_cloud, mock_resolve_data_source
):
    soda_cloud = MagicMock()
    data_source_impl = MagicMock()
    mock_resolve_soda_cloud.return_value = soda_cloud
    mock_resolve_data_source.return_value = data_source_impl
    command = MagicMock(return_value=ExitCode.OK)

    exit_code = run_with_failure_reporting("ds.yaml", "sc.yaml", command)

    assert exit_code == ExitCode.OK
    command.assert_called_once_with(data_source_impl, soda_cloud)


@patch("soda_core.cli.handlers.dependencies.resolve_data_source")
@patch("soda_core.cli.handlers.dependencies.resolve_soda_cloud")
def test_run_passes_through_command_failure_exit_code_without_reporting(
    mock_resolve_soda_cloud, mock_resolve_data_source, monkeypatch
):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    mock_resolve_soda_cloud.return_value = soda_cloud
    mock_resolve_data_source.return_value = MagicMock()
    # E.g. a rejected results upload: the command decides the exit code itself.
    command = MagicMock(return_value=ExitCode.RESULTS_NOT_SENT_TO_CLOUD)

    exit_code = run_with_failure_reporting("ds.yaml", "sc.yaml", command)

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    soda_cloud.mark_scan_as_failed.assert_not_called()
