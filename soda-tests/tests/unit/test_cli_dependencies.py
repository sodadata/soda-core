from unittest.mock import MagicMock, patch

import pytest
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.dependencies import (
    resolve_data_source,
    resolve_soda_cloud,
    run_with_failure_reporting,
)
from soda_core.cli.handlers.failure_reporting import ScanExecutionFailedException
from soda_core.common.exceptions import InvalidSodaCloudConfigurationException

# resolve_soda_cloud: expected/unusable-configuration shapes raise
# ScanExecutionFailedException carrying the user-facing message — nothing is
# logged at the raise site, the caller owns the logging.


def test_resolve_soda_cloud_without_file_path_raises():
    with pytest.raises(ScanExecutionFailedException, match=r"configuration file \(-sc\) is required"):
        resolve_soda_cloud(None)


@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
def test_resolve_soda_cloud_returns_parsed_instance(mock_soda_cloud_cls, mock_sc_yaml_source_cls):
    soda_cloud = MagicMock()
    mock_soda_cloud_cls.from_yaml_source.return_value = soda_cloud

    assert resolve_soda_cloud("sc.yaml") is soda_cloud


@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
def test_resolve_soda_cloud_with_unparseable_config_raises(mock_soda_cloud_cls, mock_sc_yaml_source_cls):
    mock_soda_cloud_cls.from_yaml_source.return_value = None

    with pytest.raises(ScanExecutionFailedException, match="could not be parsed"):
        resolve_soda_cloud("sc.yaml")


@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
def test_resolve_soda_cloud_with_invalid_config_raises_with_cause(mock_soda_cloud_cls, mock_sc_yaml_source_cls, caplog):
    original = InvalidSodaCloudConfigurationException("Missing required 'api_key_id' property")
    mock_soda_cloud_cls.from_yaml_source.side_effect = original

    with pytest.raises(ScanExecutionFailedException, match="api_key_id") as excinfo:
        resolve_soda_cloud("sc.yaml")

    assert excinfo.value.__cause__ is original
    # Nothing is logged at the raise site: the caller owns the logging.
    assert not any("api_key_id" in record.getMessage() for record in caplog.records)


@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
def test_resolve_soda_cloud_with_unexpected_failure_propagates_raw(mock_soda_cloud_cls, mock_sc_yaml_source_cls):
    mock_soda_cloud_cls.from_yaml_source.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        resolve_soda_cloud("sc.yaml")


# The two most common user mistakes — a YAML syntax error and a wrong file
# path — raise YamlParserException from the parsing layer and must take the
# clean-message path, not the raw-traceback path. Exercised with real files.


def test_resolve_soda_cloud_with_yaml_syntax_error_raises_clean(tmp_path):
    config_file = tmp_path / "sc.yml"
    config_file.write_text("soda_cloud: [unclosed")
    config_file_path = str(config_file)

    with pytest.raises(ScanExecutionFailedException, match="could not be parsed"):
        resolve_soda_cloud(config_file_path)


def test_resolve_soda_cloud_with_missing_file_raises_clean(tmp_path):
    config_file_path = str(tmp_path / "does_not_exist.yml")

    with pytest.raises(ScanExecutionFailedException, match="could not be parsed"):
        resolve_soda_cloud(config_file_path)


def test_resolve_soda_cloud_without_soda_cloud_key_raises_clean(tmp_path):
    # Root-fixed in SodaCloud.from_yaml_source: a missing top-level 'soda_cloud'
    # key raises InvalidSodaCloudConfigurationException (previously an
    # AttributeError on None), which resolves to a clean message here.
    config_file = tmp_path / "sc.yml"
    config_file.write_text("something_else: 1\n")
    config_file_path = str(config_file)

    with pytest.raises(ScanExecutionFailedException, match="Missing required 'soda_cloud' top-level key"):
        resolve_soda_cloud(config_file_path)


def test_resolve_data_source_with_yaml_syntax_error_raises_clean(tmp_path):
    config_file = tmp_path / "ds.yml"
    config_file.write_text("type: [unclosed")
    config_file_path = str(config_file)

    with pytest.raises(ScanExecutionFailedException, match="could not be created"):
        resolve_data_source(config_file_path)


def test_resolve_data_source_with_missing_file_raises_clean(tmp_path):
    config_file_path = str(tmp_path / "does_not_exist.yml")

    with pytest.raises(ScanExecutionFailedException, match="could not be created"):
        resolve_data_source(config_file_path)


# resolve_data_source: same exception contract; environment problems (e.g. a
# missing plugin) propagate raw so the caller logs the traceback.


def test_resolve_data_source_without_file_path_raises():
    with pytest.raises(ScanExecutionFailedException, match=r"configuration file \(-ds\) is required"):
        resolve_data_source(None)


@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_resolve_data_source_returns_created_instance(mock_data_source_impl_cls, mock_ds_yaml_source_cls):
    data_source_impl = MagicMock()
    mock_data_source_impl_cls.from_yaml_source.return_value = data_source_impl

    assert resolve_data_source("ds.yaml") is data_source_impl


@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_resolve_data_source_with_unparseable_config_raises(mock_data_source_impl_cls, mock_ds_yaml_source_cls):
    mock_data_source_impl_cls.from_yaml_source.return_value = None

    with pytest.raises(ScanExecutionFailedException, match="could not be created"):
        resolve_data_source("ds.yaml")


@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_resolve_data_source_with_invalid_config_raises_with_cause(
    mock_data_source_impl_cls, mock_ds_yaml_source_cls, caplog
):
    original = ValueError("Missing required 'type' in data source YAML")
    mock_data_source_impl_cls.from_yaml_source.side_effect = original

    with pytest.raises(ScanExecutionFailedException, match="Missing required 'type'") as excinfo:
        resolve_data_source("ds.yaml")

    assert excinfo.value.__cause__ is original
    # Nothing is logged at the raise site: the caller owns the logging.
    assert not any("Missing required 'type'" in record.getMessage() for record in caplog.records)


@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_resolve_data_source_with_missing_plugin_propagates_raw(mock_data_source_impl_cls, mock_ds_yaml_source_cls):
    # An environment problem, not a config mistake: propagates raw so the
    # caller logs it with the traceback, which helps there.
    mock_data_source_impl_cls.from_yaml_source.side_effect = ImportError("Data source type 'postgres' not available")

    with pytest.raises(ImportError, match="not available"):
        resolve_data_source("ds.yaml")


# run_with_failure_reporting: receives the already-constructed reporting
# channel and a zero-arg command; the ONE place where command failures map to
# exit codes and Soda Cloud failure reporting.


def test_run_with_command_raising_scan_execution_failed_marks_scan_failed(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.mark_scan_as_failed.return_value = True

    def command():
        # Convention: an expected/validation failure raises with a user-facing
        # message and logs nothing — the wrapper is the single logging site.
        raise ScanExecutionFailedException("Discovery query failed: connection dropped")

    exit_code = run_with_failure_reporting(soda_cloud, command)

    assert exit_code == ExitCode.LOG_ERRORS
    soda_cloud.mark_scan_as_failed.assert_called_once()
    _, kwargs = soda_cloud.mark_scan_as_failed.call_args
    assert kwargs["scan_id"] == "scan-123"
    # Logged exactly once, clean (no traceback), and captured into the report.
    failure_records = [record for record in kwargs["logs"] if "Discovery query failed" in record.getMessage()]
    assert len(failure_records) == 1
    assert failure_records[0].exc_info is None


def test_run_with_command_raising_unexpected_exception_marks_scan_failed(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.mark_scan_as_failed.return_value = True
    command = MagicMock(side_effect=RuntimeError("payload build failed"))

    exit_code = run_with_failure_reporting(soda_cloud, command)

    assert exit_code == ExitCode.LOG_ERRORS
    soda_cloud.mark_scan_as_failed.assert_called_once()
    _, kwargs = soda_cloud.mark_scan_as_failed.call_args
    # Unexpected exceptions are logged here with the traceback before reporting,
    # so the records carry them.
    failure_records = [record for record in kwargs["logs"] if "payload build failed" in record.getMessage()]
    assert len(failure_records) == 1
    assert failure_records[0].exc_info is not None


def test_run_with_rejected_mark_scan_as_failed_exits_results_not_sent(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.mark_scan_as_failed.return_value = False
    command = MagicMock(side_effect=RuntimeError("boom"))

    exit_code = run_with_failure_reporting(soda_cloud, command)

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_run_with_failing_command_without_scan_id_exits_log_errors(monkeypatch):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    soda_cloud = MagicMock()
    command = MagicMock(side_effect=ScanExecutionFailedException("no scan definition name"))

    exit_code = run_with_failure_reporting(soda_cloud, command)

    # Ad-hoc run: no Cloud scan to update, errors stay on the console.
    assert exit_code == ExitCode.LOG_ERRORS
    soda_cloud.mark_scan_as_failed.assert_not_called()


def test_run_returns_command_exit_code():
    soda_cloud = MagicMock()
    command = MagicMock(return_value=ExitCode.OK)

    exit_code = run_with_failure_reporting(soda_cloud, command)

    assert exit_code == ExitCode.OK
    command.assert_called_once_with()


def test_run_passes_through_command_failure_exit_code_without_reporting(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    # E.g. a rejected results upload: the command decides the exit code itself.
    command = MagicMock(return_value=ExitCode.RESULTS_NOT_SENT_TO_CLOUD)

    exit_code = run_with_failure_reporting(soda_cloud, command)

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    soda_cloud.mark_scan_as_failed.assert_not_called()
