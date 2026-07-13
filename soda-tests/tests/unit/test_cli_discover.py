import sys
from unittest.mock import MagicMock, patch

import pytest
from soda_core.cli.cli import create_cli_parser
from soda_core.cli.exit_codes import ExitCode


@patch("soda_core.cli.cli.handle_discover_data_source")
@patch("soda_core.cli.cli.run_with_failure_reporting")
def test_cli_arg_mapping_for_data_source_discover(mock_run_with_failure_reporting, mock_handler):
    # The wiring resolves the dependencies and passes them to the handler; the fake
    # wrapper hands sentinels to the command so the handler call can be asserted.
    data_source_impl = MagicMock()
    soda_cloud = MagicMock()
    mock_run_with_failure_reporting.side_effect = lambda data_source_file_path, soda_cloud_file_path, command: command(
        data_source_impl, soda_cloud
    )
    mock_handler.return_value = ExitCode.OK
    sys.argv = [
        "soda",
        "data-source",
        "discover",
        "-ds",
        "ds.yaml",
        "--include",
        "cust%",
        "--exclude",
        "tmp%",
        "--scan-definition-name",
        "my_scan",
        "-sc",
        "cloud.yaml",
    ]
    args = create_cli_parser().parse_args()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)
    assert e.value.code == ExitCode.OK
    _, run_kwargs = mock_run_with_failure_reporting.call_args
    assert run_kwargs["data_source_file_path"] == "ds.yaml"
    assert run_kwargs["soda_cloud_file_path"] == "cloud.yaml"
    mock_handler.assert_called_once_with(
        data_source_impl,
        soda_cloud,
        include=["cust%"],
        exclude=["tmp%"],
        scan_definition_name="my_scan",
    )


# Local flow wiring: without -sc, discovery runs locally — the data source is
# resolved with ad-hoc semantics (no Cloud scan to mark) and Soda Cloud is never
# touched. Exception: a managed scan (SODA_SCAN_ID set) without -sc is a
# launcher misconfiguration and exits RESULTS_NOT_SENT_TO_CLOUD (4) so the
# launcher's fallback marks the scan failed.


def _parse_discover_args_without_soda_cloud():
    sys.argv = ["soda", "data-source", "discover", "-ds", "ds.yaml", "--include", "cust%", "--exclude", "tmp%"]
    return create_cli_parser().parse_args()


@patch("soda_core.common.soda_cloud.SodaCloud.from_yaml_source")
@patch("soda_core.cli.cli.handle_discover_data_source_locally")
@patch("soda_core.cli.cli.resolve_data_source")
def test_cli_discover_without_soda_cloud_runs_locally(
    mock_resolve_data_source, mock_local_handler, mock_soda_cloud_from_yaml_source, monkeypatch
):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    data_source_impl = MagicMock()
    mock_resolve_data_source.return_value = data_source_impl
    mock_local_handler.return_value = ExitCode.OK

    args = _parse_discover_args_without_soda_cloud()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == ExitCode.OK
    mock_resolve_data_source.assert_called_once_with("ds.yaml")
    mock_local_handler.assert_called_once_with(data_source_impl, include=["cust%"], exclude=["tmp%"])
    # The local flow never constructs a SodaCloud.
    mock_soda_cloud_from_yaml_source.assert_not_called()


@patch("soda_core.cli.cli.handle_discover_data_source_locally")
@patch("soda_core.cli.cli.resolve_data_source", return_value=None)
def test_cli_discover_locally_with_unusable_data_source_exits_log_errors(
    mock_resolve_data_source, mock_local_handler, monkeypatch
):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)

    args = _parse_discover_args_without_soda_cloud()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    # Ad-hoc semantics: no Cloud scan to mark, errors stay on the console.
    assert e.value.code == ExitCode.LOG_ERRORS
    mock_local_handler.assert_not_called()


@patch("soda_core.cli.cli.handle_discover_data_source_locally", side_effect=RuntimeError("boom"))
@patch("soda_core.cli.cli.resolve_data_source")
def test_cli_discover_locally_with_raising_handler_exits_log_errors(
    mock_resolve_data_source, mock_local_handler, monkeypatch, caplog
):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    mock_resolve_data_source.return_value = MagicMock()

    args = _parse_discover_args_without_soda_cloud()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == ExitCode.LOG_ERRORS
    assert any("boom" in record.getMessage() for record in caplog.records)


@patch("soda_core.cli.cli.handle_discover_data_source_locally")
@patch("soda_core.cli.cli.resolve_data_source")
def test_cli_discover_with_scan_id_but_without_soda_cloud_exits_results_not_sent(
    mock_resolve_data_source, mock_local_handler, monkeypatch, caplog
):
    # A managed scan must publish to Soda Cloud: a missing -sc is a launcher
    # misconfiguration, not a local run. Exit 4 so the launcher fallback marks
    # the scan failed.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")

    args = _parse_discover_args_without_soda_cloud()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    assert any(
        "SODA_SCAN_ID is set but no Soda Cloud configuration" in record.getMessage() for record in caplog.records
    )
    mock_resolve_data_source.assert_not_called()
    mock_local_handler.assert_not_called()


@patch("soda_core.cli.cli.handle_discover_data_source")
@patch("soda_core.cli.cli.run_with_failure_reporting", return_value=ExitCode.OK)
def test_cli_discover_with_scan_id_and_soda_cloud_takes_cloud_flow(
    mock_run_with_failure_reporting, mock_handler, monkeypatch
):
    # The misconfiguration guard only applies without -sc: a managed scan with a
    # cloud config goes through the regular Cloud flow.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    sys.argv = ["soda", "data-source", "discover", "-ds", "ds.yaml", "-sc", "cloud.yaml"]

    args = create_cli_parser().parse_args()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == ExitCode.OK
    mock_run_with_failure_reporting.assert_called_once()
