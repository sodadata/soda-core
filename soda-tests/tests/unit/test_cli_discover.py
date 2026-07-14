import logging
import sys
from unittest.mock import MagicMock, patch

import pytest
from soda_core.cli.cli import create_cli_parser
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.failure_reporting import ScanExecutionFailedException
from soda_core.common.logs import Logs


@patch("soda_core.cli.cli.handle_discover_data_source")
@patch("soda_core.cli.cli.run_with_failure_reporting")
@patch("soda_core.cli.cli.resolve_data_source")
@patch("soda_core.cli.cli.resolve_soda_cloud")
def test_cli_arg_mapping_for_data_source_discover(
    mock_resolve_soda_cloud, mock_resolve_data_source, mock_run_with_failure_reporting, mock_handler
):
    # The wiring resolves the reporting channel first, passes it to the wrapper,
    # and resolves the data source inside the wrapped command; the fake wrapper
    # just runs the command so the handler call can be asserted.
    data_source_impl = MagicMock()
    soda_cloud = MagicMock()
    mock_resolve_soda_cloud.return_value = soda_cloud
    mock_resolve_data_source.return_value = data_source_impl
    mock_run_with_failure_reporting.side_effect = lambda soda_cloud, command: command(Logs())
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
    mock_resolve_soda_cloud.assert_called_once_with("cloud.yaml")
    mock_resolve_data_source.assert_called_once_with("ds.yaml")
    run_args, _ = mock_run_with_failure_reporting.call_args
    assert run_args[0] is soda_cloud
    mock_handler.assert_called_once_with(
        data_source_impl,
        soda_cloud,
        include=["cust%"],
        exclude=["tmp%"],
        scan_definition_name="my_scan",
    )


# Cloud-flow guard: the reporting channel resolves before the wrapper. When it
# is unusable, neither results nor a failure report can reach Cloud: exit 4 so
# the managed launcher's fallback marks the scan failed.


@patch("soda_core.cli.cli.run_with_failure_reporting")
@patch("soda_core.cli.cli.resolve_data_source")
@patch(
    "soda_core.cli.cli.resolve_soda_cloud",
    side_effect=ScanExecutionFailedException("A Soda Cloud configuration file (-sc) is required."),
)
def test_cli_discover_with_unusable_soda_cloud_exits_results_not_sent(
    mock_resolve_soda_cloud, mock_resolve_data_source, mock_run_with_failure_reporting, caplog
):
    sys.argv = ["soda", "data-source", "discover", "-ds", "ds.yaml", "-sc", "cloud.yaml"]
    args = create_cli_parser().parse_args()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    failure_records = [r for r in caplog.records if "configuration file (-sc) is required" in r.getMessage()]
    assert len(failure_records) == 1
    assert failure_records[0].exc_info is None
    # Reorder proof: the reporting channel resolves first — the data source is
    # never resolved and the wrapper never runs.
    mock_resolve_data_source.assert_not_called()
    mock_run_with_failure_reporting.assert_not_called()


@patch("soda_core.cli.cli.run_with_failure_reporting")
@patch("soda_core.cli.cli.resolve_data_source")
@patch("soda_core.cli.cli.resolve_soda_cloud", side_effect=RuntimeError("boom"))
def test_cli_discover_with_soda_cloud_resolution_raising_raw_exits_results_not_sent(
    mock_resolve_soda_cloud, mock_resolve_data_source, mock_run_with_failure_reporting, caplog
):
    sys.argv = ["soda", "data-source", "discover", "-ds", "ds.yaml", "-sc", "cloud.yaml"]
    args = create_cli_parser().parse_args()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    # Unexpected failures are logged with the traceback.
    failure_records = [r for r in caplog.records if "boom" in r.getMessage()]
    assert len(failure_records) == 1
    assert failure_records[0].exc_info is not None
    mock_run_with_failure_reporting.assert_not_called()


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
@patch(
    "soda_core.cli.cli.resolve_data_source",
    side_effect=ScanExecutionFailedException("Data source could not be created."),
)
def test_cli_discover_locally_with_unusable_data_source_exits_log_errors(
    mock_resolve_data_source, mock_local_handler, monkeypatch, caplog
):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)

    args = _parse_discover_args_without_soda_cloud()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    # Ad-hoc semantics: no Cloud scan to mark, the message stays on the console.
    assert e.value.code == ExitCode.LOG_ERRORS
    assert any("Data source could not be created" in record.getMessage() for record in caplog.records)
    mock_local_handler.assert_not_called()


@patch("soda_core.cli.cli.handle_discover_data_source_locally", side_effect=RuntimeError("boom"))
@patch("soda_core.cli.cli.resolve_data_source")
def test_cli_discover_locally_with_raising_handler_exits_log_errors(
    mock_resolve_data_source, mock_local_handler, monkeypatch, caplog
):
    # The local handler propagates failures raw; this wiring is the single
    # logging site (with the traceback) and maps them to LOG_ERRORS.
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    mock_resolve_data_source.return_value = MagicMock()

    args = _parse_discover_args_without_soda_cloud()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == ExitCode.LOG_ERRORS
    failure_records = [record for record in caplog.records if "boom" in record.getMessage()]
    assert len(failure_records) == 1
    assert failure_records[0].exc_info is not None


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
@patch("soda_core.cli.cli.resolve_soda_cloud")
def test_cli_discover_with_scan_id_and_soda_cloud_takes_cloud_flow(
    mock_resolve_soda_cloud, mock_run_with_failure_reporting, mock_handler, monkeypatch
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


@patch("soda_core.cli.cli.handle_discover_data_source_locally", return_value=ExitCode.OK)
@patch("soda_core.cli.cli.resolve_data_source")
def test_cli_discover_locally_warns_when_scan_definition_name_provided(
    mock_resolve_data_source, mock_local_handler, monkeypatch, caplog
):
    # --scan-definition-name names the Soda Cloud scan definition: in a local run
    # it has nothing to apply to, so the wiring warns instead of silently dropping it.
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    mock_resolve_data_source.return_value = MagicMock()
    sys.argv = ["soda", "data-source", "discover", "-ds", "ds.yaml", "--scan-definition-name", "my_scan"]

    args = create_cli_parser().parse_args()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == ExitCode.OK
    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert any("--scan-definition-name is ignored in a local run" in record.getMessage() for record in warnings)


# Cloud flow with a missing scan definition name (no --scan-definition-name, no
# SODA_SCAN_DEFINITION): resolution happens inside the wrapped command, so the
# raise takes the standard failure mapping — managed scans get marked failed
# with the captured log records, ad-hoc runs exit LOG_ERRORS.


def _run_discover_cloud_flow_without_scan_definition_name():
    sys.argv = ["soda", "data-source", "discover", "-ds", "ds.yaml", "-sc", "cloud.yaml"]
    args = create_cli_parser().parse_args()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)
    return e.value.code


@patch("soda_core.discovery.discovery_run.DiscoveryRun")
@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_cli_discover_cloud_flow_without_scan_definition_name_marks_managed_scan_failed(
    mock_data_source_impl_cls,
    mock_soda_cloud_cls,
    mock_sc_yaml_source_cls,
    mock_ds_yaml_source_cls,
    mock_discovery_run_cls,
    monkeypatch,
):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    monkeypatch.delenv("SODA_SCAN_DEFINITION", raising=False)
    mock_soda_cloud = MagicMock()
    mock_soda_cloud.mark_scan_as_failed.return_value = True
    mock_soda_cloud_cls.from_yaml_source.return_value = mock_soda_cloud
    mock_data_source_impl_cls.from_yaml_source.return_value = MagicMock()

    exit_code = _run_discover_cloud_flow_without_scan_definition_name()

    assert exit_code == ExitCode.LOG_ERRORS
    mock_soda_cloud.mark_scan_as_failed.assert_called_once()
    _, kwargs = mock_soda_cloud.mark_scan_as_failed.call_args
    assert kwargs["scan_id"] == "scan-123"
    assert any("scan definition name is required" in record.getMessage() for record in kwargs["logs"])
    # The name resolves before the handler runs: no discovery query is attempted.
    mock_discovery_run_cls.execute.assert_not_called()


@patch("soda_core.cli.handlers.dependencies.DataSourceYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloudYamlSource")
@patch("soda_core.cli.handlers.dependencies.SodaCloud")
@patch("soda_core.common.data_source_impl.DataSourceImpl")
def test_cli_discover_cloud_flow_without_scan_definition_name_ad_hoc_exits_log_errors(
    mock_data_source_impl_cls,
    mock_soda_cloud_cls,
    mock_sc_yaml_source_cls,
    mock_ds_yaml_source_cls,
    monkeypatch,
    caplog,
):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    monkeypatch.delenv("SODA_SCAN_DEFINITION", raising=False)
    mock_soda_cloud = MagicMock()
    mock_soda_cloud_cls.from_yaml_source.return_value = mock_soda_cloud
    mock_data_source_impl_cls.from_yaml_source.return_value = MagicMock()

    exit_code = _run_discover_cloud_flow_without_scan_definition_name()

    # Ad-hoc run: no Cloud scan to update, the error stays on the console.
    assert exit_code == ExitCode.LOG_ERRORS
    mock_soda_cloud.mark_scan_as_failed.assert_not_called()
    assert any("scan definition name is required" in record.getMessage() for record in caplog.records)


@patch("soda_core.cli.cli.handle_discover_data_source", return_value=ExitCode.OK)
@patch("soda_core.cli.cli.run_with_failure_reporting")
@patch("soda_core.cli.cli.resolve_data_source")
@patch("soda_core.cli.cli.resolve_soda_cloud")
def test_cli_discover_cloud_flow_resolves_scan_definition_name_from_env(
    mock_resolve_soda_cloud, mock_resolve_data_source, mock_run_with_failure_reporting, mock_handler, monkeypatch
):
    monkeypatch.setenv("SODA_SCAN_DEFINITION", "env_scan_def")
    mock_run_with_failure_reporting.side_effect = lambda soda_cloud, command: command(Logs())
    sys.argv = ["soda", "data-source", "discover", "-ds", "ds.yaml", "-sc", "cloud.yaml"]

    args = create_cli_parser().parse_args()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == ExitCode.OK
    _, handler_kwargs = mock_handler.call_args
    assert handler_kwargs["scan_definition_name"] == "env_scan_def"
