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
