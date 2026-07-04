import sys
from unittest.mock import patch

import pytest
from soda_core.cli.cli import create_cli_parser
from soda_core.cli.exit_codes import ExitCode


@patch("soda_core.cli.cli.handle_discover_data_source")
def test_cli_arg_mapping_for_data_source_discover(mock_handler):
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
    mock_handler.assert_called_once_with("ds.yaml", ["cust%"], ["tmp%"], "my_scan", soda_cloud_file_path="cloud.yaml")
