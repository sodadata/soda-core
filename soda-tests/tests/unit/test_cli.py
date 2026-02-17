import sys
from unittest.mock import patch

import pytest
from soda_core.cli.cli import create_cli_parser, execute
from soda_core.cli.exit_codes import ExitCode
from soda_core.common.logs import Logs

# from soda_core.cli.soda import CLI


@pytest.mark.parametrize(
    "args, expected",
    [
        (
            [
                "soda",
                "contract",
                "verify",
                "-c",
                "a.yaml",
                "-d",
                "some/remote/dataset/identifier",
                "-ds",
                "ds.yaml",
                "-sc",
                "cloud.yaml",
                "-a",
                "-btm",
                "42",
                "-p",
                "-v",
                "--set",
                "key1=value1",
                "--set",
                "key2=value2",
            ],
            [
                "a.yaml",
                "some/remote/dataset/identifier",
                ["ds.yaml"],
                "cloud.yaml",
                {"key1": "value1", "key2": "value2"},
                True,
                True,
                True,
                42,
                None,
                None,
            ],
        ),
        (
            [
                "soda",
                "contract",
                "verify",
                "-d",
                "some-dataset",
                "-ds",
                "ds.yaml",
                "-sc",
                "cloud.yaml",
                "-a",
                "-btm",
                "42",
                "-p",
                "-v",
                "--set",
                "key1=value1",
            ],
            [
                None,
                "some-dataset",
                ["ds.yaml"],
                "cloud.yaml",
                {"key1": "value1"},
                True,
                True,
                True,
                42,
                None,
                None,
            ],
        ),
        (
            [
                "soda",
                "contract",
                "verify",
                "-d",
                "some-dataset",
                "-ds",
                "ds.yaml",
                "-sc",
                "cloud.yaml",
            ],
            [None, "some-dataset", ["ds.yaml"], "cloud.yaml", {}, False, False, False, 60, None, None],
        ),
        (
            [
                "soda",
                "contract",
                "verify",
                "-d",
                "some-dataset",
                "-ds",
                "ds.yaml",
                "-sc",
                "cloud.yaml",
                "-dw",
                "diagnostics_warehouse.yaml",
            ],
            [
                None,
                "some-dataset",
                ["ds.yaml"],
                "cloud.yaml",
                {},
                False,
                False,
                False,
                60,
                None,
                "diagnostics_warehouse.yaml",
            ],
        ),
        (
            [
                "soda",
                "contract",
                "verify",
                "-d",
                "some-dataset",
                "-ds",
                "ds.yaml",
                "-sc",
                "cloud.yaml",
                "--check-paths",
                "check.path.one",
                "check.path.two",
            ],
            [
                None,
                "some-dataset",
                ["ds.yaml"],
                "cloud.yaml",
                {},
                False,
                False,
                False,
                60,
                ["check.path.one", "check.path.two"],
                None,
            ],
        ),
    ],
)
@patch("soda_core.cli.cli.handle_verify_contract")
def test_cli_argument_mapping_for_contract_verify_command(mock_handler, args, expected):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = args

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with(*expected)


def test_verify_command_raises_exception_when_none_of_contract_or_dataset_specified():
    sys.argv = [
        "soda",
        "contract",
        "verify",
        "-ds",
        "ds.yaml",
        "-sc",
        "cloud.yaml",
        "-a",
        "-btm",
        "42",
        "-p",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()
    with pytest.raises(SystemExit) as e:
        _ = args.handler_func(args)

    assert ExitCode.LOG_ERRORS == e.value.code


def test_verify_command_raises_exception_when_variables_are_incorrectly_formatted():
    sys.argv = [
        "soda",
        "contract",
        "verify",
        "-ds",
        "ds.yaml",
        "-sc",
        "cloud.yaml",
        "--set",
        "invalid_variable",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()
    with pytest.raises(SystemExit) as e:
        _ = args.handler_func(args)

    assert ExitCode.LOG_ERRORS == e.value.code


def test_verify_command_handles_variable_types():
    logs = Logs()
    sys.argv = [
        "soda",
        "contract",
        "verify",
        "-ds",
        "ds.yaml",
        "-sc",
        "cloud.yaml",
        "--set",
        "numeric_var=100.1234",
        "--set",
        "int_var=100",
        "--set",
        "string_var=hello",
        "--set",
        "bool_var=true",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()
    with pytest.raises(SystemExit) as e:
        _ = args.handler_func(args)

    assert ExitCode.LOG_ERRORS == e.value.code

    # The first log should be the variable parsing log
    assert logs.get_logs()[0] == "Variable numeric_var is a number, parsed as float: 100.1234"
    assert logs.get_logs()[1] == "Variable int_var is a number, parsed as int: 100"
    # The other values should not be parsed as a specific type, so they should be strings. We verify this by the next log being the error log.
    assert logs.get_logs()[2] == "Soda Cloud file 'cloud.yaml' does not exist"


@patch("soda_core.cli.cli.handle_publish_contract")
def test_cli_argument_mapping_for_contract_publish_command(mock_handler):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "contract",
        "publish",
        "-c",
        "a.yaml",
        "-sc",
        "cloud.yaml",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with(
        "a.yaml",
        "cloud.yaml",
    )


@patch("soda_core.cli.cli.handle_test_contract")
def test_cli_argument_mapping_for_contract_test_command(mock_handler):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "contract",
        "test",
        "-c",
        "a.yaml",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with(
        "a.yaml",
        {},
    )


@patch("soda_core.cli.cli.handle_create_data_source")
def test_cli_argument_mapping_for_data_source_create_command(mock_handler):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "data-source",
        "create",
        "-f",
        "ds.yaml",
        "-t",
        "postgres",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with("ds.yaml", "postgres")


@patch("soda_core.cli.cli.handle_test_data_source")
def test_cli_argument_mapping_for_data_source_test_command(mock_handler):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "data-source",
        "test",
        "-ds",
        "ds.yaml",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with("ds.yaml")


@patch("soda_core.cli.cli.handle_create_soda_cloud")
def test_cli_argument_mapping_for_soda_cloud_create_command(mock_handler):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "cloud",
        "create",
        "-f",
        "sc.yaml",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with("sc.yaml")


@patch("soda_core.cli.cli.handle_test_soda_cloud")
def test_cli_argument_mapping_for_soda_cloud_test_command(mock_handler):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "cloud",
        "test",
        "-sc",
        "sc.yaml",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with("sc.yaml")


@pytest.mark.parametrize(
    "legacy_command",
    [
        "scan",
        "scan_status",
        "ingest",
        "test_connection",
        "simulate_anomaly_detection",
    ],
)
def test_cli_v3_legacy_commands(legacy_command):
    sys.argv = [
        "soda",
        legacy_command,
        "-d",
        "ds",
        "-c",
        "sodacl_snowflake/configuration.yml",
        "sodacl_pg/checks.yml",
    ]

    with pytest.raises(SystemExit) as e:
        execute()

    assert e.value.code == 3
