import sys
from unittest.mock import ANY, patch

import pytest
from soda_core.cli.cli import create_cli_parser, execute
from soda_core.cli.exit_codes import ExitCode
from soda_core.common.logs import Logs
from soda_core.contracts.impl.check_selector import CheckSelector

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
                [],
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
                [],
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
            [None, "some-dataset", ["ds.yaml"], "cloud.yaml", {}, False, False, False, 60, None, [], None, None],
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
                [],
                "diagnostics_warehouse.yaml",
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
                "-dw",
                "diagnostics_warehouse.yaml",
                "-mdw",
                "metadata_diagnostics_warehouse.yaml",
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
                [],
                "diagnostics_warehouse.yaml",
                "metadata_diagnostics_warehouse.yaml",
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
                [],
                None,
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

    # The verify wiring wraps the handler in run_scan and threads
    # the wrapper's Logs collector; the argument mapping under test is positional.
    mock_handler.assert_called_once_with(*expected, logs=ANY)


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


# (command, its other required arguments, the handler that would parse, connect and call Cloud)
CONTRACT_COMMANDS_TAKING_ONE_CONTRACT = [
    ("verify", ["-ds", "ds.yaml", "-sc", "cloud.yaml"], "handle_verify_contract"),
    ("publish", ["-sc", "cloud.yaml"], "handle_publish_contract"),
    ("test", [], "handle_test_contract"),
]


@pytest.mark.parametrize("command, other_args, handler_name", CONTRACT_COMMANDS_TAKING_ONE_CONTRACT)
@pytest.mark.parametrize(
    "contract_args",
    [
        ["-c", "a.yaml", "-c", "b.yaml"],
        ["--contract", "a.yaml", "--contract=b.yaml"],
    ],
)
def test_contract_command_refuses_a_second_contract_before_it_runs(command, other_args, handler_name, contract_args):
    """A second -c used to replace the first without a word, so the command ran on b.yaml
    only. It now fails with exit 3 before any parsing, query or Cloud call. Not argparse's
    exit 2, which a launcher cannot tell apart from check warnings."""
    logs = Logs()
    sys.argv = ["soda", "contract", command, *contract_args, *other_args]

    parser = create_cli_parser()
    args = parser.parse_args()

    with patch(f"soda_core.cli.cli.{handler_name}") as mock_handler, patch(
        "soda_core.cli.cli.resolve_soda_cloud_for_failure_report"
    ) as mock_resolve_soda_cloud, patch("soda_core.cli.cli.run_scan") as mock_run_scan:
        with pytest.raises(SystemExit) as e:
            args.handler_func(args)

    assert e.value.code == ExitCode.LOG_ERRORS
    assert logs.get_errors() == [
        f"soda contract {command} takes one contract, but -c/--contract was given 2 files: a.yaml, b.yaml. "
        f"Run the command once per contract."
    ]
    mock_handler.assert_not_called()
    mock_resolve_soda_cloud.assert_not_called()
    mock_run_scan.assert_not_called()


@pytest.mark.parametrize("command, other_args, handler_name", CONTRACT_COMMANDS_TAKING_ONE_CONTRACT)
@pytest.mark.parametrize("contract_args", [["-c", "a.yaml"], ["--contract", "a.yaml"], ["--contract=a.yaml"]])
def test_contract_command_runs_one_contract_as_before(command, other_args, handler_name, contract_args):
    sys.argv = ["soda", "contract", command, *contract_args, *other_args]

    parser = create_cli_parser()
    args = parser.parse_args()

    assert args.contract == "a.yaml"
    with patch(f"soda_core.cli.cli.{handler_name}", return_value=ExitCode.CHECK_FAILURES) as mock_handler:
        with pytest.raises(SystemExit) as e:
            args.handler_func(args)

    assert e.value.code == ExitCode.CHECK_FAILURES
    mock_handler.assert_called_once()
    assert mock_handler.call_args.args[0] == "a.yaml"


@pytest.mark.parametrize("command, other_args, handler_name", CONTRACT_COMMANDS_TAKING_ONE_CONTRACT)
def test_contract_command_without_a_value_for_contract_keeps_the_argparse_error(
    command, other_args, handler_name, capsys
):
    sys.argv = ["soda", "contract", command, *other_args, "-c"]

    parser = create_cli_parser()
    with pytest.raises(SystemExit) as e:
        parser.parse_args()

    assert e.value.code == 2
    assert "argument -c/--contract: expected one argument" in capsys.readouterr().err


VERIFY_OTHER_ARGS = ["-ds", "ds.yaml", "-sc", "cloud.yaml"]


@pytest.mark.parametrize(
    "dataset_args, given",
    [
        (["-d", "ds/a", "-d", "ds/b"], "2 datasets: ds/a, ds/b"),
        (["--dataset", "ds/a", "--dataset=ds/b"], "2 datasets: ds/a, ds/b"),
        (["-d", "ds/a", "--dataset", "ds/b", "-d", "ds/c"], "3 datasets: ds/a, ds/b, ds/c"),
        (["-c", "a.yaml", "-d", "ds/a", "-d", "ds/b"], "2 datasets: ds/a, ds/b"),
    ],
)
def test_contract_verify_refuses_a_second_dataset_before_it_runs(dataset_args, given):
    """A second -d used to replace the first without a word, so verify fetched and ran only the
    last dataset's contract from Soda Cloud. It now fails with exit 3 before any Cloud call."""
    logs = Logs()
    sys.argv = ["soda", "contract", "verify", *dataset_args, *VERIFY_OTHER_ARGS]

    parser = create_cli_parser()
    args = parser.parse_args()

    with patch("soda_core.cli.cli.handle_verify_contract") as mock_handler, patch(
        "soda_core.cli.cli.resolve_soda_cloud_for_failure_report"
    ) as mock_resolve_soda_cloud, patch("soda_core.cli.cli.run_scan") as mock_run_scan:
        with pytest.raises(SystemExit) as e:
            args.handler_func(args)

    assert e.value.code == ExitCode.LOG_ERRORS
    assert logs.get_errors() == [
        f"soda contract verify takes one dataset, but -d/--dataset was given {given}. "
        f"Run the command once per dataset."
    ]
    mock_handler.assert_not_called()
    mock_resolve_soda_cloud.assert_not_called()
    mock_run_scan.assert_not_called()


def test_contract_verify_names_both_a_second_contract_and_a_second_dataset():
    logs = Logs()
    sys.argv = [
        "soda",
        "contract",
        "verify",
        *["-c", "a.yaml", "-c", "b.yaml", "-d", "ds/a", "-d", "ds/b"],
        *VERIFY_OTHER_ARGS,
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with patch("soda_core.cli.cli.handle_verify_contract") as mock_handler:
        with pytest.raises(SystemExit) as e:
            args.handler_func(args)

    assert e.value.code == ExitCode.LOG_ERRORS
    assert logs.get_errors() == [
        "soda contract verify takes one contract, but -c/--contract was given 2 files: a.yaml, b.yaml. "
        "Run the command once per contract.",
        "soda contract verify takes one dataset, but -d/--dataset was given 2 datasets: ds/a, ds/b. "
        "Run the command once per dataset.",
    ]
    mock_handler.assert_not_called()


@pytest.mark.parametrize("dataset_args", [["-d", "ds/a"], ["--dataset", "ds/a"], ["--dataset=ds/a"]])
@pytest.mark.parametrize("contract_args, expected_contract", [([], None), (["-c", "a.yaml"], "a.yaml")])
def test_contract_verify_runs_one_dataset_as_before(dataset_args, contract_args, expected_contract):
    logs = Logs()
    sys.argv = ["soda", "contract", "verify", *contract_args, *dataset_args, *VERIFY_OTHER_ARGS]

    parser = create_cli_parser()
    args = parser.parse_args()

    assert args.dataset == "ds/a"
    with patch("soda_core.cli.cli.handle_verify_contract", return_value=ExitCode.CHECK_FAILURES) as mock_handler, patch(
        "soda_core.cli.cli.resolve_soda_cloud_for_failure_report", return_value=None
    ):
        with pytest.raises(SystemExit) as e:
            args.handler_func(args)

    assert e.value.code == ExitCode.CHECK_FAILURES
    assert logs.get_errors() == []
    mock_handler.assert_called_once()
    assert mock_handler.call_args.args[:2] == (expected_contract, "ds/a")


@pytest.mark.parametrize(
    "command, one_value_args, expected_values, other_args",
    [
        ("verify", ["-d", "ds/a"], {"dataset": "ds/a"}, VERIFY_OTHER_ARGS),
        ("verify", ["-c", "a.yaml", "-d", "ds/a"], {"contract": "a.yaml", "dataset": "ds/a"}, VERIFY_OTHER_ARGS),
        ("verify", ["-c", "a.yaml"], {"contract": "a.yaml"}, VERIFY_OTHER_ARGS),
        ("publish", ["-c", "a.yaml"], {"contract": "a.yaml"}, ["-sc", "cloud.yaml"]),
        ("test", ["-c", "a.yaml"], {"contract": "a.yaml"}, []),
    ],
)
def test_one_contract_or_dataset_changes_only_its_own_value_in_the_args_telemetry_reads(
    command, one_value_args, expected_values, other_args
):
    """execute() sends vars(args) to telemetry, one attribute per key. A single -c or -d must
    set its own value and add no key."""
    parser = create_cli_parser()
    given = vars(parser.parse_args(["contract", command, *one_value_args, *other_args]))
    absent = vars(parser.parse_args(["contract", command, *other_args]))

    assert given == {**absent, **expected_values}


def test_contract_verify_without_a_value_for_dataset_keeps_the_argparse_error(capsys):
    sys.argv = ["soda", "contract", "verify", *VERIFY_OTHER_ARGS, "-d"]

    parser = create_cli_parser()
    with pytest.raises(SystemExit) as e:
        parser.parse_args()

    assert e.value.code == 2
    assert "argument -d/--dataset: expected one argument" in capsys.readouterr().err


@pytest.mark.parametrize(
    "dataset_args, expected_datasets",
    [
        (["-d", "ds/a"], ["ds/a"]),
        (["-d", "ds/a", "ds/b"], ["ds/a", "ds/b"]),
        (["--dataset", "ds/a", "ds/b"], ["ds/a", "ds/b"]),
    ],
)
def test_contract_fetch_takes_several_datasets_as_before(dataset_args, expected_datasets):
    logs = Logs()
    sys.argv = ["soda", "contract", "fetch", *dataset_args, "-f", "a.yaml", "b.yaml", "-sc", "cloud.yaml"]

    parser = create_cli_parser()
    args = parser.parse_args()

    with patch("soda_core.cli.cli.handle_fetch_contract", return_value=ExitCode.OK.value) as mock_handler:
        with pytest.raises(SystemExit) as e:
            args.handler_func(args)

    assert e.value.code == 0
    assert logs.get_errors() == []
    mock_handler.assert_called_once_with(["a.yaml", "b.yaml"], expected_datasets, "cloud.yaml")


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

    mock_handler.assert_called_once_with("ds.yaml", soda_cloud_file_path=None)


@patch("soda_core.cli.cli.handle_test_data_source")
def test_cli_argument_mapping_for_data_source_test_command_with_soda_cloud(mock_handler):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "data-source",
        "test",
        "-ds",
        "ds.yaml",
        "-sc",
        "sc.yaml",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with("ds.yaml", soda_cloud_file_path="sc.yaml")


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


def test_check_filter_help_lists_every_supported_field():
    """The -cf help text must name every field the selector actually accepts.

    These drifted apart: help documented 6 of the 10 supported fields, so
    relative_path, check_path, source, collection and standard all worked but
    were undiscoverable. A filter naming a field a user believes unsupported
    silently matches nothing, and a check selection that matches nothing still
    exits 0.
    """
    verify_parser = (
        create_cli_parser()
        ._subparsers._group_actions[0]
        .choices["contract"]
        ._subparsers._group_actions[0]
        .choices["verify"]
    )
    check_filter_action = next(action for action in verify_parser._actions if "--check-filter" in action.option_strings)

    undocumented = sorted(field for field in CheckSelector.SUPPORTED_FIELDS if field not in check_filter_action.help)
    assert not undocumented, f"--check-filter accepts {undocumented} but --help does not mention them"
