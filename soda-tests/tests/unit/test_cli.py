import sys
from argparse import ArgumentParser, _SubParsersAction
from typing import Iterator, Optional
from unittest.mock import ANY, MagicMock, patch

import pytest
from soda_core.cli.cli import create_cli_parser, execute, get_or_create_command_parser
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


def _run_soda(argv: list[str], parser: ArgumentParser) -> Optional[int]:
    """Runs the soda script's entry point on argv, parsed by the given parser. Returns the exit
    code, or None when the command's handler returned without exiting. The CLI's own logging
    setup is left out so the test session's logging stays as it is."""
    with patch("soda_core.cli.cli.cli_parser", parser), patch("soda_core.cli.cli._configure_logging"), patch.object(
        sys, "argv", ["soda", *argv]
    ):
        try:
            execute()
        except SystemExit as e:
            return e.code
    return None


def _parser_with_a_mocked_command(resource: str, command: str) -> tuple[ArgumentParser, MagicMock]:
    parser = create_cli_parser()
    handler = MagicMock(return_value=None)
    get_or_create_command_parser(parser, resource, command).set_defaults(handler_func=handler)
    return parser, handler


def _plain_argparse_cli_parser() -> ArgumentParser:
    """The soda parser as argparse builds it on its own, where a repeated flag keeps its last use."""
    with patch("soda_core.cli.cli._SodaArgumentParser", ArgumentParser):
        return create_cli_parser()


def _parsers(parser: ArgumentParser) -> Iterator[ArgumentParser]:
    yield parser
    for action in parser._actions:
        if isinstance(action, _SubParsersAction):
            for subparser in action.choices.values():
                yield from _parsers(subparser)


def _without_handler(args) -> dict:
    return {key: value for key, value in vars(args).items() if key != "handler_func"}


REPEATED_FLAGS = [
    pytest.param(
        ["contract", "verify", "-c", "a.yaml", "--contract=b.yaml", "-ds", "ds.yaml"],
        "soda contract verify got -c/--contract 2 times: a.yaml, b.yaml. Give each flag once.",
        id="verify -c, short and long",
    ),
    pytest.param(
        ["contract", "verify", "-d", "ds/a", "--dataset", "ds/b", "-d", "ds/c", "-ds", "ds.yaml"],
        "soda contract verify got -d/--dataset 3 times: ds/a, ds/b, ds/c. Give each flag once.",
        id="verify -d, 3 times",
    ),
    pytest.param(
        ["contract", "verify", "-c", "a.yaml", "-ds", "a.yaml", "-ds", "b.yaml"],
        "soda contract verify got -ds/--data-source 2 times: a.yaml, b.yaml. "
        "Give each flag once, like -ds a.yaml b.yaml.",
        id="verify -ds, several values and a default",
    ),
    pytest.param(
        ["contract", "verify", "-c", "a.yaml", "-sc", "a.yml", "-sc", "b.yml"],
        "soda contract verify got -sc/--soda-cloud 2 times: a.yml, b.yml. Give each flag once.",
        id="verify -sc",
    ),
    pytest.param(
        ["contract", "verify", "-c", "a.yaml", "-v", "--verbose"],
        "soda contract verify got -v/--verbose 2 times. Give each flag once.",
        id="verify -v, a switch",
    ),
    pytest.param(
        ["contract", "verify", "-c", "a.yaml", "-rr"],
        "soda contract verify got -r/--use-runner 2 times. Give each flag once.",
        id="verify -rr, combined",
    ),
    pytest.param(
        ["contract", "verify", "-c", "a.yaml", "-btm", "60", "-btm", "60"],
        "soda contract verify got -btm/--blocking-timeout-in-minutes 2 times: 60, 60. Give each flag once.",
        id="verify -btm, its default twice",
    ),
    pytest.param(
        ["contract", "verify", "-c", "a.yaml", "-dw", "-dw", "b.yml"],
        "soda contract verify got -dw/--diagnostics-warehouse 2 times: no value, b.yml. Give each flag once.",
        id="verify -dw, once without its optional value",
    ),
    pytest.param(
        ["contract", "verify", "-c", "a.yaml", "-cp", "p.one", "-cp", "p.two"],
        "soda contract verify got -cp/--check-paths 2 times: p.one, p.two. Give each flag once, like -cp p.one p.two.",
        id="verify -cp",
    ),
    pytest.param(
        ["contract", "publish", "-c", "a.yaml", "-sc", "a.yml", "-sc", "b.yml"],
        "soda contract publish got -sc/--soda-cloud 2 times: a.yml, b.yml. Give each flag once.",
        id="publish -sc",
    ),
    pytest.param(
        ["contract", "test", "-c", "a.yaml", "-c", "b.yaml"],
        "soda contract test got -c/--contract 2 times: a.yaml, b.yaml. Give each flag once.",
        id="test -c",
    ),
    pytest.param(
        ["contract", "fetch", "-d", "a", "b", "-d", "c", "-f", "x.yaml", "-sc", "sc.yml"],
        "soda contract fetch got -d/--dataset 2 times: a b, c. Give each flag once, like -d a b c.",
        id="fetch -d",
    ),
    pytest.param(
        ["contract", "fetch", "-d", "a", "-f", "x.yaml", "-f", "y.yaml", "-sc", "sc.yml"],
        "soda contract fetch got -f/--file 2 times: x.yaml, y.yaml. Give each flag once, like -f x.yaml y.yaml.",
        id="fetch -f",
    ),
    pytest.param(
        ["data-source", "create", "-t", "postgres", "-t", "postgres"],
        "soda data-source create got -t/--type 2 times: postgres, postgres. Give each flag once.",
        id="data-source create -t, its default twice",
    ),
    pytest.param(
        ["data-source", "test", "-ds", "a.yml", "-ds", "b.yml"],
        "soda data-source test got -ds/--data-source 2 times: a.yml, b.yml. Give each flag once.",
        id="data-source test -ds",
    ),
    pytest.param(
        ["data-source", "discover", "-ds", "ds.yml", "--include", "a%", "--include", "b%"],
        "soda data-source discover got --include 2 times: a%, b%. Give each flag once, like --include a% b%.",
        id="data-source discover --include",
    ),
    pytest.param(
        ["cloud", "create", "-f", "a.yml", "-f", "b.yml"],
        "soda cloud create got -f/--file 2 times: a.yml, b.yml. Give each flag once.",
        id="cloud create -f",
    ),
    pytest.param(
        ["cloud", "test", "-sc", "a.yml", "-sc", "b.yml"],
        "soda cloud test got -sc/--soda-cloud 2 times: a.yml, b.yml. Give each flag once.",
        id="cloud test -sc",
    ),
    pytest.param(
        ["request", "fetch", "-sc", "sc.yml", "-r", "1", "-r", "2", "-f", "out.yaml"],
        "soda request fetch got -r/--request 2 times: 1, 2. Give each flag once.",
        id="request fetch -r",
    ),
    pytest.param(
        ["request", "push", "-sc", "sc.yml", "-f", "in.yaml", "-r", "1", "-m", "a", "-m", "b"],
        "soda request push got -m/--message 2 times: a, b. Give each flag once.",
        id="request push -m",
    ),
    pytest.param(
        ["request", "transition", "-sc", "sc.yml", "-r", "1", "-s", "open", "-s", "done"],
        "soda request transition got -s/--status 2 times: open, done. Give each flag once.",
        id="request transition -s",
    ),
]


@pytest.mark.parametrize("argv, error", REPEATED_FLAGS)
def test_every_command_refuses_a_repeated_flag_before_it_runs(argv, error):
    """argparse keeps the last use of a repeated flag and drops the others without a word, so
    -ds a.yaml -ds b.yaml verified against b.yaml only. Every soda command now stops with exit 3
    before its handler runs, naming the flag and every value it was given. Not argparse's exit 2,
    which a launcher cannot tell apart from check warnings."""
    logs = Logs()
    parser, handler = _parser_with_a_mocked_command(argv[0], argv[1])

    assert _run_soda(argv, parser) == ExitCode.LOG_ERRORS
    assert logs.get_errors() == [error]
    handler.assert_not_called()


def test_a_command_with_several_repeated_flags_names_them_all_in_one_error():
    logs = Logs()
    parser, handler = _parser_with_a_mocked_command("contract", "verify")
    argv = "contract verify -c a.yaml -c b.yaml -v -ds x.yaml -v -ds y.yaml z.yaml".split()

    assert _run_soda(argv, parser) == ExitCode.LOG_ERRORS
    assert logs.get_errors() == [
        "soda contract verify got -c/--contract 2 times: a.yaml, b.yaml; -v/--verbose 2 times; "
        "-ds/--data-source 2 times: x.yaml, y.yaml z.yaml. Give each flag once, like -ds x.yaml y.yaml z.yaml."
    ]
    handler.assert_not_called()


def test_a_repeated_flag_is_logged_after_logging_is_set_up():
    parser, handler = _parser_with_a_mocked_command("contract", "verify")
    calls = MagicMock()

    with patch("soda_core.cli.cli.cli_parser", parser), patch(
        "soda_core.cli.cli._configure_logging", calls.configure_logging
    ), patch("soda_core.cli.cli.soda_logger", calls.soda_logger), patch.object(
        sys, "argv", ["soda", "contract", "verify", "-c", "a.yaml", "-v", "-v"]
    ):
        with pytest.raises(SystemExit) as e:
            execute()

    assert e.value.code == ExitCode.LOG_ERRORS
    assert [name for name, _, _ in calls.mock_calls][:2] == ["configure_logging", "soda_logger.error"]
    calls.configure_logging.assert_called_once_with(True)
    handler.assert_not_called()


ONE_USE_OF_EACH_FLAG = [
    # fmt: off
    ["contract", "verify", "-c", "a.yaml", "-d", "x/y", "-ds", "a.yml", "b.yml", "-sc", "sc.yml", "--set", "k=v",
     "-r", "-a", "-btm", "60", "-p", "-v", "-cp", "p.one", "p.two", "-cf", "name=x", "-dw", "dw.yml", "-mdw", "m.yml"],
    # fmt: on
    ["contract", "verify", "--contract=a.yaml", "--dataset=x/y", "--data-source", "a.yml", "--use-runner"],
    ["contract", "verify", "-c", "a.yaml", "-vp", "-dw", "-cp"],
    ["contract", "verify", "-d", "x/y"],
    ["contract", "publish", "-c", "a.yaml", "-sc", "sc.yml", "-v"],
    ["contract", "test", "-c", "a.yaml", "-v"],
    ["contract", "test"],
    ["contract", "fetch", "-d", "a", "b", "-f", "x.yaml", "y.yaml", "-sc", "sc.yml", "-v"],
    ["data-source", "create", "-f", "ds.yml", "-t", "postgres", "-v"],
    ["data-source", "create"],
    ["data-source", "test", "-ds", "ds.yml", "-sc", "sc.yml", "-v"],
    # fmt: off
    ["data-source", "discover", "-ds", "ds.yml", "--include", "a%", "b%", "--exclude", "c%",
     "--scan-definition-name", "sd", "-sc", "sc.yml", "-v"],
    # fmt: on
    ["cloud", "create", "-f", "sc.yml", "-v"],
    ["cloud", "test", "-sc", "sc.yml", "-v"],
    ["request", "fetch", "-sc", "sc.yml", "-r", "1", "-f", "out.yaml", "-p", "2"],
    ["request", "push", "-sc", "sc.yml", "-f", "in.yaml", "-r", "1", "-m", "hello"],
    ["request", "transition", "-sc", "sc.yml", "-r", "1", "-s", "done"],
]


@pytest.mark.parametrize("argv", ONE_USE_OF_EACH_FLAG, ids=lambda argv: " ".join(argv))
def test_one_use_of_each_flag_parses_as_plain_argparse_does(argv):
    """execute() sends vars(args) to telemetry and the handlers read the same args, so a command
    line without a repeat must parse to exactly what argparse alone makes of it."""
    args = create_cli_parser().parse_args(argv)
    plain_args = _plain_argparse_cli_parser().parse_args(argv)

    assert _without_handler(args) == _without_handler(plain_args)
    assert args.handler_func.__qualname__ == plain_args.handler_func.__qualname__


def test_every_command_prints_the_help_and_usage_plain_argparse_prints():
    parsers = list(_parsers(create_cli_parser()))
    plain_parsers = list(_parsers(_plain_argparse_cli_parser()))

    assert [parser.prog for parser in parsers] == [parser.prog for parser in plain_parsers]
    assert "soda request transition" in [parser.prog for parser in parsers]
    for parser, plain_parser in zip(parsers, plain_parsers):
        assert parser.format_usage() == plain_parser.format_usage()
        assert parser.format_help() == plain_parser.format_help()


@pytest.mark.parametrize("argv", ONE_USE_OF_EACH_FLAG, ids=lambda argv: " ".join(argv))
def test_one_use_of_each_flag_runs_the_command(argv):
    logs = Logs()
    parser, handler = _parser_with_a_mocked_command(argv[0], argv[1])

    assert _run_soda(argv, parser) is None
    assert logs.get_errors() == []
    handler.assert_called_once()
    assert _without_handler(handler.call_args.args[0]) == _without_handler(
        _plain_argparse_cli_parser().parse_args(argv)
    )


def test_flags_that_collect_every_use_stay_repeatable():
    logs = Logs()
    parser, handler = _parser_with_a_mocked_command("contract", "verify")
    argv = "contract verify -c a.yaml --set a=1 --set b=2 -cf name=x --check-filter y".split()

    assert _run_soda(argv, parser) is None
    assert logs.get_errors() == []
    args = handler.call_args.args[0]
    assert args.set == ["a=1", "b=2"]
    assert args.check_filter == ["name=x", "y"]


@pytest.mark.parametrize("resource", ["data-source", "widget"], ids=["existing resource", "new resource"])
def test_a_command_an_extension_adds_takes_each_flag_once(resource):
    """Extensions hang their commands on the root parser through get_or_create_command_parser and
    add their flags with plain add_argument calls. They get the rule without any code of their own."""
    logs = Logs()
    parser = create_cli_parser()
    command_parser = get_or_create_command_parser(parser, resource, "scan", help_str="Scan it")
    command_parser.add_argument("-ds", "--data-source", type=str)
    command_parser.add_argument("--dry-run", action="store_true")
    command_parser.add_argument("--no-wait", dest="wait", action="store_false")
    command_parser.add_argument("--dataset", action="append")
    handler = MagicMock(return_value=None)
    command_parser.set_defaults(handler_func=handler)

    argv = [resource, *"scan -ds a.yml --data-source b.yml --dry-run --dry-run --no-wait --no-wait".split()]
    assert _run_soda(argv, parser) == ExitCode.LOG_ERRORS
    assert logs.get_errors() == [
        f"soda {resource} scan got -ds/--data-source 2 times: a.yml, b.yml; --dry-run 2 times; --no-wait 2 times. "
        f"Give each flag once."
    ]
    handler.assert_not_called()

    argv = [resource, *"scan -ds a.yml --dry-run --dataset x --dataset y".split()]
    assert _run_soda(argv, parser) is None
    handler.assert_called_once()
    args = handler.call_args.args[0]
    assert (args.data_source, args.dry_run, args.wait, args.dataset) == ("a.yml", True, True, ["x", "y"])


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
