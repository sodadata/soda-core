import sys
from argparse import ArgumentParser, _SubParsersAction
from typing import Iterator, Optional
from unittest.mock import ANY, MagicMock, patch

import pytest
from soda_core.cli.cli import create_cli_parser, execute, get_or_create_command_parser
from soda_core.cli.exit_codes import ExitCode
from soda_core.common.logs import Logs
from soda_core.contracts.impl.check_selector import CHECK_FILTER_HELP, CheckSelector

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


# One repeated flag of each kind argparse has: a single value, short and long; several values in
# one use; a switch; switches combined; and an optional value.
REPEATED_FLAGS = [
    pytest.param(
        ["contract", "verify", "-c", "a.yaml", "--contract=b.yaml", "-ds", "ds.yaml"],
        "soda contract verify: error: -c/--contract given more than once. Give it once.",
        id="single value, short and long",
    ),
    pytest.param(
        ["contract", "verify", "-c", "a.yaml", "-ds", "a.yaml", "-ds", "b.yaml"],
        "soda contract verify: error: -ds/--data-source given more than once. Give it once, with all its values after it.",
        id="several values",
    ),
    pytest.param(
        ["contract", "verify", "-c", "a.yaml", "-v", "--verbose"],
        "soda contract verify: error: -v/--verbose given more than once. Give it once.",
        id="switch",
    ),
    pytest.param(
        ["contract", "verify", "-c", "a.yaml", "-rr"],
        "soda contract verify: error: -r/--use-runner given more than once. Give it once.",
        id="switches combined",
    ),
    pytest.param(
        ["contract", "verify", "-c", "a.yaml", "-dw", "-dw", "b.yml"],
        "soda contract verify: error: -dw/--diagnostics-warehouse given more than once. Give it once.",
        id="optional value, once without it",
    ),
]


def _error_line(stderr: str) -> str:
    """The last line argparse prints for a usage error, after the usage."""
    return stderr.strip().splitlines()[-1]


@pytest.mark.parametrize("argv, error", REPEATED_FLAGS)
def test_every_command_refuses_a_repeated_flag_before_it_runs(argv, error, capsys):
    """argparse keeps the last use of a repeated flag and drops the others without a word, so
    -ds a.yaml -ds b.yaml verified against b.yaml only. Every soda command now stops with a usage
    error before its handler runs, naming the first flag it got twice."""
    parser, handler = _parser_with_a_mocked_command(argv[0], argv[1])

    assert _run_soda(argv, parser) == ExitCode.LOG_ERRORS
    assert _error_line(capsys.readouterr().err) == error
    handler.assert_not_called()


def test_a_command_with_several_repeated_flags_names_the_first(capsys):
    parser, handler = _parser_with_a_mocked_command("contract", "verify")
    argv = "contract verify -c a.yaml -c b.yaml -v -ds x.yaml -v -ds y.yaml z.yaml".split()

    assert _run_soda(argv, parser) == ExitCode.LOG_ERRORS
    assert _error_line(capsys.readouterr().err) == (
        "soda contract verify: error: -c/--contract given more than once. Give it once."
    )
    handler.assert_not_called()


@pytest.mark.parametrize(
    "argv",
    [["contract", "verify", "-c", "a.yaml", "--no-such-flag"], ["contract", "verify", "-c"]],
    ids=["unknown flag", "missing value"],
)
def test_every_usage_error_exits_3_not_argparses_2(argv, capsys):
    """Exit 2 is also the check-warnings code, so a launcher could not tell a usage error from warnings."""
    parser, handler = _parser_with_a_mocked_command(argv[0], argv[1])

    assert _run_soda(argv, parser) == ExitCode.LOG_ERRORS
    assert "error:" in _error_line(capsys.readouterr().err)
    handler.assert_not_called()


ONE_USE_OF_EACH_FLAG = [
    # fmt: off
    ["contract", "verify", "-c", "a.yaml", "-d", "x/y", "-ds", "a.yml", "b.yml", "-sc", "sc.yml", "--set", "k=v",
     "-r", "-a", "-btm", "60", "-p", "-v", "-cp", "p.one", "p.two", "-cf", "name=x", "-dw", "dw.yml", "-mdw", "m.yml"],
    # fmt: on
    ["contract", "verify", "-c", "a.yaml", "-vp", "-dw", "-cp"],
    ["contract", "fetch", "-d", "a", "b", "-f", "x.yaml", "y.yaml", "-sc", "sc.yml", "-v"],
    ["data-source", "discover", "-ds", "ds.yml", "--include", "a%", "b%", "--exclude", "c%", "-sc", "sc.yml"],
]


def test_every_command_prints_the_help_and_usage_plain_argparse_prints():
    parsers = list(_parsers(create_cli_parser()))
    plain_parsers = list(_parsers(_plain_argparse_cli_parser()))

    assert [parser.prog for parser in parsers] == [parser.prog for parser in plain_parsers]
    assert "soda request transition" in [parser.prog for parser in parsers]
    for parser, plain_parser in zip(parsers, plain_parsers):
        assert parser.format_usage() == plain_parser.format_usage()
        assert parser.format_help() == plain_parser.format_help()


@pytest.mark.parametrize("argv", ONE_USE_OF_EACH_FLAG, ids=lambda argv: " ".join(argv))
def test_one_use_of_each_flag_runs_the_command_with_what_plain_argparse_parses(argv):
    """execute() sends vars(args) to telemetry and the handlers read the same args, so a command
    line without a repeat must reach the handler exactly as argparse alone parses it."""
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
def test_a_command_an_extension_adds_takes_each_flag_once(resource, capsys):
    """Extensions hang their commands on the root parser through get_or_create_command_parser and
    add their flags with plain add_argument calls. They get the rule without any code of their own."""
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
    assert _error_line(capsys.readouterr().err) == (
        f"soda {resource} scan: error: -ds/--data-source given more than once. Give it once."
    )
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


def _contract_verify_check_filter_help() -> str:
    verify_parser = (
        create_cli_parser()
        ._subparsers._group_actions[0]
        .choices["contract"]
        ._subparsers._group_actions[0]
        .choices["verify"]
    )
    check_filter_action = next(action for action in verify_parser._actions if "--check-filter" in action.option_strings)
    return check_filter_action.help


def test_check_filter_help_lists_every_supported_field():
    """The -cf help text must name every field the selector actually accepts.

    These drifted apart: help documented 6 of the 10 supported fields, so
    relative_path, check_path, source, collection and standard all worked but
    were undiscoverable. A filter naming a field a user believes unsupported
    silently matches nothing, and a check selection that matches nothing still
    exits 0.
    """
    check_filter_help = _contract_verify_check_filter_help()

    undocumented = sorted(field for field in CheckSelector.SUPPORTED_FIELDS if field not in check_filter_help)
    assert not undocumented, f"--check-filter accepts {undocumented} but --help does not mention them"


def test_check_filter_help_explains_negation():
    assert "key!=value" in CHECK_FILTER_HELP


def test_check_filter_help_is_the_shared_constant():
    """Other CLIs that take -cf import CHECK_FILTER_HELP, so contract verify must show exactly that text."""
    assert _contract_verify_check_filter_help() == CHECK_FILTER_HELP
