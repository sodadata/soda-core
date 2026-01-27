from __future__ import annotations

import signal
import sys
import traceback
from argparse import ArgumentParser, _SubParsersAction
from typing import Dict, List, Optional, Union

from soda_core.__version__ import SODA_CORE_VERSION
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import (
    handle_fetch_contract,
    handle_publish_contract,
    handle_test_contract,
    handle_verify_contract,
)
from soda_core.cli.handlers.data_source import (
    handle_create_data_source,
    handle_test_data_source,
)
from soda_core.cli.handlers.request import (
    handle_fetch_proposal,
    handle_push_proposal,
    handle_transition_request,
)
from soda_core.cli.handlers.soda_cloud import (
    handle_create_soda_cloud,
    handle_test_soda_cloud,
)
from soda_core.common.logging_configuration import configure_logging
from soda_core.common.logging_constants import soda_logger
from soda_core.contracts.contract_request import RequestStatus
from soda_core.telemetry.soda_telemetry import SodaTelemetry
from soda_core.telemetry.soda_tracer import soda_trace

CLOUD_CONFIG_PATH_HELP = "A Soda Cloud configuration file path."
FILE_PATH_HELP = "The path to the file to be created. (directories will be created if needed)"
REQUEST_NUMBER_HELP = "The Contract Request number"

soda_telemetry = SodaTelemetry()


@soda_trace
def execute() -> None:
    try:
        print(r"  __|  _ \|  \   \\")
        print(r"\__ \ (   |   | _ \\")
        print(r"____/\___/___/_/  _\\ CLI v%s" % SODA_CORE_VERSION)

        signal.signal(signal.SIGINT, handle_ctrl_c)

        handle_legacy_commands()

        args = cli_parser.parse_args()

        soda_telemetry.ingest_cli_arguments(vars(args))

        if len(sys.argv) == 1:
            cli_parser.print_help()
            sys.exit(ExitCode.LOG_ERRORS)

        verbose = args.verbose if hasattr(args, "verbose") else False
        _configure_logging(verbose)

        if not hasattr(args, "handler_func"):
            soda_logger.error(f"No handler found for resource '{args.resource}' and command '{args.command}'")
            exit_with_code(ExitCode.LOG_ERRORS)

        args.handler_func(args)

    except Exception as e:
        traceback.print_exc()
        exit_with_code(ExitCode.LOG_ERRORS)


def _configure_logging(verbose: bool) -> None:
    """
    Purpose of this method is to enable override in test environment
    """
    configure_logging(verbose=verbose)


def handle_legacy_commands():
    legacy_cmds = [
        "scan",
        "scan_status",
        "ingest",
        "test_connection",
        "simulate_anomaly_detection",
    ]
    if len(sys.argv) > 1 and sys.argv[1] in legacy_cmds:
        print("Soda v3 commands are no longer supported in Soda Core v4 CLI.")
        print("Please use the v3 version of Soda Core or upgrade to v4 Soda Contracts.")
        print("See https://docs.soda.io/soda-v4/ for more information.")
        exit_with_code(ExitCode.LOG_ERRORS)


def create_cli_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog="soda",
        epilog="Run 'soda {resource} {command} -h' for help on a specific command",
    )
    resource_parsers = parser.add_subparsers(dest="resource", help="Available Soda resources")

    _setup_contract_resource(resource_parsers)
    _setup_data_source_resource(resource_parsers)
    _setup_soda_cloud_resource(resource_parsers)
    _setup_contract_request_resource(resource_parsers)

    return parser


def _setup_contract_resource(resource_parsers) -> None:
    contract_parser = resource_parsers.add_parser("contract", help="Contract commands")
    contract_subparsers = contract_parser.add_subparsers(dest="command", help="Contract commands")

    _setup_contract_verify_command(contract_subparsers)
    _setup_contract_publish_command(contract_subparsers)
    _setup_contract_test_command(contract_subparsers)
    _setup_contract_fetch_command(contract_subparsers)


def _setup_contract_verify_command(contract_parsers) -> None:
    verify_parser = contract_parsers.add_parser("verify", help="Verify a contract")

    verify_parser.add_argument(
        "-c",
        "--contract",
        type=str,
        help="Contract file path to verify. Use this to work with local contracts.",
    )
    verify_parser.add_argument(
        "-d",
        "--dataset",
        type=str,
        help="Name of dataset to verify. Use this to work with remote contracts present in Soda Cloud.",
    )

    verify_parser.add_argument(
        "-ds", "--data-source", type=str, help="The data source configuration file.", nargs="+", default=[]
    )
    verify_parser.add_argument("-sc", "--soda-cloud", type=str, help=CLOUD_CONFIG_PATH_HELP)
    verify_parser.add_argument(
        "--set",
        action="append",
        type=str,
        help="Set variable values to be used in the contract with format '--set <variable_name>=<variable_value>'.",
    )
    verify_parser.add_argument(
        "-a",
        "--use-agent",
        const=True,
        action="store_const",
        default=False,
        help="Executes contract verification on Soda Agent instead of locally in this library.",
    )
    verify_parser.add_argument(
        "-btm",
        "--blocking-timeout-in-minutes",
        type=int,
        default=60,
        help="Max time in minutes that the CLI should wait for the contract "
        "verification to complete on Soda Agent.  Default is 60 minutes.",
    )
    verify_parser.add_argument(
        "-p",
        "--publish",
        const=True,
        action="store_const",
        default=False,
        help="Send the verification results to Soda Cloud.",
    )
    verify_parser.add_argument(
        "-v",
        "--verbose",
        const=True,
        action="store_const",
        default=False,
        help="Show more detailed logs on the console.",
    )
    verify_parser.add_argument(
        "-cp",
        "--check-paths",
        type=str,
        nargs="*",
        help="One or more check paths to run in a contract.",
    )
    # TODO: move into extensions
    verify_parser.add_argument(
        "-dw",
        "--diagnostics-warehouse",
        type=str,
        nargs="?",
        help="Specify the path to the diagnostics warehouse configuration file. ",
    )

    def handle(args):
        contract_file_path = args.contract
        dataset_identifier = args.dataset
        data_source_file_paths = args.data_source
        soda_cloud_file_path = args.soda_cloud
        variables = _parse_variables(args.set)
        if variables is None:
            exit_with_code(ExitCode.LOG_ERRORS)
        publish = args.publish
        verbose = args.verbose
        use_agent = args.use_agent
        blocking_timeout_in_minutes = args.blocking_timeout_in_minutes
        check_paths = args.check_paths
        diagnostics_warehouse_file_path = args.diagnostics_warehouse

        exit_code = handle_verify_contract(
            contract_file_path,
            dataset_identifier,
            data_source_file_paths,
            soda_cloud_file_path,
            variables,
            publish,
            verbose,
            use_agent,
            blocking_timeout_in_minutes,
            check_paths,
            diagnostics_warehouse_file_path,
        )

        exit_with_code(exit_code)

    verify_parser.set_defaults(handler_func=handle)


def _parse_variables(variables: Optional[List[str]]) -> Optional[Dict[str, str]]:
    if not variables:
        return {}

    result = {}
    for variable in variables:
        if "=" not in variable:
            soda_logger.error(f"Variable {variable} is incorrectly formatted. Please use the format KEY=VALUE")
            return None
        key, value = variable.split("=", 1)
        key = key.strip()
        value = value.strip()
        value = _parse_variable_value(key=key, value=value)
        if not key or not value:
            soda_logger.error(
                f"Incorrectly formatted variable '{variable}', key or value is empty. Please use the format KEY=VALUE"
            )
            return None
        result[key] = value
    return result


def _parse_variable_value(key: str, value: str) -> Union[str, float, int]:
    # Try to parse the value as an integer first
    # Note: we don't use the .isnumeric() or isdigit() methods here, as they are not reliable for floats, negative numbers...
    try:
        value = int(value)
        soda_logger.info(f"Variable {key} is a number, parsed as int: {value}")
        return value
    except ValueError:
        pass
    # Then try to parse it as a float
    try:
        value = float(value)
        soda_logger.info(f"Variable {key} is a number, parsed as float: {value}")
        return value
    except ValueError:
        pass
    # If it's not a number, return the string
    return value


def _setup_contract_publish_command(contract_parsers) -> None:
    publish_parser = contract_parsers.add_parser("publish", help="Publish a contract")
    publish_parser.add_argument("-c", "--contract", type=str, help="Contract file path to publish.")

    publish_parser.add_argument(
        "-sc",
        "--soda-cloud",
        type=str,
        help=CLOUD_CONFIG_PATH_HELP,
        required=True,
    )

    publish_parser.add_argument(
        "-v",
        "--verbose",
        const=True,
        action="store_const",
        default=False,
        help="Show more detailed logs on the console.",
    )

    def handle(args):
        contract_file_path = args.contract
        soda_cloud_file_path = args.soda_cloud
        exit_code = handle_publish_contract(contract_file_path, soda_cloud_file_path)
        exit_with_code(exit_code)

    publish_parser.set_defaults(handler_func=handle)


def _setup_contract_test_command(contract_parsers) -> None:
    test_contract_parser = contract_parsers.add_parser(name="test", help="Test a contract syntax without executing it")
    test_contract_parser.add_argument("-c", "--contract", type=str, help="Contract file path to test.")

    test_contract_parser.add_argument(
        "-v",
        "--verbose",
        const=True,
        action="store_const",
        default=False,
        help="Show more detailed logs on the console.",
    )

    def handle(args):
        contract_file_path = args.contract

        exit_code = handle_test_contract(contract_file_path, {})
        exit_with_code(exit_code)

    test_contract_parser.set_defaults(handler_func=handle)


def _setup_contract_fetch_command(contract_parsers) -> None:
    fetch_parser = contract_parsers.add_parser("fetch", help="Pull a contract")
    fetch_parser.add_argument(
        "-d",
        "--dataset",
        type=str,
        nargs="+",
        help="Fully qualified names of datasets whose cloud contracts you wish to fetch.",
    )
    fetch_parser.add_argument(
        "-sc",
        "--soda-cloud",
        type=str,
        help=CLOUD_CONFIG_PATH_HELP,
        required=True,
    )
    fetch_parser.add_argument(
        "-f",
        "--file",
        type=str,
        nargs="+",
        help="The path(s) to the contract files to be created or updated. (directories will be created if needed)",
    )
    fetch_parser.add_argument(
        "-v",
        "--verbose",
        const=True,
        action="store_const",
        default=False,
        help="Show more detailed logs on the console.",
    )

    def handle(args):
        contract_file_paths = args.file
        soda_cloud_file_path = args.soda_cloud
        dataset_identifiers = args.dataset

        exit_code = handle_fetch_contract(contract_file_paths, dataset_identifiers, soda_cloud_file_path)
        exit_with_code(exit_code)

    fetch_parser.set_defaults(handler_func=handle)


def _setup_data_source_resource(resource_parsers) -> None:
    data_source_parser = resource_parsers.add_parser("data-source", help="Data source commands")
    data_source_subparsers = data_source_parser.add_subparsers(dest="command", help="Data source commands")

    _setup_data_source_create_command(data_source_subparsers)
    _setup_data_source_test_command(data_source_subparsers)


def _setup_data_source_create_command(data_source_parsers) -> None:
    create_data_source_parser = data_source_parsers.add_parser(
        name="create", help="Create a data source YAML configuration file"
    )
    create_data_source_parser.add_argument(
        "-f",
        "--file",
        type=str,
        help=FILE_PATH_HELP,
    )
    create_data_source_parser.add_argument(
        "-t", "--type", type=str, default="postgres", help="Type of the data source.  Eg postgres"
    )

    create_data_source_parser.add_argument(
        "-v",
        "--verbose",
        const=True,
        action="store_const",
        default=False,
        help="Show more detailed logs on the console.",
    )

    def handle(args):
        data_source_file_path = args.file
        data_source_type = args.type

        exit_code = handle_create_data_source(data_source_file_path, data_source_type)
        exit_with_code(exit_code)

    create_data_source_parser.set_defaults(handler_func=handle)


def _setup_data_source_test_command(data_source_parsers) -> None:
    test_parser = data_source_parsers.add_parser("test", help="Test a data source connection")
    test_parser.add_argument("-ds", "--data-source", type=str, help="The name of a configured data source to test.")

    test_parser.add_argument(
        "-v",
        "--verbose",
        const=True,
        action="store_const",
        default=False,
        help="Show more detailed logs on the console.",
    )

    def handle(args):
        data_source_file_path = args.data_source

        exit_code = handle_test_data_source(data_source_file_path)
        exit_with_code(exit_code)

    test_parser.set_defaults(handler_func=handle)


def _setup_soda_cloud_resource(resource_parsers) -> None:
    soda_cloud_parser = resource_parsers.add_parser("cloud", help="Soda Cloud commands")
    soda_cloud_subparsers = soda_cloud_parser.add_subparsers(dest="command", help="Soda Cloud commands")

    _setup_soda_cloud_create_command(soda_cloud_subparsers)
    _setup_soda_cloud_test_command(soda_cloud_subparsers)


def _setup_soda_cloud_create_command(soda_cloud_parsers) -> None:
    create_soda_cloud_parser = soda_cloud_parsers.add_parser(
        name="create", help="Create a Soda Cloud YAML configuration file"
    )
    create_soda_cloud_parser.add_argument(
        "-f",
        "--file",
        type=str,
        help=FILE_PATH_HELP,
    )

    create_soda_cloud_parser.add_argument(
        "-v",
        "--verbose",
        const=True,
        action="store_const",
        default=False,
        help="Show more detailed logs on the console.",
    )

    def handle(args):
        soda_cloud_file_path = args.file
        exit_code = handle_create_soda_cloud(soda_cloud_file_path)
        exit_with_code(exit_code)

    create_soda_cloud_parser.set_defaults(handler_func=handle)


def _setup_soda_cloud_test_command(soda_cloud_parsers) -> None:
    test_soda_cloud_parser = soda_cloud_parsers.add_parser("test", help="Test the Soda Cloud connection")
    test_soda_cloud_parser.add_argument("-sc", "--soda-cloud", type=str, help=CLOUD_CONFIG_PATH_HELP)

    test_soda_cloud_parser.add_argument(
        "-v",
        "--verbose",
        const=True,
        action="store_const",
        default=False,
        help="Show more detailed logs on the console.",
    )

    def handle(args):
        soda_cloud_file_path = args.soda_cloud
        exit_code = handle_test_soda_cloud(soda_cloud_file_path)
        exit_with_code(exit_code)

    test_soda_cloud_parser.set_defaults(handler_func=handle)


def _setup_contract_request_resource(resource_parsers) -> None:
    contract_request_parser = resource_parsers.add_parser("request", help="Contract request commands")
    contract_request_subparsers = contract_request_parser.add_subparsers(
        dest="command", help="Contract request commands"
    )

    _setup_contract_request_fetch_proposal_command(contract_request_subparsers)
    _setup_contract_request_push_proposal_command(contract_request_subparsers)
    _setup_contract_request_transition_command(contract_request_subparsers)


def _setup_contract_request_fetch_proposal_command(contract_request_parsers: ArgumentParser) -> None:
    fetch_proposal_parser = contract_request_parsers.add_parser("fetch", help="Fetch a proposal from Soda Cloud")
    fetch_proposal_parser.add_argument(
        "-sc",
        "--soda-cloud",
        type=str,
        required=True,
        help=CLOUD_CONFIG_PATH_HELP,
    )

    fetch_proposal_parser.add_argument(
        "-r",
        "--request",
        type=int,
        required=True,
        help=REQUEST_NUMBER_HELP,
    )

    fetch_proposal_parser.add_argument(
        "-f",
        "--file",
        type=str,
        required=True,
        help=FILE_PATH_HELP,
    )

    fetch_proposal_parser.add_argument(
        "-p",
        "--proposal",
        type=int,
        help="The Proposal number (optional). The latest proposal will be fetched if not provided.",
    )

    def handle(args):
        soda_cloud_file_path = args.soda_cloud
        request_number = args.request
        output_file_path = args.file
        proposal_number = args.proposal

        exit_code = handle_fetch_proposal(
            soda_cloud_file_path=soda_cloud_file_path,
            request_number=request_number,
            output_file_path=output_file_path,
            proposal_number=proposal_number,
        )

        exit_with_code(exit_code)

    fetch_proposal_parser.set_defaults(handler_func=handle)


def _setup_contract_request_push_proposal_command(contract_request_parsers: ArgumentParser) -> None:
    push_proposal_parser = contract_request_parsers.add_parser("push", help="Push a proposal to Soda Cloud")

    push_proposal_parser.add_argument(
        "-sc",
        "--soda-cloud",
        type=str,
        required=True,
        help=CLOUD_CONFIG_PATH_HELP,
    )

    push_proposal_parser.add_argument(
        "-f",
        "--file",
        type=str,
        required=True,
        help=FILE_PATH_HELP,
    )

    push_proposal_parser.add_argument(
        "-r",
        "--request",
        type=int,
        required=True,
        help=REQUEST_NUMBER_HELP,
    )

    push_proposal_parser.add_argument(
        "-m",
        "--message",
        type=str,
        help="The message to be pushed to Soda Cloud (optional)",
    )

    def handle(args) -> None:
        soda_cloud_file_path = args.soda_cloud
        file_path = args.file
        request_number = args.request
        message = args.message

        exit_code = handle_push_proposal(
            soda_cloud_file_path=soda_cloud_file_path,
            file_path=file_path,
            request_number=request_number,
            message=message,
        )

        exit_with_code(exit_code)

    push_proposal_parser.set_defaults(handler_func=handle)


def _setup_contract_request_transition_command(contract_request_parsers: ArgumentParser) -> None:
    transition_request_parser = contract_request_parsers.add_parser(
        "transition", help="Transition a request's status on Soda Cloud"
    )

    transition_request_parser.add_argument(
        "-sc",
        "--soda-cloud",
        type=str,
        required=True,
        help=CLOUD_CONFIG_PATH_HELP,
    )

    transition_request_parser.add_argument(
        "-r",
        "--request",
        type=int,
        required=True,
        help=REQUEST_NUMBER_HELP,
    )

    transition_request_parser.add_argument(
        "-s",
        "--status",
        type=str,
        required=True,
        choices=[status.value for status in RequestStatus],
        help="The status to transition the request to",
    )

    def handle(args) -> None:
        soda_cloud_file_path = args.soda_cloud
        request_number = args.request
        status = RequestStatus(args.status)

        exit_code = handle_transition_request(
            soda_cloud_file_path=soda_cloud_file_path,
            request_number=request_number,
            status=status,
        )
        exit_with_code(exit_code)

    transition_request_parser.set_defaults(handler_func=handle)


def exit_with_code(exit_code: int):
    soda_logger.debug(f"Exiting with code {exit_code}")
    soda_telemetry.set_attribute("cli__exit_code", exit_code)
    exit(exit_code)


def handle_ctrl_c(self, sig, frame):
    soda_logger.info("")
    soda_logger.info(f"CTRL+C detected")
    exit_with_code(ExitCode.LOG_ERRORS)


def get_or_create_resource_parser(
    root_parser: ArgumentParser, resource_name: str, help_str: Optional[str] = None
) -> ArgumentParser:
    resource_subparsers = _get_or_create_subparsers(root_parser, "resource")
    if resource_name in resource_subparsers.choices:
        return resource_subparsers.choices[resource_name]

    return resource_subparsers.add_parser(resource_name, help=help_str)


def get_or_create_command_parser(
    root_parser: ArgumentParser, resource_name: str, command_name: str, help_str: Optional[str] = None
) -> ArgumentParser:
    resource_parser = get_or_create_resource_parser(root_parser, resource_name)
    command_subparsers = _get_or_create_subparsers(resource_parser, "command")

    if command_name in command_subparsers.choices:
        return command_subparsers.choices[command_name]

    return command_subparsers.add_parser(command_name, help=help_str)


def _get_or_create_subparsers(parser: ArgumentParser, dest: str, help_str: Optional[str] = None) -> _SubParsersAction:
    for action in parser._actions:
        if isinstance(action, _SubParsersAction) and action.dest == dest:
            return action

    return parser.add_subparsers(dest=dest, help=help_str)


cli_parser = create_cli_parser()


if __name__ == "__main__":
    execute()
