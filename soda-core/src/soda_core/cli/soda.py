from __future__ import annotations

import sys

from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import handle_publish_contract, handle_verify_contract, handle_test_contract
from soda_core.cli.handlers.data_source import handle_create_data_source, handle_test_data_source
from soda_core.cli.handlers.soda_cloud import handle_create_soda_cloud, handle_test_soda_cloud
from soda_core.common.logging_configuration import configure_logging
from soda_core.common.logging_constants import soda_logger

import signal
import traceback
from argparse import ArgumentParser


def execute() -> None:
    try:
        print(r"  __|  _ \|  \   \\")
        print(r"\__ \ (   |   | _ \\")
        print(r"____/\___/___/_/  _\\ CLI 4.0.0.dev??")

        signal.signal(signal.SIGINT, handle_ctrl_c)

        cli_parser = create_cli_parser()

        args = cli_parser.parse_args()

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


def create_cli_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog="soda",
        epilog="Run 'soda {resource} {command} -h' for help on a specific command",
    )
    resource_parsers = parser.add_subparsers(dest="resource", help="Available Soda resources")

    _setup_contract_resource(resource_parsers)
    _setup_data_source_resource(resource_parsers)
    _setup_soda_cloud_resource(resource_parsers)

    return parser


def _setup_contract_resource(resource_parsers) -> None:
    contract_parser = resource_parsers.add_parser("contract", help="Contract commands")
    contract_subparsers = contract_parser.add_subparsers(dest="command", help="Contract commands")

    _setup_contract_verify_command(contract_subparsers)
    _setup_contract_publish_command(contract_subparsers)
    _setup_contract_test_command(contract_subparsers)


def _setup_contract_verify_command(contract_parsers) -> None:
    verify_parser = contract_parsers.add_parser("verify", help="Verify a contract")

    verify_parser.add_argument(
        "-c", "--contract",
        type=str,
        nargs="+",
        help="One or more contract file paths. Use this to work with local contracts.",
    )
    verify_parser.add_argument(
        "-d",
        "--dataset",
        type=str,
        nargs="+",
        help="Names of datasets to verify. Use this to work with remote contracts present in Soda Cloud.",
    )

    verify_parser.add_argument("-ds", "--data-source", type=str, help="The data source configuration file.")
    verify_parser.add_argument("-sc", "--soda-cloud", type=str, help="A Soda Cloud configuration file path.")
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

    def handle(args):
        contract_file_paths = args.contract
        dataset_identifiers = args.dataset
        data_source_file_path = args.data_source
        soda_cloud_file_path = args.soda_cloud
        publish = args.publish
        use_agent = args.use_agent
        blocking_timeout_in_minutes = args.blocking_timeout_in_minutes

        exit_code = handle_verify_contract(
            contract_file_paths,
            dataset_identifiers,
            data_source_file_path,
            soda_cloud_file_path,
            publish,
            use_agent,
            blocking_timeout_in_minutes,
        )

        exit_with_code(exit_code)

    verify_parser.set_defaults(handler_func=handle)


def _setup_contract_publish_command(contract_parsers) -> None:
    publish_parser = contract_parsers.add_parser("publish", help="Publish a contract")
    publish_parser.add_argument(
        "-c", "--contract", type=str, nargs="+", help="One or more contract file paths."
    )

    publish_parser.add_argument(
        "-sc",
        "--soda-cloud",
        type=str,
        help="A Soda Cloud configuration file path.",
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
        contract_file_paths = args.contract
        soda_cloud_file_path = args.soda_cloud
        exit_code = handle_publish_contract(contract_file_paths, soda_cloud_file_path)
        exit_with_code(exit_code)

    publish_parser.set_defaults(handler_func=handle)


def _setup_contract_test_command(contract_parsers) -> None:
    test_contract_parser = contract_parsers.add_parser(
        name="test", help="Test a contract syntax without executing it"
    )
    test_contract_parser.add_argument(
        "-c", "--contract", type=str, nargs="+", help="One or more contract file paths."
    )
    test_contract_parser.add_argument(
        "-ds", "--data-source", type=str, help="The data source configuration file."
    )

    test_contract_parser.add_argument(
        "-v",
        "--verbose",
        const=True,
        action="store_const",
        default=False,
        help="Show more detailed logs on the console.",
    )

    def handle(args):
        contract_file_paths = args.contract
        data_source_file_path = args.data_source

        exit_code = handle_test_contract(contract_file_paths, data_source_file_path)
        exit_with_code(exit_code)

    test_contract_parser.set_defaults(handler_func=handle)


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
        help="The path to the file to be created. (directories will be created if needed)",
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
    test_parser.add_argument(
        "-ds", "--data-source", type=str, help="The name of a configured data source to test."
    )

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
        help="The path to the file to be created. (directories will be created if needed)",
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
    test_soda_cloud_parser.add_argument("-sc", "--soda-cloud", type=str, help="A Soda Cloud configuration file path.")

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


def exit_with_code(exit_code: int):
    soda_logger.debug(f"Exiting with code {exit_code}")
    exit(exit_code)


def handle_ctrl_c(self, sig, frame):
    soda_logger.info("")
    soda_logger.info(f"CTRL+C detected")
    exit_with_code(ExitCode.LOG_ERRORS)

if __name__ == "__main__":
    execute()
