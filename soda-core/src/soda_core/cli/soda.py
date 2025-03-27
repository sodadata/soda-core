from __future__ import annotations

import logging
import signal
import traceback
from argparse import ArgumentParser
from os.path import dirname, exists
from pathlib import Path
from textwrap import dedent
from typing import Optional

from soda_core.common.logging_configuration import configure_logging
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlFileContent, YamlSource
from soda_core.contracts.contract_publication import ContractPublication
from soda_core.contracts.contract_verification import (
    ContractVerificationSession,
    ContractVerificationSessionResult,
)

logger: logging.Logger = soda_logger


class CLI:
    # https://docs.soda.io/soda-library/programmatic.html#scan-exit-codes
    EXIT_CODE_0_ALL_IS_GOOD = 0
    EXIT_CODE_1_CHECK_FAILURES_OCCURRED = 1
    EXIT_CODE_2_LOG_WARNINGS_OCCURRED = 2
    EXIT_CODE_3_LOG_ERRORS_OCCURRED = 3
    EXIT_CODE_4_RESULTS_NOT_SENT_TO_CLOUD = 4

    def execute(self) -> None:
        try:
            print(r"  __|  _ \|  \   \\")
            print(r"\__ \ (   |   | _ \\")
            print(r"____/\___/___/_/  _\\ CLI 4.0.0.dev??")

            signal.signal(signal.SIGINT, self.handle_ctrl_c)

            cli_parser = self._create_argument_parser("Run 'soda {command} -h' for help on a particular soda command")

            sub_parsers = cli_parser.add_subparsers(dest="command", help="Soda command description")
            verify_parser = sub_parsers.add_parser("verify", help="Verify a contract")

            verify_parser.add_argument("-c", "--contract", type=str, nargs="+", help="One or more contract file paths.")
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
                "-sp",
                "--skip-publish",
                const=True,
                action="store_const",
                default=False,
                help="Skips publishing of the contract when sending results to Soda Cloud.  Precondition: The contract version "
                "must already exist on Soda Cloud.",
            )
            verify_parser.add_argument(
                "-v",
                "--verbose",
                const=True,
                action="store_const",
                default=False,
                help="Show more detailed logs on the console.",
            )

            publish_parser = sub_parsers.add_parser("publish", help="Publish a contract (not yet implemented)")
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

            test_contract_parser = sub_parsers.add_parser(
                name="test-contract", help="Test a contract syntax without executing it"
            )
            test_contract_parser.add_argument(
                "-c", "--contract", type=str, nargs="+", help="One or more contract file paths."
            )
            test_contract_parser.add_argument(
                "-ds", "--data-source", type=str, help="The data source configuration file."
            )

            create_data_source_parser = sub_parsers.add_parser(
                name="create-data-source", help="Create a data source YAML configuration file"
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

            test_parser = sub_parsers.add_parser("test-data-source", help="Test a data source connection")
            test_parser.add_argument(
                "-ds", "--data-source", type=str, help="The name of a configured data source to test."
            )

            create_soda_cloud_parser = sub_parsers.add_parser(
                name="create-soda-cloud", help="Create a Soda Cloud YAML configuration file"
            )
            create_soda_cloud_parser.add_argument(
                "-f",
                "--file",
                type=str,
                help="The path to the file to be created. (directories will be created if needed)",
            )

            test_parser = sub_parsers.add_parser("test-soda-cloud", help="Test the Soda Cloud connection")
            test_parser.add_argument("-sc", "--soda-cloud", type=str, help="A Soda Cloud configuration file path.")

            args = cli_parser.parse_args()

            verbose = args.verbose if hasattr(args, "verbose") else False
            self._configure_logging(verbose)

            if args.command == "verify":
                self._verify_contract(
                    contract_file_paths=args.contract,
                    data_source_file_path=args.data_source,
                    soda_cloud_file_path=args.soda_cloud,
                    skip_publish=args.skip_publish,
                    use_agent=args.use_agent,
                    blocking_timeout_in_minutes=args.blocking_timeout_in_minutes,
                )
            elif args.command == "test-contract":
                self._validate_contract(
                    contract_file_paths=args.contract,
                )
            elif args.command == "publish":
                self._publish_contract(args.contract, args.soda_cloud)
            elif args.command == "create-data-source":
                self._create_data_source(args.file, args.type)
            elif args.command == "test-data-source":
                self._test_data_source(args.data_source)
            elif args.command == "create-soda-cloud":
                self._create_soda_cloud(args.file)
            elif args.command == "test-soda-cloud":
                self._test_soda_cloud(args.soda_cloud)
            else:
                cli_parser.print_help()

        except Exception as e:
            traceback.print_exc()
            self._exit_with_code(self.EXIT_CODE_3_LOG_ERRORS_OCCURRED)

    def _configure_logging(self, verbose: bool) -> None:
        """
        Purpose of this method is to enable override in test environment
        """
        configure_logging(verbose=verbose)

    def _verify_contract(
        self,
        contract_file_paths: Optional[list[str]],
        data_source_file_path: Optional[str],
        soda_cloud_file_path: Optional[str],
        skip_publish: bool,
        use_agent: bool,
        blocking_timeout_in_minutes: int,
    ) -> ContractVerificationSessionResult:
        contract_verification_session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
            contract_yaml_sources=[
                YamlSource.from_file_path(contract_file_path) for contract_file_path in contract_file_paths
            ],
            data_source_yaml_sources=(
                [YamlSource.from_file_path(data_source_file_path)] if data_source_file_path else []
            ),
            soda_cloud_yaml_source=(YamlSource.from_file_path(soda_cloud_file_path) if soda_cloud_file_path else None),
            soda_cloud_skip_publish=skip_publish,
            soda_cloud_use_agent=use_agent,
            soda_cloud_use_agent_blocking_timeout_in_minutes=blocking_timeout_in_minutes,
        )
        if self.contract_verification_is_not_sent_to_cloud(contract_verification_session_result):
            self._exit_with_code(self.EXIT_CODE_4_RESULTS_NOT_SENT_TO_CLOUD)
        elif contract_verification_session_result.has_errors():
            self._exit_with_code(self.EXIT_CODE_3_LOG_ERRORS_OCCURRED)
        elif contract_verification_session_result.is_failed():
            self._exit_with_code(self.EXIT_CODE_1_CHECK_FAILURES_OCCURRED)

        return contract_verification_session_result

    def contract_verification_is_not_sent_to_cloud(
        self, contract_verification_session_result: ContractVerificationSessionResult
    ) -> bool:
        return any(
            cr.sending_results_to_soda_cloud_failed
            for cr in contract_verification_session_result.contract_verification_results
        )

    def _validate_contract(
        self,
        contract_file_paths: Optional[list[str]],
    ):
        for contract_file_path in contract_file_paths:
            contract_verification_session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
                contract_yaml_sources=[YamlSource.from_file_path(contract_file_path)],
                only_validate_without_execute=True,
            )
            if not contract_verification_session_result.has_errors():
                logger.info(f"{Emoticons.WHITE_CHECK_MARK} {contract_file_path} is valid")

    def _publish_contract(
        self,
        contract_file_paths: Optional[list[str]],
        soda_cloud_file_path: Optional[str],
    ):
        contract_publication_builder = ContractPublication.builder()

        for contract_file_path in contract_file_paths:
            contract_publication_builder.with_contract_yaml_file(contract_file_path)

        if soda_cloud_file_path:
            contract_publication_builder.with_soda_cloud_yaml_file(soda_cloud_file_path)

        contract_publication_result = contract_publication_builder.build().execute()

        if contract_publication_result.has_errors():
            self._exit_with_code(self.EXIT_CODE_4_RESULTS_NOT_SENT_TO_CLOUD)

        return contract_publication_result

    def _create_data_source(self, data_source_file_path: str, data_source_type: str):
        print(f"Creating {data_source_type} data source YAML file '{data_source_file_path}'")
        if exists(data_source_file_path):
            print(
                f"Could not create data source file '{data_source_file_path}'. "
                f"File already exists {Emoticons.POLICE_CAR_LIGHT}."
            )
            return
        if data_source_type != "postgres":
            print(f"{Emoticons.POLICE_CAR_LIGHT} Only type postgres is supported atm")
            return
        dir: str = dirname(data_source_file_path)
        Path(dir).mkdir(parents=True, exist_ok=True)
        with open(data_source_file_path, "w") as text_file:
            text_file.write(
                dedent(
                    """
                type: postgres
                name: postgres_ds
                connection:
                    host: localhost
                    user: ${POSTGRES_USERNAME}
                    password: ${POSTGRES_PASSWORD}
                    database: your_postgres_db
                """
                ).strip()
            )
        print(f"{Emoticons.WHITE_CHECK_MARK} Created data source file '{data_source_file_path}'")

    def _test_data_source(self, data_source_file_path: str):
        print(f"Testing data source configuration file {data_source_file_path}")
        from soda_core.common.data_source_impl import DataSourceImpl

        data_source_impl: DataSourceImpl = DataSourceImpl.from_file(data_source_file_path)
        error_message: Optional[str] = (
            data_source_impl.test_connection_error_message()
            if data_source_impl
            else "Data source could not be created. See logs above. Or re-run with -v"
        )
        if error_message:
            print(
                f"{Emoticons.POLICE_CAR_LIGHT} Could not connect using data source '{data_source_file_path}': "
                f"{error_message}"
            )
            self._exit_with_code(self.EXIT_CODE_3_LOG_ERRORS_OCCURRED)
        else:
            print(f"{Emoticons.WHITE_CHECK_MARK} Success! Connection in '{data_source_file_path}' tested ok.")

    def _create_soda_cloud(self, soda_cloud_file_path: str):
        print(f"Creating Soda Cloud YAML file '{soda_cloud_file_path}'")
        if exists(soda_cloud_file_path):
            print(
                f"Could not create soda cloud file '{soda_cloud_file_path}'. "
                f"File already exists {Emoticons.POLICE_CAR_LIGHT}"
            )
        dir: str = dirname(soda_cloud_file_path)
        Path(dir).mkdir(parents=True, exist_ok=True)
        with open(soda_cloud_file_path, "w") as text_file:
            text_file.write(
                dedent(
                    """
                soda_cloud:
                  host: cloud.soda.io
                  api_key_id: ${SODA_CLOUD_API_KEY_ID}
                  api_key_secret: ${SODA_CLOUD_API_KEY_SECRET}
                """
                ).strip()
            )
        print(f"{Emoticons.WHITE_CHECK_MARK} Created Soda Cloud configuration file '{soda_cloud_file_path}'")

    def _test_soda_cloud(self, soda_cloud_file_path: str):
        from soda_core.common.soda_cloud import SodaCloud

        # Start capturing logs
        logs: Logs = Logs()

        print(f"Testing soda cloud file {soda_cloud_file_path}")
        soda_cloud_yaml_source: YamlSource = YamlSource.from_file_path(soda_cloud_file_path)
        soda_cloud_file_content: YamlFileContent = soda_cloud_yaml_source.parse_yaml_file_content(
            file_type="soda_cloud", variables={}
        )
        soda_cloud: SodaCloud = SodaCloud.from_file(soda_cloud_file_content)

        error_msg: Optional[str] = None
        if soda_cloud:
            soda_cloud.test_connection()
            if logs.has_errors():
                error_msg = logs.get_errors_str()
        else:
            error_msg = "Soda Cloud connection could not be created. See logs above. Or re-run with -v"

        if error_msg:
            print(f"{Emoticons.POLICE_CAR_LIGHT} Could not connect to Soda Cloud: {error_msg}")
            self._exit_with_code(self.EXIT_CODE_3_LOG_ERRORS_OCCURRED)
        else:
            print(f"{Emoticons.WHITE_CHECK_MARK} Success! Tested Soda Cloud credentials in '{soda_cloud_file_path}'")

    def _exit_with_code(self, exit_code: int):
        print(f"Exiting with code {exit_code}")
        exit(exit_code)

    def _create_argument_parser(self, epilog: str) -> ArgumentParser:
        return ArgumentParser(epilog="Run 'soda {command} -h' for help on a particular soda command")

    def handle_ctrl_c(self, sig, frame):
        print()
        print(f"CTRL+C detected")
        self._exit_with_code(1)


def main():
    CLI().execute()


if __name__ == "__main__":
    main()
