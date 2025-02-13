from __future__ import annotations

import argparse
import logging
import sys
from os.path import dirname, exists
from pathlib import Path
from textwrap import dedent
from typing import Optional

from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlFileContent, YamlSource
from soda_core.contracts.contract_verification import ContractVerification, ContractVerificationBuilder, \
    ContractVerificationResult


def configure_logging():
    sys.stderr = sys.stdout
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("pyathena").setLevel(logging.WARNING)
    logging.getLogger("faker").setLevel(logging.ERROR)
    logging.getLogger("snowflake").setLevel(logging.WARNING)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    logging.getLogger("pyhive").setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.INFO)
    logging.getLogger("segment").setLevel(logging.WARNING)
    logging.basicConfig(
        level=logging.DEBUG,
        force=True,  # Override any previously set handlers.
        # https://docs.python.org/3/library/logging.html#logrecord-attributes
        # %(name)s
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def verify_contract(
    contract_file_paths: list[str] | None,
    data_source_file_path: str | None,
    soda_cloud_file_path: str | None,
    skip_publish: bool,
    use_agent: bool
):
    contract_verification_builder: ContractVerificationBuilder = ContractVerification.builder()

    if contract_file_paths is None or len(contract_file_paths) == 0:
        print(f"\U0001F92F I'm suppose to verify a contract but no contract file specified")
        return

    print(f"Verifying...")
    for contract_file_path in contract_file_paths:
        print(f"\U0001F4DC contract {contract_file_path}")
        contract_verification_builder.with_contract_yaml_file(contract_file_path)

    if use_agent:
        if soda_cloud_file_path:
            print(f"\U0001F325 on Soda Agent")
        else:
            print(f"\U0001F92F I'm suppose to verify the contract on Soda Agent but no Soda Cloud configured")
            return
    else:
        print(f"\U0001F4BB locally")
        if data_source_file_path:
            print(f"\u2705 on data source {data_source_file_path} \U0001F4BE")
            contract_verification_builder.with_data_source_yaml_file(data_source_file_path)

    if soda_cloud_file_path:
        print(f"\u2705 Sending results to Soda Cloud \U0001F4AD")
        contract_verification_builder.with_soda_cloud_yaml_file(soda_cloud_file_path)
        if skip_publish:
            print(f"\u274C Not publishing the contract on Soda Cloud \U0001F4AD")
            contract_verification_builder.with_soda_cloud_skip_publish()
        else:
            print(f"\u2705 Publishing contract to Soda Cloud \U0001F4AD")
    else:
        print(f"\u274C Not sending results to Soda Cloud \U0001F4AD")

    contract_verification_result: ContractVerificationResult = contract_verification_builder.execute()
    print(str(contract_verification_result))


def publish_contract(contract_file_paths: list[str] | None):
    print(
        f"Publishing contracts {contract_file_paths}"
    )


def create_data_source(data_source_file_path: str, data_source_type: str):
    print(f"Creating {data_source_type} data source YAML file '{data_source_file_path}'")
    if exists(data_source_file_path):
        print(f"\U0001F92F I'm suppose to create data source file '{data_source_file_path}' but it already exists")
        return
    if data_source_type!= "postgres":
        print(f"\U0001F92F Only type postgres is supported atm")
        return
    dir: str = dirname(data_source_file_path)
    Path(dir).mkdir(parents=True, exist_ok=True)
    with open(data_source_file_path, "w") as text_file:
        text_file.write(dedent(
            """
            type: postgres
            name: postgres_ds
            connection:
                host: localhost
                user: ${POSTGRES_USERNAME}
                password: ${POSTGRES_PASSWORD}
                database: your_postgres_db,
            format_regexes:
              # Example named regex format
              single_digit_test_format: ^[0-9]$
            """
        ).strip())
    print(f"\u2705 Created data source file '{data_source_file_path}'")


def test_data_source(data_source_file_path: str):
    print(f"Testing data source configuration file {data_source_file_path}")
    from soda_core.common.data_source import DataSource
    data_source: DataSource = DataSource.from_file(data_source_file_path)
    error_message: Optional[str] = data_source.test_connection_error_message()
    if error_message:
        print(f"\U0001F92F Error: Connection configured in data source file '{data_source_file_path}' failed: {error_message}")
    else:
        print(f"\u2705 Success! Tested data source connection in '{data_source_file_path}'")


def create_soda_cloud(soda_cloud_file_path: str):
    print(f"Creating Soda Cloud YAML file '{soda_cloud_file_path}'")
    if exists(soda_cloud_file_path):
        print(f"\U0001F92F I'm suppose to create soda cloud file '{soda_cloud_file_path}' but it already exists")
    dir: str = dirname(soda_cloud_file_path)
    Path(dir).mkdir(parents=True, exist_ok=True)
    with open(soda_cloud_file_path, "w") as text_file:
        text_file.write(dedent(
            """
            soda_cloud:
              host: cloud.soda.io
              api_key_id: ${SODA_CLOUD_API_KEY_ID}
              api_key_secret: ${SODA_CLOUD_API_KEY_SECRET}
            """
        ).strip())
    print(f"\u2705 Created Soda Cloud configuration file '{soda_cloud_file_path}'")


def test_soda_cloud(soda_cloud_file_path: str):
    from soda_core.common.soda_cloud import SodaCloud
    print(f"Testing soda cloud file {soda_cloud_file_path}")
    soda_cloud_yaml_source: YamlSource = YamlSource.from_file_path(soda_cloud_file_path)
    soda_cloud_file_content: YamlFileContent = soda_cloud_yaml_source.parse_yaml_file_content(
        file_type="soda_cloud", variables={}, logs=Logs()
    )
    soda_cloud: SodaCloud = SodaCloud.from_file(soda_cloud_file_content)
    error_msg = soda_cloud.test_connection()
    if error_msg:
        print(f"\U0001F92F Could not connect to Soda Cloud: {error_msg}")
    else:
        print(f"\u2705 Success! Tested Soda Cloud credentials in '{soda_cloud_file_path}'")


def main():
    configure_logging()

    print(dedent("""
          __|  _ \|  \   \\
        \__ \ (   |   | _ \\
        ____/\___/___/_/  _\\ CLI 4.0.0b1
    """).strip("\n"))

    cli_parser = argparse.ArgumentParser(epilog="Run 'soda {command} -h' for help on a particular soda command")

    sub_parsers = cli_parser.add_subparsers(dest="command", help='Soda command description')
    verify_parser = sub_parsers.add_parser('verify', help='Verify a contract')

    verify_parser.add_argument(
        "-c", "--contract",
        type=str,
        nargs='+',
        help="One or more contract file paths."
    )
    verify_parser.add_argument(
        "-ds", "--data-source",
        type=str,
        help="The data source configuration file."
    )
    verify_parser.add_argument(
        "-sc", "--soda-cloud",
        type=str,
        help="A Soda Cloud configuration file path."
    )
    verify_parser.add_argument(
        "-a", "--use-agent",
        const=True,
        action='store_const',
        default=False,
        help="Executes contract verification on Soda Agent instead of locally in this library."
    )
    verify_parser.add_argument(
        "-sp", "--skip-publish",
        const=True,
        action='store_const',
        default=False,
        help="Skips publishing of the contract when sending results to Soda Cloud.  Precondition: The contract version "
             "must already exist on Soda Cloud."
    )

    publish_parser = sub_parsers.add_parser('publish', help='Publish a contract')
    publish_parser.add_argument(
        "-c", "--contract",
        type=str,
        nargs='+',
        help="One or more contract file paths."
    )

    create_data_source_parser = sub_parsers.add_parser(
        name="create-data-source",
        help="Create a data source YAML configuration file"
    )
    create_data_source_parser.add_argument(
        "-f", "--file",
        type=str,
        help="The path to the file to be created. (directories will be created if needed)"
    )
    create_data_source_parser.add_argument(
        "-t", "--type",
        type=str,
        default="postgres",
        help="Type of the data source.  Eg postgres"
    )

    test_parser = sub_parsers.add_parser('test-data-source', help='Test a data source connection')
    test_parser.add_argument(
        "-ds", "--data-source",
        type=str,
        help="The name of a configured data source to test."
    )
    test_parser = sub_parsers.add_parser('test-data-source', help='Test a data source connection')
    test_parser.add_argument(
        "-ds", "--data-source",
        type=str,
        help="The name of a configured data source to test."
    )

    create_soda_cloud_parser = sub_parsers.add_parser(
        name="create-soda-cloud",
        help="Create a Soda Cloud YAML configuration file"
    )
    create_soda_cloud_parser.add_argument(
        "-f", "--file",
        type=str,
        help="The path to the file to be created. (directories will be created if needed)"
    )

    test_parser = sub_parsers.add_parser('test-soda-cloud', help='Test the Soda Cloud connection')
    test_parser.add_argument(
        "-sc", "--soda-cloud",
        type=str,
        help="A Soda Cloud configuration file path."
    )

    args = cli_parser.parse_args()

    try:
        if args.command == "verify":
            verify_contract(args.contract, args.data_source, args.soda_cloud, args.skip_publish, args.use_agent)
        elif args.command == "publish":
            publish_contract(args.contract)
        elif args.command == "create-data-source":
            create_data_source(args.file, args.type)
        elif args.command == "test-data-source":
            test_data_source(args.data_source)
        elif args.command == "create-soda-cloud":
            create_soda_cloud(args.file)
        elif args.command == "test-soda-cloud":
            test_soda_cloud(args.soda_cloud)
        else:
            cli_parser.print_help()
    except Exception as e:
        cli_parser.print_help()
        print()
        print(f"Error: {e}")
        exit(1)
    exit(0)


if __name__ == "__main__":
    main()
