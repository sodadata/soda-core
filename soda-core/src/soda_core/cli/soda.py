from __future__ import annotations

import argparse
from textwrap import dedent

from soda_core.contracts.contract_verification import ContractVerification, ContractVerificationBuilder


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
    elif len(contract_file_paths) == 1:
        print(f"Verifying contract {contract_file_paths}")
    else:
        print(f"Verifying contracts")
        for contract_file_path in contract_file_paths:
            print(f"  \U0001F4DC {contract_file_path}")
            contract_verification_builder.with_contract_yaml_file(contract_file_path)

    if use_agent:
        if soda_cloud_file_path:
            print(f"on Soda Agent \U0001F325")
        else:
            print(f"\U0001F92F I'm suppose to verify the contract on Soda Agent but no Soda Cloud configured")
            return
    else:
        print(f"locally \U0001F4BB")
        if data_source_file_path:
            print(f"\U0001F92F on data source {data_source_file_path}")
            contract_verification_builder.with_data_source_yaml_file(data_source_file_path)

    if soda_cloud_file_path:
        print(f"\u2705 Sending results to Soda Cloud \U0001F325")
        contract_verification_builder.with_soda_cloud_yaml_file(soda_cloud_file_path)
        if skip_publish:
            print(f"\u274C Not publishing the contract on Soda Cloud")
            contract_verification_builder.skip_publish()
        else:
            print(f"\u2705 Publishing contract to Soda Cloud \U0001F325")
    else:
        print(f"\u274C Not sending results to Soda Cloud")


def publish_contract(contract_file_paths: list[str] | None):
    print(
        f"Publishing contracts {contract_file_paths}"
    )


def test_data_source(data_source_name: str):
    print(f"Testing data source {data_source_name}")


def test_soda_cloud(soda_cloud_path: str):
    print(f"Testing soda cloud {soda_cloud_path}")


def main():
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

    test_parser = sub_parsers.add_parser('test-data-source', help='Test a data source connection')
    test_parser.add_argument(
        "-ds", "--data-source",
        type=str,
        help="The name of a configured data source to test."
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
            verify_contract(args.contract, args.soda_cloud, args.skip_publish, args.use_agent)
        elif args.command == "publish":
            publish_contract(args.contract)
        elif args.command == "test-data-source":
            test_data_source(args.data_source)
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
