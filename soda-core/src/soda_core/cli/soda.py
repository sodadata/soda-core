from __future__ import annotations

import argparse
from textwrap import dedent


def verify_contract(
    contract_file_paths: list[str] | None,
    send_results: bool,
    skip_publish: bool,
    use_agent: bool
):
    print(f"Verifying contracts {contract_file_paths} with ")
    if send_results:
        print(f"  ✓ Sending results to Soda Cloud")
    else:
        print(f"  \u2713 Skip sending results to Soda Cloud")
    # if skip_publish:
    #     print(f"  \N{cross mark button} Skipping publication to Soda Cloud")
    # else:
    #     print(f"  \N{check mark button} Publishing contract to Soda Cloud")
    # if use_agent:
    #     print(f"  \N{check mark button} Using Soda Agent")
    # else:
    #     print(f"  \N{check mark button} Executing locally")


def publish_contract(contract_file_paths: list[str] | None):
    print(
        f"Publishing contracts {contract_file_paths}"
    )


def test_data_source(data_source_name: str):
    print(f"Testing data source {data_source_name}")


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
        "-s", "--send-results",
        const=True,
        action='store_const',
        default=False,
        help="Sends contract verification results to Soda Cloud."
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

    test_parser = sub_parsers.add_parser('test-connection', help='Test a data source connection')
    test_parser.add_argument(
        "-ds", "--data-source",
        type=str,
        help="The name of a configured data source to test."
    )

    args = cli_parser.parse_args()

    try:
        if args.command == "verify":
            # No -c means args.contract is None
            if args.contract is not None:
                verify_contract(args.contract, args.send_results, args.skip_publish, args.use_agent)
            else:
                raise Exception(
                    "No contract provided. Add one or more contracts with option --contract <contract-file-path1> "
                    "<contract-file-path2> ..."
                )
        elif args.command == "publish":
            publish_contract(args.contract)
        elif args.command == "test":
            test_data_source(args.data_source)
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
