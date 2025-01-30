import argparse
from textwrap import dedent


def verify_contract(contract_file_path: str):
    print(f"verifying contract {contract_file_path}")


def main():
    print(dedent("""
          __|  _ \ _ \   \\
        \__ \ (   ||  | _ \\
        ____/\___/___/_/  _\\ 4.0.0b1
    """))

    cli_parser = argparse.ArgumentParser(
        description="The Soda CLI"
    )

    sub_parsers = cli_parser.add_subparsers(dest="command", help='Subcommand description')
    parser = sub_parsers.add_parser('verify', help='Verify a contract')

    parser.add_argument(
        "-c", "--contract",
        type=str,
        help="A contract file path. Prepend -c for each contract file you specify."
    )
    parser.add_argument(
        "-s", "--send-results",
        const=True,
        action='store_const',
        help="Sends contract verification results to Soda Cloud."
    )
    parser.add_argument(
        "-a", "--use-agent",
        const=True,
        action='store_const',
        help="Executes contract verification on Soda Agent instead of locally in this library."
    )
    parser.add_argument(
        "-sp", "--skip-publish",
        const=True,
        action='store_const',
        help="Skips publishing of the contract when sending results to Soda Cloud.  Precondition: The contract version "
             "must already exist on Soda Cloud."
    )

    parser = sub_parsers.add_parser('publish', help='Publish a contract')
    parser.add_argument(
        "-c", "--contract",
        type=str,
        help="A contract file path. Prepend -c for each contract file you specify."
    )

    parser = sub_parsers.add_parser('help', help='Soda CLI help')

    args = cli_parser.parse_args()

    try:
        if args.command == "verify":
            verify_contract(args.contract)
        else:
            cli_parser.print_help()
    except Exception as e:
        cli_parser.print_help()
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
