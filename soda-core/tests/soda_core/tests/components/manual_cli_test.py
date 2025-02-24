import sys

from dotenv import load_dotenv

from soda_core.cli.soda import main, configure_logging

if __name__ == "__main__":
    configure_logging(verbose=True)

    project_root_dir = __file__[: -len("/soda-core/tests/soda_core/tests/components/manual_cli_test.py")]
    load_dotenv(f"{project_root_dir}/.env", override=True)

    sys.argv = ["soda", "verify", "-ds", "/Users/tom/Code/ccli/ds2.yml", "-sc", "/Users/tom/Code/ccli/sc.yml", "-c", "/Users/tom/Code/ccli/c2.yml"]
    main()
