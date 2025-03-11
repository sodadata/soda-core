import sys
import tempfile
from argparse import ArgumentParser
from typing import Optional

from soda_core.cli.soda import CLI
from soda_core.contracts.contract_verification import (
    ContractResult,
    ContractVerificationResult,
)
from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.tests.helpers.test_functions import dedent_and_strip
from soda_core.tests.helpers.test_table import TestTableSpecification


class CLI4Test(CLI):
    def __init__(self, argv: list[str]):
        super().__init__()
        self.contract_file_paths: Optional[list[str]] = None
        self.data_source_file_path: Optional[str] = None
        self.soda_cloud_file_path: Optional[str] = None
        self.skip_publish: Optional[bool] = None
        self.use_agent: Optional[bool] = None
        self.exit_code: int = 0
        self.argv: list[str] = argv
        self.contract_verification_result: Optional[ContractVerificationResult] = None

    def execute(self) -> None:
        sys.argv = self.argv
        super().execute()

    def _verify_contract(
        self,
        contract_file_paths: Optional[list[str]],
        data_source_file_path: Optional[str],
        soda_cloud_file_path: Optional[str],
        skip_publish: bool,
        use_agent: bool,
        blocking_timeout_in_minutes: int,
    ):
        self.contract_file_paths = contract_file_paths
        self.data_source_file_path = data_source_file_path
        self.soda_cloud_file_path = soda_cloud_file_path
        self.skip_publish = skip_publish
        self.use_agent = use_agent

        self.contract_verification_result = super()._verify_contract(
            contract_file_paths=contract_file_paths,
            data_source_file_path=data_source_file_path,
            soda_cloud_file_path=soda_cloud_file_path,
            skip_publish=skip_publish,
            use_agent=use_agent,
            blocking_timeout_in_minutes=blocking_timeout_in_minutes,
        )

    def _configure_logging(self, verbose: bool):
        pass

    def _end_with_exit_code(self, exit_code: int) -> None:
        self.exit_code = exit_code

    def _create_argument_parser(self, epilog: str) -> ArgumentParser:
        return ArgumentParser4Test(epilog=epilog)


class ArgumentParser4Test(ArgumentParser):
    def exit(self, status=0, message=None):
        print(f"Skipping exit in unit test status={status}, message={message}")


test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("cli")
    .column_text("id")
    .rows(
        rows=[
            ("1",),
            ("2",),
            ("3",),
        ]
    )
    .build()
)


def test_cli(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_yaml_str: str = dedent_and_strip(
        f"""
        data_source: postgres_test_ds
        dataset: {test_table.unique_name}
        dataset_prefix: {data_source_test_helper.dataset_prefix}
        columns:
          - name: id
        checks:
          - row_count:
              threshold:
                must_be: 3
          - schema:
    """
    )

    data_source_yaml_str: str = dedent_and_strip(
        """
        type: postgres
        name: postgres_test_ds
        connection:
          host: localhost
          user: soda_test
          database: soda_test
    """
    )

    contract_tmp_file = tempfile.NamedTemporaryFile()
    with open(contract_tmp_file.name, "w") as f:
        f.write(contract_yaml_str)

    data_source_tmp_file = tempfile.NamedTemporaryFile()
    with open(data_source_tmp_file.name, "w") as f:
        f.write(data_source_yaml_str)

    test_cli: CLI4Test = CLI4Test(["soda", "verify", "-ds", data_source_tmp_file.name, "-c", contract_tmp_file.name])
    test_cli.execute()
    assert test_cli.exit_code == 0
    assert test_cli.contract_verification_result.is_ok()
    contract_result: ContractResult = test_cli.contract_verification_result.contract_results[0]
    assert len(contract_result.check_results) == 2


def test_cli_wrong_pwd(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_yaml_str: str = dedent_and_strip(
        f"""
        data_source: postgres_test_ds
        dataset: {test_table.unique_name}
        dataset_prefix: {data_source_test_helper.dataset_prefix}
        columns:
          - name: id
        checks:
          - row_count:
              threshold:
                must_be: 3
          - schema:
    """
    )

    data_source_yaml_str: str = dedent_and_strip(
        """
        type: postgres
        name: postgres_test_ds
        connection:
          host: localhost
          user: wrongpwd!
          database: soda_test
    """
    )

    contract_tmp_file = tempfile.NamedTemporaryFile()
    with open(contract_tmp_file.name, "w") as f:
        f.write(contract_yaml_str)

    data_source_tmp_file = tempfile.NamedTemporaryFile()
    with open(data_source_tmp_file.name, "w") as f:
        f.write(data_source_yaml_str)

    test_cli: CLI4Test = CLI4Test(["soda", "verify", "-ds", data_source_tmp_file.name, "-c", contract_tmp_file.name])
    test_cli.execute()
