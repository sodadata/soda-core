from __future__ import annotations

import logging
from textwrap import dedent

from helpers.data_source_fixture import DataSourceFixture
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract import ContractResult
from soda.contracts.contract_verification import (
    ContractVerification,
    ContractVerificationBuilder,
    ContractVerificationResult,
)
from soda.contracts.impl.data_source import DataSource

# class TestVerificationDataSource(VerificationDataSource):
#     __test__ = False
#
#     def __init__(self, data_source: DataSource):
#         super().__init__()
#         self.data_source = data_source
#         self.data_source_name = data_source.data_source_name
#
#     def requires_with_block(self) -> bool:
#         return False


class TestContractVerificationBuilder(ContractVerificationBuilder):
    __test__ = False

    def __init__(self):
        super().__init__()
        self.data_source = None

    def with_data_source(self, data_source) -> TestContractVerificationBuilder:
        self.data_source = data_source
        return self

    def build(self) -> TestContractVerification:
        return TestContractVerification(self)


class TestContractVerification(ContractVerification):
    __test__ = False

    @classmethod
    def builder(cls) -> TestContractVerificationBuilder:
        return TestContractVerificationBuilder()

    def __init__(self, test_contract_verification_builder: TestContractVerificationBuilder):
        super().__init__(contract_verification_builder=test_contract_verification_builder)

    def _initialize_data_source(self, contract_verification_builder: ContractVerificationBuilder) -> None:
        self.data_source = contract_verification_builder.data_source


class TestDataSource(DataSource):
    __test__ = False

    def __init__(self, data_source_fixture: DataSourceFixture):
        super().__init__()
        self.data_source_fixture = data_source_fixture
        self.sodacl_data_source = data_source_fixture.data_source
        # DataSource field initialization
        self.data_source_name = data_source_fixture.data_source_name
        self.data_source_type = data_source_fixture.data_source.type
        self.dbapi_connection = data_source_fixture.data_source.connection

    def ensure_test_table(self, test_table: TestTable) -> str:
        return self.data_source_fixture.ensure_test_table(test_table=test_table)

    def data_type_text(self) -> str:
        return self.sodacl_data_source.get_sql_type_for_schema_check(DataType.TEXT)

    def data_type_decimal(self) -> str:
        return self.sodacl_data_source.get_sql_type_for_schema_check(DataType.DECIMAL)

    def data_type_integer(self) -> str:
        return self.sodacl_data_source.get_sql_type_for_schema_check(DataType.INTEGER)

    def data_type_date(self) -> str:
        return self.sodacl_data_source.get_sql_type_for_schema_check(DataType.DATE)

    def _create_dbapi_connection(self) -> object:
        # already initialized in constructor
        return self.dbapi_connection

    def __enter__(self) -> DataSource:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def assert_contract_pass(self, contract_yaml_str: str, variables: dict[str, str] | None = None) -> ContractResult:
        contract_yaml_str = dedent(contract_yaml_str)
        logging.debug(contract_yaml_str)
        contract_verification_result: ContractVerificationResult = (
            TestContractVerification.builder()
            .with_data_source(self)
            .with_contract_yaml_str(contract_yaml_str)
            .with_variables(variables)
            .execute()
        )
        if contract_verification_result.failed():
            raise AssertionError(f"Expected contract verification passed, but was: {contract_verification_result}")
        logging.debug(f"Contract result: {contract_verification_result}")
        return contract_verification_result.contract_results[0]

    def assert_contract_fail(self, contract_yaml_str: str, variables: dict[str, str] | None = None) -> ContractResult:
        contract_yaml_str = dedent(contract_yaml_str).strip()
        logging.debug(contract_yaml_str)
        contract_verification_result: ContractVerificationResult = (
            TestContractVerification.builder()
            .with_data_source(self)
            .with_contract_yaml_str(contract_yaml_str)
            .with_variables(variables)
            .execute()
        )
        if not contract_verification_result.failed():
            raise AssertionError(
                f"Expected contract verification failed, but got contract result: {contract_verification_result}"
            )
        logging.debug(f"Contract result: {contract_verification_result}")
        return contract_verification_result.contract_results[0]

        # except SodaException as e:
        #     assert e.contract_result
        #     if e.contract_result.has_execution_errors():
        #         raise AssertionError(str(e.contract_result))
        #     contract_result = e.contract_result

    def assert_contract_error(
        self, contract_yaml_str: str, variables: dict[str, str] | None = None
    ) -> ContractVerificationResult:
        contract_yaml_str = dedent(contract_yaml_str).strip()
        logging.debug(contract_yaml_str)
        contract_verification_result: ContractVerificationResult = (
            TestContractVerification.builder()
            .with_data_source(self)
            .with_contract_yaml_str(contract_yaml_str)
            .with_variables(variables)
            .execute()
        )
        logs_text = "\n".join([str(l) for l in contract_verification_result.logs.logs])
        if not contract_verification_result.has_errors():
            raise AssertionError(f"Expected contract execution errors, but got none. Logs:\n{logs_text}")
        contract_result_str = str(contract_verification_result)
        logging.debug(f"Contract result: {contract_result_str}")
        return contract_verification_result
