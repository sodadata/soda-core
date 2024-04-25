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
from soda.contracts.impl.contract_verification_impl import VerificationWarehouse
from soda.contracts.impl.warehouse import Warehouse


class TestVerificationWarehouse(VerificationWarehouse):
    def __init__(self, warehouse: Warehouse):
        super().__init__()
        self.warehouse = warehouse
        self.warehouse_name = warehouse.warehouse_name

    def requires_with_block(self) -> bool:
        return False


class TestContractVerificationBuilder(ContractVerificationBuilder):

    def __init__(self):
        super().__init__()
        self.warehouse = None

    def with_warehouse(self, warehouse) -> TestContractVerificationBuilder:
        self.warehouse = warehouse
        return self

    def build(self) -> TestContractVerification:
        return TestContractVerification(self)


class TestContractVerification(ContractVerification):

    @classmethod
    def builder(cls) -> TestContractVerificationBuilder:
        return TestContractVerificationBuilder()

    def __init__(self, test_contract_verification_builder: TestContractVerificationBuilder):
        super().__init__(contract_verification_builder=test_contract_verification_builder)

    def _initialize_verification_warehouses(
        self, contract_verification_builder: TestContractVerificationBuilder
    ) -> None:
        super()._initialize_verification_warehouses(contract_verification_builder)
        warehouse: Warehouse = contract_verification_builder.warehouse
        warehouse_name: str = warehouse.warehouse_name
        self.verification_warehouses_by_name[warehouse_name] = TestVerificationWarehouse(warehouse)


class TestWarehouse(Warehouse):

    def __init__(self, data_source_fixture: DataSourceFixture):
        super().__init__()
        self.warehouse_fixture = data_source_fixture
        self.sodacl_data_source = data_source_fixture.data_source
        # Warehouse field initialization
        self.warehouse_name = data_source_fixture.data_source_name
        self.warehouse_type = data_source_fixture.data_source.type
        self.dbapi_connection = data_source_fixture.data_source.connection

    def ensure_test_table(self, test_table: TestTable) -> str:
        return self.warehouse_fixture.ensure_test_table(test_table=test_table)

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

    def assert_contract_pass(self, contract_yaml_str: str, variables: dict[str, str] | None = None) -> ContractResult:
        contract_yaml_str = dedent(contract_yaml_str)
        logging.debug(contract_yaml_str)
        contract_verification_result: ContractVerificationResult = (
            TestContractVerification.builder()
            .with_warehouse(self)
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
            .with_warehouse(self)
            .with_contract_yaml_str(contract_yaml_str)
            .with_variables(variables)
            .execute()
        )
        if not contract_verification_result.failed():
            raise AssertionError(
                f"Expected contract verification exception, but got contract result: {contract_verification_result}"
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
            .with_warehouse(self)
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
