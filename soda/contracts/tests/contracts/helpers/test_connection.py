import logging
from textwrap import dedent

from helpers.data_source_fixture import DataSourceFixture
from helpers.test_table import TestTable
from soda.contracts.connection import Connection, SodaException
from soda.contracts.contract import ContractResult, Contract
from soda.execution.data_type import DataType


class TestConnection(Connection):

    def __init__(self, data_source_fixture: DataSourceFixture):
        super().__init__(dbapi_connection=data_source_fixture.data_source.connection)
        self.data_source_fixture = data_source_fixture
        self.data_source = data_source_fixture.data_source

    def ensure_test_table(self, test_table: TestTable) -> str:
        return self.data_source_fixture.ensure_test_table(test_table=test_table)

    def data_type_text(self) -> str:
        return self.data_source.get_sql_type_for_schema_check(DataType.TEXT)

    def data_type_decimal(self) -> str:
        return self.data_source.get_sql_type_for_schema_check(DataType.DECIMAL)

    def data_type_integer(self) -> str:
        return self.data_source.get_sql_type_for_schema_check(DataType.INTEGER)

    def data_type_date(self) -> str:
        return self.data_source.get_sql_type_for_schema_check(DataType.DATE)

    def assert_contract_pass(self, contract_yaml_str: str) -> ContractResult:
        contract: Contract = Contract.from_yaml_str(dedent(contract_yaml_str))
        contract_result: ContractResult = contract.verify(self)
        assert not contract_result.has_problems()
        contract_result_str = str(contract_result)
        logging.debug(f"Contract result: {contract_result_str}")
        assert contract_result_str == "All is good. No checks failed. No contract execution errors."
        return contract_result

    def assert_contract_fail(self, contract_yaml_str: str) -> ContractResult:
        contract_yaml_str = dedent(contract_yaml_str).strip()
        logging.debug(contract_yaml_str)
        contract: Contract = Contract.from_yaml_str(contract_yaml_str)
        try:
            contract_result: ContractResult = contract.verify(self)
            raise AssertionError(f"Expected contract verification exception, but got contract result: {contract_result}")
        except SodaException as e:
            assert e.contract_result
            assert e.contract_result.has_problems()
            contract_result = e.contract_result
        contract_result_str = str(contract_result)
        logging.debug(f"Contract result: {contract_result_str}")
        return contract_result
