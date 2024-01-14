from textwrap import dedent

from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from soda.contracts.connection import Connection, SodaException
from soda.contracts.contract import Contract, ContractResult, CheckResult


def test_contract_schema_pass(data_source_fixture: DataSourceFixture, connection: Connection):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    contract: Contract = Contract.from_yaml_str(dedent(f"""
      schema: {data_source_fixture.schema_name}
      dataset: {table_name}
      columns:
      - name: id
        data_type: text
      - name: cst_size
        data_type: numeric
      - name: cst_size_txt
        data_type: text
      - name: distance
        data_type: integer
      - name: pct
        data_type: text
      - name: cat
        data_type: text
      - name: country
        data_type: text
      - name: zip
        data_type: text
      - name: email
        data_type: text
      - name: date_updated
        data_type: date
      - name: ts
        data_type: timestamp without time zone
      - name: ts_with_tz
        data_type: timestamp with time zone
    """))

    contract_result: ContractResult = contract.verify(connection)


def test_contract_schema_column_missing(data_source_fixture: DataSourceFixture, connection: Connection):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    contract: Contract = Contract.from_yaml_str(dedent(f"""
      schema: {data_source_fixture.schema_name}
      dataset: {table_name}
      columns:
      - name: id
        data_type: text
      - name: cst_size
        data_type: numeric
      - name: cst_size_txt
        data_type: text
      - name: distance
        data_type: integer
      - name: pct
        data_type: text
      - name: cat
        data_type: text
      # - name: country
      #   data_type: text
      - name: zip
        data_type: text
      - name: email
        data_type: text
      - name: date_updated
        data_type: date
      - name: ts
        data_type: timestamp without time zone
      - name: ts_with_tz
        data_type: timestamp with time zone
    """))

    try:
        contract.verify(connection)
        raise AssertionError("Expected contract verification exception")
    except SodaException as e:
        contract_result: ContractResult = e.contract_result
        assert contract_result.has_problems()
        schema_check_result: CheckResult = contract_result.check_results[0]
