import pytest
from contracts.helpers.test_data_source import TestDataSource
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract import ContractResult

contracts_spark_partitioning_test_table = TestTable(
    name="contracts_spark_partitioning",
    columns=[
        ("id", DataType.TEXT),
    ],
    # fmt: off
    values=[ ('1',), ('2',), ('3',), ('4',), ('5',), ('6',) ]
    # fmt: on
)


@pytest.mark.skip("Takes too long to be part of the local development test suite")
def test_spark_partitionind_columns(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(contracts_spark_partitioning_test_table)

    spark_session = test_data_source.sodacl_data_source.spark_session

    spark_session.sql(
        f"""
        DROP TABLE IF EXISTS customer;
    """
    )

    spark_session.sql(
        f"""
        CREATE TABLE customer(
            cust_id INT,
            state VARCHAR(20),
            name STRING COMMENT 'Short name'
        )
        USING PARQUET
        PARTITIONED BY (state);
    """
    )

    cols_df = spark_session.sql(
        f"""
            DESCRIBE TABLE customer
        """
    )
    cols_df.show()

    data_df = spark_session.sql(f"SELECT * FROM customer;")
    data_df.show()

    table_df = spark_session.table("customer")
    for field in table_df.schema.fields:
        print(field.dataType.simpleString())
        print(field.name)

    contract_result: ContractResult = test_data_source.assert_contract_pass(
        contract_yaml_str=f"""
        dataset: customer
        columns:
          - name: cust_id
          - name: state
          - name: name
    """
    )
    contract_result_str = str(contract_result)
    print(contract_result_str)
