import logging
from textwrap import dedent

import pytest
from pyspark.sql import SparkSession

from soda.contracts.data_source import Connection
from soda.contracts.contract import Contract, ContractResult, SodaException


@pytest.mark.skip
def test_spark_session_api():
    spark_session = SparkSession.builder.master("local").appName("test").getOrCreate()

    contract_yaml_str = dedent(
        f"""
      dataset: CUSTOMERS
      columns:
      - name: id
        data_type: text
      - name: size
        data_type: decimal
      - name: distance
        data_type: integer
      - name: created
        data_type: date
    """
    )

    try:
        with Connection.from_spark_session(spark_session) as connection:

            # Parsing the contract YAML into a contract python object
            contract: Contract = Contract.from_yaml_str(contract_yaml_str)

            contract_result: ContractResult = contract.verify(connection)

            # This place in the code means contract verification has passed successfully:
            # No exceptions means there are no contract execution exceptions and no check failures.

            # The default way to visualize diagnostics information for checks is Soda Cloud.
            # But contract results information can be transformed and sent to any destination
            # (contract_results includes information like eg the check results and diagnostics information)
            logging.debug(f"Contract verification passed:\n{contract_result}")

    except SodaException as e:
        # An exception is raised means there are either check failures or contract verification exceptions.
        # Those include:
        # -
        logging.exception(f"Contract verification failed:\n{e}", exc_info=e)
