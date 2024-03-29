import logging
from textwrap import dedent

import pytest
from pyspark.sql import SparkSession

from soda.contracts.contract import Contract, ContractResult
from soda.contracts.contract_verification import ContractVerification, SodaException


@pytest.mark.skip
def test_spark_session_api():
    spark_session = SparkSession.builder.master("local").appName("test").getOrCreate()

    contract_yaml_str = dedent(
        f"""
      dataset: CUSTOMERS
      columns:
      - name: id
      - name: size
    """
    )

    try:
        (
            ContractVerification()
            .with_contract_yaml_str(contract_yaml_str)
            .with_data_source_spark_session(spark_session=spark_session, data_source_name="spark_ds")
            .execute()
            .assert_no_problems()
        )

    except SodaException as e:
        # An exception is raised means there are either check failures or contract verification exceptions.
        # Those include:
        # -
        logging.exception(f"Contract verification failed:\n{e}", exc_info=e)
