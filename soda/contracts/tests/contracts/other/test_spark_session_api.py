import logging
from textwrap import dedent

import pytest
from pyspark.sql import SparkSession

from soda.contracts.contract_verification import (
    ContractVerification,
    ContractVerificationResult,
    SodaException,
)


@pytest.mark.skip("Takes too long to be part of the local development test suite")
def test_spark_session_api():
    spark_session = SparkSession.builder.master("local").appName("test").getOrCreate()

    contract_yaml_str = dedent(
        """
      dataset: CUSTOMERS
      columns:
      - name: id
      - name: size
    """
    )

    try:
        contract_verification_result: ContractVerificationResult = (
            ContractVerification.builder()
            .with_contract_yaml_str(contract_yaml_str)
            .with_data_source_spark_session(spark_session=spark_session)
            .execute()
            .assert_ok()
        )

        print(str(contract_verification_result))

    except SodaException as e:
        # An exception is raised means there are either check failures or contract verification exceptions.
        # Those include:
        # -
        logging.exception(f"Contract verification failed:\n{e}", exc_info=e)
