from __future__ import annotations

import logging
import os
from datetime import date
from textwrap import dedent

from contracts.helpers.test_warehouse import TestWarehouse
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract_verification import (
    ContractVerification,
    ContractVerificationResult,
)

contracts_api_test_table = TestTable(
    name="contracts_api",
    columns=[
        ("id", DataType.TEXT),
        ("size", DataType.DECIMAL),
        ("distance", DataType.INTEGER),
        ("created", DataType.DATE),
    ],
    # fmt: off
    values=[
        ('ID1',  1,    0,       date(2020, 6, 23)),
        ('N/A',  1,    None,    date(2020, 6, 23)),
        (None,   1,    None,    date(2020, 6, 23)),
    ]
    # fmt: on
)


def test_contract_verification_api(test_warehouse: TestWarehouse, environ: dict):
    table_name: str = test_warehouse.ensure_test_table(contracts_api_test_table)

    environ["USERNAME"] = "sodasql"
    environ["PORT"] = os.getenv("POSTGRES_PORT", "5432")

    warehouse_yaml_str = dedent(
        """
        name: postgres_ds
        type: postgres
        connection:
            host: localhost
            database: sodasql
            username: ${USERNAME}
            port: ${PORT}
    """
    )

    contract_yaml_str = dedent(
        """
      dataset: ${TABLE_NAME}
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

    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder()
        .with_contract_yaml_str(contract_yaml_str)
        .with_warehouse_yaml_str(warehouse_yaml_str)
        .with_variables({"TABLE_NAME": table_name})
        .execute()
        .assert_ok()
    )

    logging.debug(str(contract_verification_result))
