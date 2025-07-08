from textwrap import dedent, indent

import pytest

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    ContractVerificationResult,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("row_count")
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


def test_row_count(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    # data_source_test_helper.enable_soda_cloud_mock(
    #     [
    #         MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
    #     ]
    # )

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - row_count:
                    threshold:
                        must_be: 3
        """,
    )

    # soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    # check_json: dict = soda_core_insert_scan_results_command["checks"][0]
    # assert check_json["diagnostics"]["v4"] == {
    #     "type": "row_count",
    #     "datasetRowsTested": 3,
    #     "checkRowsTested": 3,
    # }
