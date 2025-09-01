from unittest.mock import MagicMock, patch

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("contract_verification_handler")
    .column_varchar("id")
    .rows(
        rows=[
            ("1",),
            ("2",),
            ("3",),
        ]
    )
    .build()
)


@patch("soda_core.contracts.impl.contract_verification_impl.ContractVerificationHandler.instance")
def test_failure_in_contract_verification_handler_does_not_fail_scan(
    mock_instance, data_source_test_helper: DataSourceTestHelper, caplog
):
    mock_handler = MagicMock()
    mock_handler.handle.side_effect = Exception("Simulated failure in ContractVerificationHandler")
    mock_instance.return_value = mock_handler

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - row_count:
        """,
    )

    assert any(
        [
            "Error in contract verification handler: Simulated failure in ContractVerificationHandler" in record.message
            for record in caplog.records
        ]
    )
