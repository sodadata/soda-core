from typing import Optional

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.soda_cloud import SodaCloud
from soda_core.contracts.contract_verification import (
    ContractVerificationResult,
    PostProcessingStage,
)
from soda_core.contracts.impl.contract_verification_impl import (
    ContractImpl,
    ContractVerificationHandler,
    ContractVerificationHandlerRegistry,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("verification_handler")
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


def test_failure_in_contract_verification_handler_does_not_fail_scan(
    data_source_test_helper: DataSourceTestHelper, caplog
):
    class DummyHandler(ContractVerificationHandler):
        def handle(
            self,
            contract_impl: ContractImpl,
            data_source_impl: Optional[DataSourceImpl],
            contract_verification_result: ContractVerificationResult,
            soda_cloud: SodaCloud,
            soda_cloud_send_results_response_json: dict,
            dwh_data_source_file_path: Optional[str] = None,
        ):
            raise RuntimeError("Simulated failure in ContractVerificationHandler")

        def provides_post_processing_stages(self) -> list[PostProcessingStage]:
            return []

    ContractVerificationHandlerRegistry.register(DummyHandler())

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
