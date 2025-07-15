from datetime import datetime, timezone

import pytest
from freezegun import freeze_time
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckResult,
    ContractVerificationResult,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("identity")
    .column_integer("id")
    .column_timestamp_tz("created_at")
    .rows(
        rows=[
            (1, datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)),
            (2, datetime(year=2025, month=1, day=2, hour=2, minute=0, second=0, tzinfo=timezone.utc)),
            (3, datetime(year=2025, month=1, day=3, hour=4, minute=0, second=0, tzinfo=timezone.utc)),
        ]
    )
    .build()
)


@pytest.mark.ci
def test_identity_stable(data_source_test_helper: DataSourceTestHelper):
    """
    This test verifies that the identity mechanism does not change over time.

    This is achieved by hardcoding the identity in the test.
    """
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    with freeze_time(datetime(year=2025, month=1, day=3, hour=10, minute=0, second=0, tzinfo=timezone.utc)):
        contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
            test_table=test_table,
            contract_yaml_str=f"""
                check_attributes:
                    description: "Test description"
                columns:
                    - name: id
                      checks:
                        - aggregate:
                            function: avg
                            threshold:
                                must_be: 2
                        - invalid:
                            valid_values: ['1', '2', '3']
                        - missing:
                        - duplicate:
                    - name: created_at
                checks:
                    - schema:
                    - freshness:
                        column: created_at
                        threshold:
                            must_be_less_than: 12
            """,
        )
        check_results: list[CheckResult] = contract_verification_result.check_results

        for check_result in check_results:
            print(check_result.check.identity)

        assert check_results[0].check.identity == "576d36fc"
        assert check_results[1].check.identity == "01c19ef9"
        assert check_results[2].check.identity == "7652c6f4"
        assert check_results[3].check.identity == "836ad5de"
        assert check_results[4].check.identity == "c2423bf1"
        assert check_results[5].check.identity == "1651a1a7"
