from datetime import datetime, timezone

from freezegun import freeze_time
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.common.datetime_conversions import convert_datetime_to_str
from soda_core.contracts.contract_verification import (
    CheckResult,
    ContractVerificationResult,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("freshness")
    .column_integer("id")
    .column_timestamp("created_at")
    .rows(
        rows=[
            (1, datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)),
            (2, datetime(year=2025, month=1, day=2, hour=2, minute=0, second=0, tzinfo=timezone.utc)),
            (4, datetime(year=2025, month=1, day=2, hour=5, minute=0, second=0, tzinfo=timezone.utc)),
            (3, datetime(year=2025, month=1, day=3, hour=4, minute=0, second=0, tzinfo=timezone.utc)),
            (5, datetime(year=2025, month=1, day=3, hour=7, minute=0, second=0, tzinfo=timezone.utc)),
            (6, datetime(year=2025, month=1, day=4, hour=9, minute=0, second=0, tzinfo=timezone.utc)),
        ]
    )
    .build()
)


def test_freshness(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_yaml_str: str = f"""
        checks:
          - freshness:
              column: created_at
              threshold:
                must_be_less_than: 2
    """

    with freeze_time(datetime(year=2025, month=1, day=4, hour=10, minute=0, second=0)):
        contract_verification_result_t1: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
            test_table=test_table, contract_yaml_str=contract_yaml_str
        )
        check_result: CheckResult = contract_verification_result_t1.check_results[0]
        assert get_diagnostic_value(check_result, "max_timestamp") == "2025-01-04 09:00:00"
        assert get_diagnostic_value(check_result, "max_timestamp_utc") == "2025-01-04 09:00:00+00:00"
        assert get_diagnostic_value(check_result, "now_timestamp") == "2025-01-04 10:00:00+00:00"
        assert get_diagnostic_value(check_result, "now_timestamp_utc") == "2025-01-04 10:00:00+00:00"
        assert get_diagnostic_value(check_result, "freshness") == "1:00:00"
        assert get_diagnostic_value(check_result, "freshness_in_seconds") == "3600.0"
        assert get_diagnostic_value(check_result, "unit") == "hour"
        assert get_diagnostic_value(check_result, "freshness_in_hours") == "1.00"


def test_freshness_in_days(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_yaml_str: str = f"""
        checks:
          - freshness:
              column: created_at
              threshold:
                unit: day
                must_be_less_than: 1
    """

    with freeze_time(datetime(year=2025, month=1, day=5, hour=11, minute=0, second=0)):
        contract_verification_result_t1: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
            test_table=test_table, contract_yaml_str=contract_yaml_str
        )
        check_result: CheckResult = contract_verification_result_t1.check_results[0]
        assert get_diagnostic_value(check_result, "max_timestamp") == "2025-01-04 09:00:00"
        assert get_diagnostic_value(check_result, "max_timestamp_utc") == "2025-01-04 09:00:00+00:00"
        assert get_diagnostic_value(check_result, "now_timestamp") == "2025-01-05 11:00:00+00:00"
        assert get_diagnostic_value(check_result, "now_timestamp_utc") == "2025-01-05 11:00:00+00:00"
        assert get_diagnostic_value(check_result, "freshness") == "1 day, 2:00:00"
        assert get_diagnostic_value(check_result, "freshness_in_seconds") == "93600.0"
        assert get_diagnostic_value(check_result, "unit") == "day"
        assert get_diagnostic_value(check_result, "freshness_in_days") == "1.08"


def test_freshness_now_variable(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_yaml_str: str = f"""
        variables:
          NNOWW:
        checks:
          - freshness:
              column: created_at
              now_variable: NNOWW
              threshold:
                must_be_less_than: 2
    """

    contract_verification_result_t1: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=contract_yaml_str,
        variables={
            "NNOWW": convert_datetime_to_str(
                datetime(year=2025, month=1, day=4, hour=10, minute=0, second=0, tzinfo=timezone.utc)
            )
        },
    )
    check_result: CheckResult = contract_verification_result_t1.check_results[0]
    assert get_diagnostic_value(check_result, "max_timestamp") == "2025-01-04 09:00:00"
    assert get_diagnostic_value(check_result, "max_timestamp_utc") == "2025-01-04 09:00:00+00:00"
    assert get_diagnostic_value(check_result, "now_timestamp") == "2025-01-04 10:00:00+00:00"
    assert get_diagnostic_value(check_result, "now_timestamp_utc") == "2025-01-04 10:00:00+00:00"
    assert get_diagnostic_value(check_result, "freshness") == "1:00:00"
    assert get_diagnostic_value(check_result, "freshness_in_seconds") == "3600.0"
    assert get_diagnostic_value(check_result, "unit") == "hour"
    assert get_diagnostic_value(check_result, "freshness_in_hours") == "1.00"
