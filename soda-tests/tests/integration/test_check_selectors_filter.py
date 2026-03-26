from datetime import datetime, timezone

from freezegun import freeze_time
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import ContractVerificationResult
from soda_core.contracts.impl.check_selector import CheckSelector

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("check_selectors_filter")
    .column_integer("id")
    .column_varchar("name")
    .column_timestamp_tz("created_at")
    .rows(
        rows=[
            (1, "Alice", datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)),
            (2, "Bob", datetime(year=2025, month=1, day=2, hour=2, minute=0, second=0, tzinfo=timezone.utc)),
            (3, "Charlie", datetime(year=2025, month=1, day=3, hour=4, minute=0, second=0, tzinfo=timezone.utc)),
        ]
    )
    .build()
)


def get_contract_yaml(data_source_test_helper: DataSourceTestHelper) -> str:
    id_quoted = data_source_test_helper.quote_column("id")
    return f"""
    check_attributes:
        description: "Test description"
    columns:
        - name: id
          checks:
            - aggregate:
                function: avg
                threshold:
                    must_be: 2
                attributes:
                    severity: critical
            - invalid:
                valid_values: [1, 2, 3]
                attributes:
                    severity: low
            - missing:
                attributes:
                    severity: critical
            - duplicate:
            - failed_rows:
                expression: "{id_quoted} > 100"
        - name: name
        - name: created_at
    checks:
        - schema:
        - row_count:
        - freshness:
            column: created_at
            threshold:
                must_be_less_than: 12
"""


def test_filter_by_type(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock(
        [MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"})]
    )

    with freeze_time(datetime(year=2025, month=1, day=3, hour=10, minute=0, second=0, tzinfo=timezone.utc)):
        result = data_source_test_helper.verify_contract(
            test_table=test_table,
            check_selectors=[CheckSelector.parse("type=missing")],
            contract_yaml_str=get_contract_yaml(data_source_test_helper),
        )
        assert result.is_ok
        cvr: ContractVerificationResult = result.contract_verification_results[0]
        assert cvr.number_of_checks_excluded == 7
        non_excluded = [cr for cr in cvr.check_results if not cr.is_excluded()]
        assert len(non_excluded) == 1
        assert non_excluded[0].check.type == "missing"


def test_filter_by_column(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock(
        [MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"})]
    )

    with freeze_time(datetime(year=2025, month=1, day=3, hour=10, minute=0, second=0, tzinfo=timezone.utc)):
        result = data_source_test_helper.verify_contract(
            test_table=test_table,
            check_selectors=[CheckSelector.parse("column=id")],
            contract_yaml_str=get_contract_yaml(data_source_test_helper),
        )
        assert result.is_ok
        cvr: ContractVerificationResult = result.contract_verification_results[0]
        # 5 column checks on id, 3 dataset-level checks excluded
        non_excluded = [cr for cr in cvr.check_results if not cr.is_excluded()]
        assert len(non_excluded) == 5
        assert all(cr.check.column_name == "id" for cr in non_excluded)


def test_filter_by_attribute(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock(
        [MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"})]
    )

    with freeze_time(datetime(year=2025, month=1, day=3, hour=10, minute=0, second=0, tzinfo=timezone.utc)):
        result = data_source_test_helper.verify_contract(
            test_table=test_table,
            check_selectors=[CheckSelector.parse("attributes.severity=critical")],
            contract_yaml_str=get_contract_yaml(data_source_test_helper),
        )
        assert result.is_ok
        cvr: ContractVerificationResult = result.contract_verification_results[0]
        non_excluded = [cr for cr in cvr.check_results if not cr.is_excluded()]
        assert len(non_excluded) == 2
        assert all(cr.check.attributes.get("severity") == "critical" for cr in non_excluded)


def test_filter_and_across_fields(data_source_test_helper: DataSourceTestHelper):
    """type=aggregate AND column=id should match only the aggregate check on id."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock(
        [MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"})]
    )

    with freeze_time(datetime(year=2025, month=1, day=3, hour=10, minute=0, second=0, tzinfo=timezone.utc)):
        result = data_source_test_helper.verify_contract(
            test_table=test_table,
            check_selectors=CheckSelector.parse_all(["type=aggregate", "column=id"]),
            contract_yaml_str=get_contract_yaml(data_source_test_helper),
        )
        assert result.is_ok
        cvr: ContractVerificationResult = result.contract_verification_results[0]
        non_excluded = [cr for cr in cvr.check_results if not cr.is_excluded()]
        assert len(non_excluded) == 1
        assert non_excluded[0].check.type == "aggregate"


def test_filter_or_within_same_field(data_source_test_helper: DataSourceTestHelper):
    """type=missing OR type=invalid should match both."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock(
        [MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"})]
    )

    with freeze_time(datetime(year=2025, month=1, day=3, hour=10, minute=0, second=0, tzinfo=timezone.utc)):
        result = data_source_test_helper.verify_contract(
            test_table=test_table,
            check_selectors=CheckSelector.parse_all(["type=missing", "type=invalid"]),
            contract_yaml_str=get_contract_yaml(data_source_test_helper),
        )
        assert result.is_ok
        cvr: ContractVerificationResult = result.contract_verification_results[0]
        non_excluded = [cr for cr in cvr.check_results if not cr.is_excluded()]
        assert len(non_excluded) == 2
        types = {cr.check.type for cr in non_excluded}
        assert types == {"missing", "invalid"}


def test_filter_wildcard(data_source_test_helper: DataSourceTestHelper):
    """type=miss* should match missing checks."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock(
        [MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"})]
    )

    with freeze_time(datetime(year=2025, month=1, day=3, hour=10, minute=0, second=0, tzinfo=timezone.utc)):
        result = data_source_test_helper.verify_contract(
            test_table=test_table,
            check_selectors=[CheckSelector.parse("type=miss*")],
            contract_yaml_str=get_contract_yaml(data_source_test_helper),
        )
        assert result.is_ok
        cvr: ContractVerificationResult = result.contract_verification_results[0]
        non_excluded = [cr for cr in cvr.check_results if not cr.is_excluded()]
        assert len(non_excluded) == 1
        assert non_excluded[0].check.type == "missing"


def test_backward_compat_check_paths(data_source_test_helper: DataSourceTestHelper):
    """Existing check_paths parameter should still work."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock(
        [MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"})]
    )

    with freeze_time(datetime(year=2025, month=1, day=3, hour=10, minute=0, second=0, tzinfo=timezone.utc)):
        result = data_source_test_helper.verify_contract(
            test_table=test_table,
            check_paths=["columns.id.checks.aggregate"],
            contract_yaml_str=get_contract_yaml(data_source_test_helper),
        )
        assert result.is_ok
        cvr: ContractVerificationResult = result.contract_verification_results[0]
        non_excluded = [cr for cr in cvr.check_results if not cr.is_excluded()]
        assert len(non_excluded) == 1
        assert non_excluded[0].check.type == "aggregate"


def test_no_selectors_runs_all(data_source_test_helper: DataSourceTestHelper):
    """With no selectors, all checks should run."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock(
        [MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"})]
    )

    with freeze_time(datetime(year=2025, month=1, day=3, hour=10, minute=0, second=0, tzinfo=timezone.utc)):
        result = data_source_test_helper.verify_contract(
            test_table=test_table,
            contract_yaml_str=get_contract_yaml(data_source_test_helper),
        )
        assert result.is_ok
        cvr: ContractVerificationResult = result.contract_verification_results[0]
        assert cvr.number_of_checks_excluded == 0
