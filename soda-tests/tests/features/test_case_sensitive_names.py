from datetime import datetime, timezone

from freezegun import freeze_time
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("caseSensitiveNames")
    .column_integer("Id")
    .column_timestamp_tz("CreatedAt")
    .rows(
        rows=[
            (1, datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)),
            (2, datetime(year=2025, month=1, day=2, hour=2, minute=0, second=0, tzinfo=timezone.utc)),
            (3, datetime(year=2025, month=1, day=3, hour=4, minute=0, second=0, tzinfo=timezone.utc)),
        ]
    )
    .build()
)


def test_attributes_global_apply(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    with freeze_time(datetime(year=2025, month=1, day=3, hour=10, minute=0, second=0, tzinfo=timezone.utc)):
        data_source_test_helper.assert_contract_pass(
            test_table=test_table,
            contract_yaml_str=f"""
                check_attributes:
                    description: "Test description"
                columns:
                    - name: Id
                      checks:
                        - aggregate:
                            function: avg
                            threshold:
                                must_be: 2
                        - invalid:
                            valid_values: [1, 2, 3]
                        - missing:
                        - duplicate:
                    - name: CreatedAt
                checks:
                    - schema:
                    - freshness:
                        column: CreatedAt
                        threshold:
                            must_be_less_than: 12
            """,
        )
