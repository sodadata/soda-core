from sodasql.dialects.spark_dialect import ColumnMetadata

from tests.common.sql_test_case import TARGET_SPARK
from tests.common.sql_test_suite import SqlTestSuite


class SparkSuite(SqlTestSuite):

    def setUp(self) -> None:
        self.target = TARGET_SPARK
        super().setUp()

    def test_sql_columns_metadata(self):
        data_type = self.dialect.data_type_varchar_255.lower()
        expected = [
            ColumnMetadata("name", data_type, is_nullable="YES")
        ]

        self.sql_recreate_table([" ".join(column[:2]) for column in expected])
        columns_metadata = self.dialect.sql_columns_metadata(
            self.default_test_table_name)

        assert columns_metadata == expected
