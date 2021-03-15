from tests.common.sql_test_case import TARGET_HIVE
from tests.common.sql_test_suite import SqlTestSuite


class HiveSuite(SqlTestSuite):

    def setUp(self) -> None:
        self.target = TARGET_HIVE
        super().setUp()
