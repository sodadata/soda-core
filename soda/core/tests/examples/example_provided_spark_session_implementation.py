from textwrap import dedent

from soda.scan import Scan


class MockSparkRow:
    def __init__(self):
        self.__fields__ = ["COUNT(*)"]

    def __getitem__(self, field):
        return 3


class MockSparkDataframe:
    def __init__(self, sql: str):
        self.sql = sql

    def collect(self):
        return [MockSparkRow()]


class MockSparkSession:
    def sql(self, sql: str):
        return MockSparkDataframe(sql)


def test_provided_spark_session_implementation():

    scan = Scan()
    scan.set_data_source_name("provided_spark_session")
    scan.set_scan_definition_name("my_programmatic_test_scan")
    scan.add_sodacl_yaml_str(
        dedent(
            """
      checks for my_dataframe:
        - row_count > 0
    """
        )
    )

    spark_session = MockSparkSession()
    # my_dataframe.createOrReplaceTempView("my_dataframe")
    scan.add_configuration_spark_session("provided_spark_session", spark_session)
    scan.execute()
    scan.assert_no_checks_warn_or_fail()
