from helpers.data_source_fixture import DataSourceFixture
from helpers.test_table import TestTable


def test_vertica_row_count(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(
        TestTable(
            name="vertica_row_count",
            columns=[
                ("id", "int"),
                ("name", "varchar"),
            ],
            values=[(1, 2, 3, 4, 5, 6, 7, 8), (None, None, None, None, None, None, None, None)],
        )
    )

    scan = data_source_fixture.create_test_scan()

    scan.add_sodacl_yaml_str(
        f"""
        checks for {table_name}:
            - row_count > 0
        """
    )

    scan.execute(allow_warnings_only=True)
    scan.assert_no_error_logs()
