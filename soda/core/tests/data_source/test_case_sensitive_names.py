from soda.execution.data_type import DataType
from tests.helpers.scanner import Scanner
from tests.helpers.test_table import TestTable


def test_row_count_thresholds_passing(scanner: Scanner):
    """
    Tests all passing thresholds on a simple row count
    """
    table_name = scanner.ensure_test_table(
        TestTable(
            name="CaseSensitive",
            columns=[("Id", DataType.TEXT)],
            quote_names=True,
        )
    )

    quoted_table_name = scanner.data_source.quote_table(table_name)
    quoted_id = scanner.data_source.quote_column("Id")

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {quoted_table_name}:
        - row_count = 0
        - missing_count({quoted_id}) = 0
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()
