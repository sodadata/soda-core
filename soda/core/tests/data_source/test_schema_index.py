from soda.execution.schema_check import SchemaCheck
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner
from tests.helpers.utils import format_checks


def test_required_columns_indexes_pass(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()

    checks_str = format_checks(
        [("id", "0"), ("sizeTxt", "2"), ("distance", "3")],
        indent=15,
        data_source=scanner.data_source,
    )
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            fail:
              when wrong column index:
{checks_str}
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_required_columns_indexes_fail(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)
    format_column_default = scanner.data_source.format_column_default
    checks_str = format_checks(
        [("id", "6"), ("sizeTxt", "3"), ("distance", "4")],
        indent=15,
        data_source=scanner.data_source,
    )

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            fail:
              when wrong column index:
{checks_str}
    """
    )
    scan.execute()

    scan.assert_all_checks_fail()
    check: SchemaCheck = scan._checks[0]
    assert check.schema_column_index_mismatches == {
        format_column_default("distance"): {
            "actual_index": 3,
            "column_on_expected_index": format_column_default("pct"),
            "expected_index": 4,
        },
        format_column_default("id"): {
            "actual_index": 0,
            "column_on_expected_index": format_column_default("country"),
            "expected_index": 6,
        },
        format_column_default("sizeTxt"): {
            "actual_index": 2,
            "column_on_expected_index": format_column_default("distance"),
            "expected_index": 3,
        },
    }


def test_required_columns_indexes_warn(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)
    format_column_default = scanner.data_source.format_column_default
    checks_str = format_checks(
        [("id", "6"), ("sizeTxt", "3"), ("distance", "4")],
        indent=15,
        data_source=scanner.data_source,
    )

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            warn:
              when wrong column index:
{checks_str}
    """
    )
    scan.execute()

    scan.assert_all_checks_warn()
    check: SchemaCheck = scan._checks[0]
    assert check.schema_column_index_mismatches == {
        format_column_default("distance"): {
            "actual_index": 3,
            "column_on_expected_index": format_column_default("pct"),
            "expected_index": 4,
        },
        format_column_default("id"): {
            "actual_index": 0,
            "column_on_expected_index": format_column_default("country"),
            "expected_index": 6,
        },
        format_column_default("sizeTxt"): {
            "actual_index": 2,
            "column_on_expected_index": format_column_default("distance"),
            "expected_index": 3,
        },
    }
