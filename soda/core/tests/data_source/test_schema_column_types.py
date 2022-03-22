from soda.execution.check_outcome import CheckOutcome
from soda.execution.data_type import DataType
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner
from tests.helpers.utils import format_checks


def test_columns_types_pass(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()

    checks_str = format_checks(
        [("id", "varchar"), ("distance", "integer")],
        indent=15,
        data_source=scanner.data_source,
    )
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            fail:
              when wrong column type:
{checks_str}
    """
    )
    # This Also verifies type aliasing - check using "varchar", actual is "character varying"
    scan.execute()

    scan.assert_all_checks_pass()


def test_columns_types_fail(scanner: Scanner):
    checks_str = format_checks(
        [("id", "integer"), ("does_not_exist", "integer"), ("pct", "varchar")],
        indent=15,
        data_source=scanner.data_source,
    )
    table_name = scanner.ensure_test_table(customers_test_table)
    format_column_default = scanner.data_source.format_column_default
    format_type_default = scanner.data_source.format_type_default

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            fail:
              when wrong column type:
{checks_str}
    """
    )
    scan.execute()

    check = scan._checks[0]

    assert check.outcome == CheckOutcome.FAIL
    assert check.schema_missing_column_names == [format_column_default("does_not_exist")]
    assert check.schema_column_type_mismatches == {
        format_column_default("id"): {
            "expected_type": format_type_default("integer"),
            "actual_type": scanner.data_source.get_sql_type_for_schema_check(DataType.TEXT),
        }
    }


def test_columns_types_warn(scanner: Scanner):
    checks_str = format_checks(
        [("id", "integer"), ("does_not_exist", "integer"), ("pct", "varchar")],
        indent=15,
        data_source=scanner.data_source,
    )
    format_column_default = scanner.data_source.format_column_default
    format_type_default = scanner.data_source.format_type_default
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            warn:
              when wrong column type:
{checks_str}
    """
    )
    scan.execute()

    check = scan._checks[0]

    assert check.outcome == CheckOutcome.WARN
    assert check.schema_missing_column_names == [format_column_default("does_not_exist")]
    assert check.schema_column_type_mismatches == {
        format_column_default("id"): {
            "expected_type": format_type_default("integer"),
            "actual_type": scanner.data_source.get_sql_type_for_schema_check(DataType.TEXT),
        }
    }
