from soda.execution.check_outcome import CheckOutcome
from soda.execution.data_type import DataType
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner
from tests.helpers.utils import format_checks


def test_columns_types_pass(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()

    def column_type_format(column_name: str) -> str:
        test_column = customers_test_table.find_test_column_by_name(column_name)
        casified_column_name = scanner.casify_column_name(column_name)
        casified_data_type = scanner.casify_data_type(test_column.data_type)
        return f"{casified_column_name}: {casified_data_type}"

    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            fail:
              when wrong column type:
                {column_type_format('id')}
                {column_type_format('size')}
                {column_type_format('sizeTxt')}
                {column_type_format('distance')}
                {column_type_format('date')}
                {column_type_format('ts')}
                {column_type_format('ts_with_tz')}
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

    data_source = scanner.data_source
    default_casify_column_name = data_source.default_casify_column_name

    assert check.schema_missing_column_names == [default_casify_column_name("does_not_exist")]
    assert check.schema_column_type_mismatches == {
        default_casify_column_name("id"): {
            "expected_type": data_source.default_casify_type_name("integer"),
            "actual_type": data_source.get_sql_type_for_schema_check(DataType.TEXT),
        }
    }


def test_columns_types_warn(scanner: Scanner):
    checks_str = format_checks(
        [("id", "integer"), ("does_not_exist", "integer"), ("pct", "varchar")],
        indent=15,
        data_source=scanner.data_source,
    )
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

    data_source = scanner.data_source
    default_casify_column_name = data_source.default_casify_column_name

    assert check.schema_missing_column_names == [default_casify_column_name("does_not_exist")]
    assert check.schema_column_type_mismatches == {
        default_casify_column_name("id"): {
            "expected_type": data_source.default_casify_type_name("integer"),
            "actual_type": data_source.get_sql_type_for_schema_check(DataType.TEXT),
        }
    }
