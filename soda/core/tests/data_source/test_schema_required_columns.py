from soda.execution.schema_check import SchemaCheck
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner
from tests.helpers.utils import format_checks


def test_required_columns_pass(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    format_column_default = scanner.data_source.format_column_default
    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            fail:
              when required column missing: [{format_column_default('id')}, {format_column_default('sizeTxt')}, {format_column_default('distance')}]
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_required_columns_fail(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)
    format_column_default = scanner.data_source.format_column_default

    scan = scanner.create_test_scan()
    checks_str = format_checks(
        ["id", "sizeTxt", "non_existing_column", "name"],
        indent=15,
        prefix="-",
        data_source=scanner.data_source,
    )
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            fail:
              when required column missing:
{checks_str}
    """
    )
    scan.execute()

    scan.assert_all_checks_fail()
    check: SchemaCheck = scan._checks[0]
    assert sorted(check.schema_missing_column_names) == sorted(
        [format_column_default("non_existing_column"), format_column_default("name")]
    )


def test_required_columns_warn(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)
    format_column_default = scanner.data_source.format_column_default

    scan = scanner.create_test_scan()
    checks_str = format_checks(
        ["id", "sizeTxt", "non_existing_column", "name"],
        indent=15,
        prefix="-",
        data_source=scanner.data_source,
    )
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            warn:
              when required column missing:
{checks_str}
    """
    )
    scan.execute()

    scan.assert_all_checks_warn()
    check: SchemaCheck = scan._checks[0]
    assert sorted(check.schema_missing_column_names) == sorted(
        [format_column_default("non_existing_column"), format_column_default("name")]
    )
