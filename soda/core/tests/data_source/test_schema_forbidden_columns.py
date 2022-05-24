from soda.execution.schema_check import SchemaCheck
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_forbidden_columns_pass(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)
    default_casify_column_name = scanner.data_source.default_casify_column_name

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            fail:
              when forbidden column present: [{default_casify_column_name('non_existing_column_one')}, {default_casify_column_name('"%non_existing%"')}]
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_forbidden_columns_fail(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    default_casify_column_name = scanner.data_source.default_casify_column_name

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            fail:
              when forbidden column present: [{default_casify_column_name('id')}, {default_casify_column_name('non_existing_column_one')}, {default_casify_column_name('"%non_existing%"')}]
    """
    )
    scan.execute()

    scan.assert_all_checks_fail()
    check: SchemaCheck = scan._checks[0]
    assert sorted(check.schema_present_column_names) == sorted([default_casify_column_name("id")])


def test_forbidden_columns_fail_matching_wildcard(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    default_casify_column_name = scanner.data_source.default_casify_column_name

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            fail:
              when forbidden column present:
               - {default_casify_column_name('size*')}
               - {default_casify_column_name('non_existing_column_two')}
    """
    )
    scan.execute()

    scan.assert_all_checks_fail()
    check: SchemaCheck = scan._checks[0]
    assert sorted(check.schema_present_column_names) == sorted(
        [
            default_casify_column_name("size"),
            default_casify_column_name("sizeTxt")
        ]
    )


def test_forbidden_columns_warn(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    default_casify_column_name = scanner.data_source.default_casify_column_name

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - schema:
            warn:
              when forbidden column present: [{default_casify_column_name('id')}, {default_casify_column_name('non_existing_column_one')}, {default_casify_column_name('"%non_existing%"')}]
    """
    )
    scan.execute()

    scan.assert_all_checks_warn()
    check: SchemaCheck = scan._checks[0]
    assert sorted(check.schema_present_column_names) == sorted([default_casify_column_name("id")])
