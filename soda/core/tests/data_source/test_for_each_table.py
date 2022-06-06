import pytest
from tests.helpers.common_test_tables import (
    customers_test_table,
    raw_customers_test_table,
)
from tests.helpers.fixtures import test_data_source
from tests.helpers.scanner import Scanner


def test_for_each_table(scanner: Scanner):
    customers_table_name = scanner.ensure_test_table(customers_test_table)
    rawcustomers_table_name = scanner.ensure_test_table(raw_customers_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          for each table T:
            tables:
              - {customers_table_name}
              - include {rawcustomers_table_name}%
              - include {scanner.data_source.data_source_name}.{rawcustomers_table_name}%
              - exclude non_existing_table
            checks:
              - row_count > 0
              - missing_count(id) = 1
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()
    assert len(scan._checks) == 4


@pytest.mark.skipif(
    test_data_source != "postgres",
    reason="Run for postgres only as nothing data source specific is tested.",
)
def test_for_each_table_schema(scanner: Scanner):
    customers_table_name = scanner.ensure_test_table(customers_test_table)
    casify = scanner.data_source.default_casify_column_name

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          for each table T:
            tables:
              - {customers_table_name}
            checks:
              - schema:
                  warn:
                    when required column missing: [{casify('id')}]
                  fail:
                    when forbidden column present:
                      - ssn
                      - credit_card%
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()
