from tests.helpers.common_test_tables import customers_test_table, orders_test_table
from tests.helpers.scanner import Scanner


def test_reference_check_fail(scanner: Scanner):
    customers_table_name = scanner.ensure_test_table(customers_test_table)
    orders_table_name = scanner.ensure_test_table(orders_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {orders_table_name}:
        - values in customer_id_nok must exist in {customers_table_name} id
    """
    )
    scan.execute()

    scan.assert_all_checks_fail()


def test_reference_check_pass(scanner: Scanner):
    customers_table_name = scanner.ensure_test_table(customers_test_table)
    orders_table_name = scanner.ensure_test_table(orders_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {orders_table_name}:
        - values in (customer_id_ok) must exist in {customers_table_name} (id)
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_multi_column_reference_check(scanner: Scanner):
    customers_table_name = scanner.ensure_test_table(customers_test_table)
    orders_table_name = scanner.ensure_test_table(orders_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {orders_table_name}:
        - values in (customer_country, customer_zip) must exist in {customers_table_name} (country, zip)
    """
    )
    scan.execute()

    scan.assert_all_checks_fail()
