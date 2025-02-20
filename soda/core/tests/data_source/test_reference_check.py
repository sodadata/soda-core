from helpers.common_test_tables import customers_test_table, orders_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.utils import execute_scan_and_get_scan_result


def test_reference_check_fail(data_source_fixture: DataSourceFixture):
    customers_table_name = data_source_fixture.ensure_test_table(customers_test_table)
    orders_table_name = data_source_fixture.ensure_test_table(orders_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {orders_table_name}:
        - values in customer_id_nok must exist in {customers_table_name} id
    """
    )
    scan.execute()

    scan.assert_all_checks_fail()


def test_reference_check_pass(data_source_fixture: DataSourceFixture):
    customers_table_name = data_source_fixture.ensure_test_table(customers_test_table)
    orders_table_name = data_source_fixture.ensure_test_table(orders_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {orders_table_name}:
        - values in (customer_id_ok) must exist in {customers_table_name} (id)
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_reference_check_pass_identity(data_source_fixture: DataSourceFixture):
    customers_table_name = data_source_fixture.ensure_test_table(customers_test_table)
    orders_table_name = data_source_fixture.ensure_test_table(orders_test_table)

    identity = "test_identity"
    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
          checks for {orders_table_name}:
            - values in (customer_id_ok) must exist in {customers_table_name} (id):
                identity: {identity}
        """,
    )
    assert "v4" in scan_result["checks"][0]["identities"]
    assert scan_result["checks"][0]["identities"]["v4"] == identity


def test_multi_column_reference_check(data_source_fixture: DataSourceFixture):
    customers_table_name = data_source_fixture.ensure_test_table(customers_test_table)
    orders_table_name = data_source_fixture.ensure_test_table(orders_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {orders_table_name}:
        - values in (customer_country, customer_zip) must exist in {customers_table_name} (country, zip)
    """
    )
    scan.execute()

    scan.assert_all_checks_fail()


def test_reference_check_pass_not(data_source_fixture: DataSourceFixture):
    customers_table_name = data_source_fixture.ensure_test_table(customers_test_table)
    orders_table_name = data_source_fixture.ensure_test_table(orders_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
    checks for {orders_table_name}:
        - values in id must not exist in {customers_table_name} id
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_reference_check_fail_not(data_source_fixture: DataSourceFixture):
    customers_table_name = data_source_fixture.ensure_test_table(customers_test_table)
    orders_table_name = data_source_fixture.ensure_test_table(orders_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
    checks for {orders_table_name}:
        - values in (customer_id_ok) must not exist in {customers_table_name} (id)
    """
    )
    scan.execute()

    scan.assert_all_checks_fail()


def test_multi_column_reference_check_not(data_source_fixture: DataSourceFixture):
    customers_table_name = data_source_fixture.ensure_test_table(customers_test_table)
    orders_table_name = data_source_fixture.ensure_test_table(orders_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
    checks for {orders_table_name}:
        - values in (id, text) must not exist in {customers_table_name} (id, cst_size_txt)
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()
