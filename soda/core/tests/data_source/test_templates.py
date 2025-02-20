from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture


def test_templates_safe(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - row_count > 0:
            name: testing name ${{''.__class__.mro()[1].__subclasses__()[240]('whoami',shell=True,stdout=-1).communicate()[0].strip()}}
    """
    )
    scan.execute_unchecked()

    scan.assert_log("access to attribute '__class__' of 'str' object is unsafe")
