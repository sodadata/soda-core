from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.tests.helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("postgres_schema")
    .column_text("id")
    .column_text("size")
    .column_text("created")
    .column_text("destroyed")
    .build()
)


def test_postgres_schema_pass(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - type: schema
            columns:
              - name: id
                data_type: varchar
              - name: size
                data_type: varchar(255)
              - name: created
                data_type: character varying
              - name: destroyed
                data_type: character varying(255)
        """,
    )


def test_postgres_schema_fail(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - type: schema
            columns:
              - name: id
                data_type: varchar(16)
              - name: size
                data_type: character varying(16)
              - name: created
              - name: destroyed
        """,
    )
