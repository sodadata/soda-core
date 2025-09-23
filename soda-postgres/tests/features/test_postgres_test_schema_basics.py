from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("postgres_schema")
    .column_varchar("id")
    .column_varchar("size")
    .column_varchar("created")
    .column_varchar("destroyed")
    .build()
)


def test_postgres_schema_pass(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: id
                data_type: varchar
              - name: size
                data_type: varchar
              - name: created
                data_type: varchar
              - name: destroyed
                data_type: varchar
        """,
    )


def test_postgres_schema_fail(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: id
                data_type: varchar(16)
              - name: size
                data_type: character varying(16)
              - name: created
              - name: destroyed
        """,
    )
