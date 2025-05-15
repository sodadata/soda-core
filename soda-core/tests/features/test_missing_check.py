from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("missing")
    .column_text("id")
    .column_integer("age")
    .rows(
        rows=[
            ("1", 1),
            (None, -1),
            ("3", None),
            ("X", 2),
        ]
    )
    .build()
)


def test_missing_count(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                checks:
                  - missing
                    # Expected check to fail because...
                    # must_be: 0 is the default threshold
        """,
    )


def test_missing_count_custom_missing_values(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                missing_values: ['X', 'Y']
                checks:
                  - missing:
                      threshold:
                        must_be: 2
        """,
    )


def test_missing_count_custom_missing_values_int_on_column(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                missing_values: [-1, -2]
                checks:
                  - missing:
                      threshold:
                        must_be: 2
        """,
    )


def test_missing_count_missing_values_on_check(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                checks:
                  - missing:
                      missing_values: ['X', 'Y']
                      threshold:
                        must_be: 2
        """,
    )


def test_missing_count_overwrite_missing_values_on_check(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                missing_format:
                  regex: ^xxx$
                  name: 3 x-es
                checks:
                  - missing:
                      missing_values: ['X', 'Y']
                      threshold:
                        must_be: 2
        """,
    )


def test_missing_percent(data_source_test_helper: DataSourceTestHelper):
    # https://dev.sodadata.io/o/f35cb402-ad17-4aca-9166-02c9eb75c979/datasets/dc3321b2-d147-4c84-a965-9019ad6ad55d/checks

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                checks:
                  - missing:
                      threshold:
                        metric: percent
                        must_be_between: [24, 26]
        """,
    )
