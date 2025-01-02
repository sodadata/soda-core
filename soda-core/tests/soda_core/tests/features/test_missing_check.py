from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.tests.helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("missing")
    .column_text("id")
    .rows(rows=[
        ("1",),
        (None,),
        ("3",),
        ("X",),
    ])
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
                  - type: missing_count
                    # Expected check to fail because...
                    # must_be: 0 is the default threshold
        """
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
                  - type: missing_count
                    must_be: 2
        """
    )


def test_missing_percent(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                checks:
                  - type: missing_percent
                    must_be_between: [24, 26]
        """
    )


missing_no_rows_specification = (
    TestTableSpecification.builder()
    .table_purpose("missing_no_rows")
    .column_text("id")
    .build()
)


def test_missing_percent_no_division_by_zero(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(missing_no_rows_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                checks:
                  - type: missing_percent
        """
    )


def test_missing_count_no_rows(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(missing_no_rows_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                checks:
                  - type: missing_count
        """
    )
