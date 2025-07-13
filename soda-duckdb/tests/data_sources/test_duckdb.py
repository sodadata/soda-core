import pandas as pd
import polars as pl
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification

# Teset table spec is just used to build the contract, the actual data is in the df/csv/etc
test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("row_count_duckdb")
    .column_text("id")
    .rows(
        rows=[
            ("1",),
            ("2",),
            ("3",),
        ]
    )
    .build()
)

contract_str = f"""
    columns:
        - name: id
          data_type: text
          checks:
            - missing:
    checks:
        - row_count:
            threshold:
                must_be: 3
"""


def test_df(data_source_test_helper: DataSourceTestHelper):
    test_df = pd.DataFrame.from_dict({"id": [1, 2, 3]})

    test_table = data_source_test_helper._create_test_table_python_object(test_table_specification)

    con = data_source_test_helper.data_source_impl.connection.connection
    con.register(test_table.unique_name, test_df)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=contract_str,
    )


def test_polars(data_source_test_helper: DataSourceTestHelper):
    test_df = pl.DataFrame({"id": [1, 2, 3]})

    test_table = data_source_test_helper._create_test_table_python_object(test_table_specification)

    con = data_source_test_helper.data_source_impl.connection.connection
    con.register(test_table.unique_name, test_df)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=contract_str,
    )
