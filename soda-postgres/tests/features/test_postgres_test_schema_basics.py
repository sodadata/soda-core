from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("postgres_schema")
    .column_varchar("id")
    .column_varchar("size")
    .column_varchar("created")
    .column_varchar("destroyed")
    .column_bigint("bigint")
    .column_char("char_4", character_maximum_length=4)
    .column_char("char_8", character_maximum_length=8)
    .column_smallint("smallint")
    .column_integer("integer")
    .column_boolean("flag")
    .column_decimal("decimal_10_2", numeric_precision=10, numeric_scale=2)
    .column_numeric("numeric_15_5", numeric_precision=15, numeric_scale=5)
    .column_float("float")
    .column_double("double")
    .column_timestamp("ts")
    .column_timestamp_tz("ts_tz")
    .column_date("date")
    .column_time("time")
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
              - name: bigint
                data_type: bigint
              - name: char_4
                data_type: character
              - name: char_8
                data_type: character
              - name: smallint
                data_type: smallint
              - name: integer
                data_type: integer
              - name: flag
                data_type: boolean
              - name: decimal_10_2
                data_type: numeric
              - name: numeric_15_5
                data_type: numeric
              - name: float
                data_type: double precision
              - name: double
                data_type: double precision
              - name: ts
                data_type: timestamp
              - name: ts_tz
                data_type: timestamp with time zone
              - name: date
                data_type: date
              - name: time
                data_type: time without time zone
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


def test_postgres_schema_pass_view(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    view = data_source_test_helper.create_view_from_test_table(test_table=test_table)

    data_source_test_helper.assert_contract_pass(
        test_table=view,
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
              - name: bigint
                data_type: bigint
              - name: char_4
                data_type: character
              - name: char_8
                data_type: character
              - name: smallint
                data_type: smallint
              - name: integer
                data_type: integer
              - name: flag
                data_type: boolean
              - name: decimal_10_2
                data_type: numeric
              - name: numeric_15_5
                data_type: numeric
              - name: float
                data_type: double precision
              - name: double
                data_type: double precision
              - name: ts
                data_type: timestamp
              - name: ts_tz
                data_type: timestamp with time zone
              - name: date
                data_type: date
              - name: time
                data_type: time without time zone
        """,
    )


def test_postgres_schema_pass_materialized_view(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    view = data_source_test_helper.create_materialized_view_from_test_table(test_table=test_table)

    data_source_test_helper.assert_contract_pass(
        test_table=view,
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
              - name: bigint
                data_type: bigint
              - name: char_4
                data_type: character
              - name: char_8
                data_type: character
              - name: smallint
                data_type: smallint
              - name: integer
                data_type: integer
              - name: flag
                data_type: boolean
              - name: decimal_10_2
                data_type: numeric
              - name: numeric_15_5
                data_type: numeric
              - name: float
                data_type: double precision
              - name: double
                data_type: double precision
              - name: ts
                data_type: timestamp
              - name: ts_tz
                data_type: timestamp with time zone
              - name: date
                data_type: date
              - name: time
                data_type: time without time zone
        """,
    )


def test_postgres_source_data_types_are_spelled_by_format_type(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    columns = data_source_test_helper.data_source_impl.get_columns_metadata(
        dataset_prefixes=test_table.dataset_prefix, dataset_name=test_table.unique_name
    )

    assert {column.column_name: column.source_data_type for column in columns} == {
        "id": "character varying",
        "size": "character varying",
        "created": "character varying",
        "destroyed": "character varying",
        "bigint": "bigint",
        "char_4": "character(4)",
        "char_8": "character(8)",
        "smallint": "smallint",
        "integer": "integer",
        "flag": "boolean",
        "decimal_10_2": "numeric(10,2)",
        "numeric_15_5": "numeric(15,5)",
        "float": "double precision",
        "double": "double precision",
        "ts": "timestamp without time zone",
        "ts_tz": "timestamp with time zone",
        "date": "date",
        "time": "time without time zone",
    }
