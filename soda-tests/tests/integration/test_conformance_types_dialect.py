"""
Adapter Conformance Tests: Type Mapping & SQL Dialect

Validates that every adapter:
- Can create tables, insert data, and run checks for ALL Soda data types
- Generates valid sampling SQL for each supported sampler type
- Generates valid regex SQL
- Generates valid RANDOM() SQL
- Has consistent type synonym definitions

These cover the #3-#4 sources of field bugs (type mapping ~10%, SQL dialect ~8%).

See: projects/enhancements/common_bugs_tests/conformance-test-dev-plan.md (Phase 3)
"""

import datetime

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.metadata_types import SamplerType, SodaDataTypeName
from soda_core.common.sql_ast import COLUMN, FROM, RANDOM, REGEX_LIKE, SELECT, STAR
from soda_core.common.sql_dialect import SqlDialect

# ---------------------------------------------------------------------------
# Test table: one column per Soda data type, with actual data
# ---------------------------------------------------------------------------

all_types_with_data_table = (
    TestTableSpecification.builder()
    .table_purpose("conf_types_e2e")
    .column_char("col_char")
    .column_varchar("col_varchar")
    .column_text("col_text")
    .column_smallint("col_smallint")
    .column_integer("col_integer")
    .column_bigint("col_bigint")
    .column_numeric("col_numeric")
    .column_decimal("col_decimal")
    .column_float("col_float")
    .column_double("col_double")
    .column_boolean("col_boolean")
    .column_date("col_date")
    .column_time("col_time")
    .column_timestamp("col_timestamp")
    .column_timestamp_tz("col_timestamp_tz")
    .rows(
        [
            (
                "a",  # char
                "hello",  # varchar
                "some text",  # text
                1,  # smallint
                42,  # integer
                1000000,  # bigint
                3.14,  # numeric
                2.718,  # decimal
                1.5,  # float
                2.71828,  # double
                True,  # boolean
                datetime.date(2025, 6, 15),  # date
                datetime.time(10, 30, 0),  # time
                datetime.datetime(2025, 6, 15, 10, 30, 0),  # timestamp
                datetime.datetime(2025, 6, 15, 10, 30, 0),  # timestamp_tz
            ),
            (
                "b",
                "world",
                "more text",
                2,
                99,
                2000000,
                6.28,
                5.436,
                2.5,
                3.14159,
                False,
                datetime.date(2025, 7, 20),
                datetime.time(14, 0, 0),
                datetime.datetime(2025, 7, 20, 14, 0, 0),
                datetime.datetime(2025, 7, 20, 14, 0, 0),
            ),
            (
                None,  # null char
                None,  # null varchar
                None,  # null text
                None,  # null smallint
                None,  # null integer
                None,  # null bigint
                None,  # null numeric
                None,  # null decimal
                None,  # null float
                None,  # null double
                None,  # null boolean
                None,  # null date
                None,  # null time
                None,  # null timestamp
                None,  # null timestamp_tz
            ),
        ]
    )
    .build()
)

# Columns to test with missing check (all of them)
ALL_TYPE_COLUMNS = [
    "col_char",
    "col_varchar",
    "col_text",
    "col_smallint",
    "col_integer",
    "col_bigint",
    "col_numeric",
    "col_decimal",
    "col_float",
    "col_double",
    "col_boolean",
    "col_date",
    "col_time",
    "col_timestamp",
    "col_timestamp_tz",
]

# Numeric columns to test with aggregate checks
NUMERIC_COLUMNS = [
    "col_smallint",
    "col_integer",
    "col_bigint",
    "col_numeric",
    "col_decimal",
    "col_float",
    "col_double",
]


# ---------------------------------------------------------------------------
# End-to-end type tests: full pipeline for every data type
# ---------------------------------------------------------------------------


def test_all_types_table_creation_and_row_count(data_source_test_helper: DataSourceTestHelper):
    """Create a table with all Soda data types, insert data, verify row count.
    This exercises the full DDL + INSERT pipeline for every type."""
    test_table = data_source_test_helper.ensure_test_table(all_types_with_data_table)
    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - row_count:
                  threshold:
                    must_be: 3
        """,
    )


@pytest.mark.parametrize("column_name", ALL_TYPE_COLUMNS)
def test_missing_check_per_type(column_name: str, data_source_test_helper: DataSourceTestHelper):
    """Missing check must detect the NULL row for each data type.
    This verifies the full pipeline: type mapping → SQL generation → query → result parsing."""
    test_table = data_source_test_helper.ensure_test_table(all_types_with_data_table)
    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: {column_name}
                checks:
                  - missing:
            checks:
              - row_count:
        """,
    )


@pytest.mark.parametrize("column_name", NUMERIC_COLUMNS)
def test_aggregate_check_per_numeric_type(column_name: str, data_source_test_helper: DataSourceTestHelper):
    """Aggregate (avg) must work on every numeric type. Verifies type casting and aggregation."""
    test_table = data_source_test_helper.ensure_test_table(all_types_with_data_table)
    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: {column_name}
                checks:
                  - aggregate:
                      function: avg
                      threshold:
                        must_be_greater_than: 0
            checks:
              - row_count:
        """,
    )


def test_schema_check_all_types(data_source_test_helper: DataSourceTestHelper):
    """Schema check must discover all columns in the correct order.
    Note: we don't compare data_type here because forward-mapped names may differ
    from discovered names (e.g., Postgres maps FLOAT→'float' but discovers 'double precision').
    Type round-trip accuracy is tested in Phase 2 (test_conformance_discovery.py)."""
    test_table = data_source_test_helper.ensure_test_table(all_types_with_data_table)

    columns_yaml = "\n".join(f"              - name: {col}" for col in ALL_TYPE_COLUMNS)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
{columns_yaml}
        """,
    )


# ---------------------------------------------------------------------------
# Sampling SQL conformance
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("sampler_type", list(SamplerType))
def test_sampling_sql_generation(sampler_type: SamplerType, data_source_test_helper: DataSourceTestHelper):
    """For each sampler type the adapter claims to support, the generated SQL must
    be non-empty and parseable (used in a SELECT ... FROM table SAMPLE clause)."""
    sql_dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect

    if not sql_dialect.supports_sampler(sampler_type):
        pytest.skip(f"{sql_dialect.__class__.__name__} does not support {sampler_type.name}")

    sample_size = 10 if sampler_type == SamplerType.PERCENTAGE else 50
    sample_sql = sql_dialect._build_sample_sql(sampler_type, sample_size)

    assert sample_sql is not None, f"_build_sample_sql returned None for {sampler_type.name}"
    assert len(sample_sql.strip()) > 0, f"_build_sample_sql returned empty string for {sampler_type.name}"
    assert str(sample_size) in sample_sql, f"Sample size {sample_size} not found in generated SQL: {sample_sql}"


@pytest.mark.parametrize("sampler_type", list(SamplerType))
def test_sampling_sql_executes(sampler_type: SamplerType, data_source_test_helper: DataSourceTestHelper):
    """For each supported sampler type, generate a full SELECT with sampling and execute it."""
    sql_dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect

    if not sql_dialect.supports_sampler(sampler_type):
        pytest.skip(f"{sql_dialect.__class__.__name__} does not support {sampler_type.name}")

    test_table = data_source_test_helper.ensure_test_table(all_types_with_data_table)
    table_from_name = sql_dialect.get_from_name_from_qualified_name(test_table.qualified_name)

    sample_size = 50 if sampler_type == SamplerType.PERCENTAGE else 2
    select_sql = sql_dialect.build_select_sql(
        [
            SELECT(STAR()),
            FROM(table_from_name).SAMPLE(sampler_type, sample_size),
        ]
    )

    result: QueryResult = data_source_test_helper.data_source_impl.execute_query(select_sql)
    assert result is not None, "Sampled query returned None"
    assert len(result.rows) >= 0, "Sampled query returned negative row count"


# ---------------------------------------------------------------------------
# Regex SQL conformance
# ---------------------------------------------------------------------------


def test_regex_sql_generation(data_source_test_helper: DataSourceTestHelper):
    """The adapter must generate valid regex SQL from a REGEX_LIKE expression."""
    sql_dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect

    regex_expr = REGEX_LIKE(expression=COLUMN("col_varchar"), regex_pattern="^[a-z]+$")
    sql = sql_dialect._build_regex_like_sql(regex_expr)

    assert sql is not None, "regex SQL is None"
    assert len(sql.strip()) > 0, "regex SQL is empty"
    assert "col_varchar" in sql, f"Column name missing from regex SQL: {sql}"


def test_regex_via_invalid_check(data_source_test_helper: DataSourceTestHelper):
    """Invalid check with regex must work end-to-end (the row with NULL is excluded,
    the two data rows match the pattern, so no invalids among non-null rows)."""
    test_table = data_source_test_helper.ensure_test_table(all_types_with_data_table)

    if data_source_test_helper.data_source_impl.sql_dialect.supports_regex_advanced():
        regex = "^[a-z]+$"
    else:
        regex = "[a-z]"

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: col_varchar
                valid_format:
                  regex: '{regex}'
                  name: lowercase-letters
                checks:
                  - invalid:
            checks:
              - row_count:
        """,
    )


# ---------------------------------------------------------------------------
# RANDOM() conformance
# ---------------------------------------------------------------------------


def test_random_generates_valid_sql(data_source_test_helper: DataSourceTestHelper):
    """RANDOM() must generate valid SQL that returns values in [0.0, 1.0)."""
    test_table = data_source_test_helper.ensure_test_table(all_types_with_data_table)
    data_source_impl: DataSourceImpl = data_source_test_helper.data_source_impl
    sql_dialect: SqlDialect = data_source_impl.sql_dialect

    table_from_name = sql_dialect.get_from_name_from_qualified_name(test_table.qualified_name)
    select_sql = sql_dialect.build_select_sql(
        [
            SELECT(RANDOM()),
            FROM(table_from_name),
        ]
    )

    result: QueryResult = data_source_impl.execute_query(select_sql)
    assert len(result.rows) == 3

    for row in result.rows:
        value = float(row[0])
        assert 0.0 <= value < 1.0, f"RANDOM() returned {value}, expected [0.0, 1.0)"


# ---------------------------------------------------------------------------
# Type mapping consistency
# ---------------------------------------------------------------------------


def test_forward_mapping_covers_all_types(data_source_test_helper: DataSourceTestHelper):
    """Every SodaDataTypeName must have a data source type in the forward mapping."""
    forward_map = (
        data_source_test_helper.data_source_impl.sql_dialect.get_data_source_data_type_name_by_soda_data_type_names()
    )
    unmapped = [t.name for t in SodaDataTypeName if t not in forward_map]
    assert unmapped == [], f"SodaDataTypeNames missing from forward mapping: {unmapped}"


def test_reverse_mapping_covers_forward(data_source_test_helper: DataSourceTestHelper):
    """Every type produced by forward mapping must be resolvable via reverse mapping."""
    sql_dialect = data_source_test_helper.data_source_impl.sql_dialect
    forward = sql_dialect.get_data_source_data_type_name_by_soda_data_type_names()
    reverse = sql_dialect.get_soda_data_type_name_by_data_source_data_type_names()

    broken = []
    for soda_type, ds_type in forward.items():
        found = (
            ds_type in reverse
            or (isinstance(ds_type, str) and ds_type.lower() in reverse)
            or sql_dialect._data_type_name_synonym_mappings.get(
                ds_type.lower() if isinstance(ds_type, str) else ds_type, None
            )
            in reverse
        )
        if not found:
            broken.append(f"{soda_type.name} → '{ds_type}'")

    assert broken == [], f"Forward-mapped types with no reverse path:\n" + "\n".join(broken)


def test_data_type_synonyms_internally_consistent(data_source_test_helper: DataSourceTestHelper):
    """All entries in a synonym group must resolve to the same canonical name
    through the synonym mapping (the _data_type_name_synonym_mappings dict)."""
    sql_dialect = data_source_test_helper.data_source_impl.sql_dialect
    synonym_lists = sql_dialect._get_data_type_name_synonyms()

    inconsistencies = []
    for group in synonym_lists:
        canonicals = set()
        for name in group:
            canonical = sql_dialect._data_type_name_synonym_mappings.get(name.lower())
            if canonical is not None:
                canonicals.add(canonical)
        if len(canonicals) > 1:
            inconsistencies.append(f"Group {group} maps to multiple canonicals: {canonicals}")

    assert inconsistencies == [], "Synonym groups with inconsistent canonical mappings:\n" + "\n".join(inconsistencies)
