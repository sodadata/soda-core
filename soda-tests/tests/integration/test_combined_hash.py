"""COMBINED_HASH must give two rows the data source stores differently two different keys.

It is the row key behind the multi-column duplicate check, `__soda_row_id` and the diagnostics
warehouse keys, so a collision reports unique rows as duplicates or attaches the wrong source rows
to a failure. Every part of the rendered key can collide on its own: the separator between
operands (#2929: BigQuery joined them with an empty string), the cast of each operand to text
(width defaults, lossy number and datetime formats) and the NULL sentinel.

Each case is a pair of rows that differ only where one of those parts can lose the difference.
A pair the data source itself cannot store apart (Oracle's '' is NULL, a source that keeps only
milliseconds) is skipped, not counted as a collision.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_fixtures import test_datasource
from helpers.test_table import TestTableSpecification
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.metadata_types import SqlDataType
from soda_core.common.sql_ast import (
    ALIAS,
    COLUMN,
    COMBINED_HASH,
    CREATE_TABLE_COLUMN,
    CREATE_TABLE_IF_NOT_EXISTS,
    DROP_TABLE_IF_EXISTS,
    EQ,
    FROM,
    LITERAL,
    SELECT,
    WHERE,
)
from soda_core.common.sql_dialect import SqlDialect

_UTC = datetime.timezone.utc
_PLUS_1 = datetime.timezone(datetime.timedelta(hours=1))
_PLUS_2 = datetime.timezone(datetime.timedelta(hours=2))


@dataclass(frozen=True)
class HashCase:
    case_id: str
    values_a: dict
    values_b: dict
    rows_differ: bool = True

    @property
    def columns(self) -> list[str]:
        return list(self.values_a)


def _case(case_id: str, a: dict, b: dict, rows_differ: bool = True) -> HashCase:
    assert list(a) == list(b), f"{case_id}: both rows must set the same columns"
    return HashCase(case_id=case_id, values_a=a, values_b=b, rows_differ=rows_differ)


_CASES: list[HashCase] = [
    # The separator between operands.
    _case("separator_text", {"text_a": "123", "text_b": "4567"}, {"text_a": "1234", "text_b": "567"}),
    _case("separator_integer", {"int_a": 123, "int_b": 4567}, {"int_a": 1234, "int_b": 567}),
    _case("separator_text_and_integer", {"text_a": "12", "int_a": 34}, {"text_a": "123", "int_a": 4}),
    _case(
        "separator_three_operands",
        {"text_a": "1", "text_b": "23", "int_a": 4},
        {"text_a": "12", "text_b": "3", "int_a": 4},
    ),
    _case("separator_inside_a_value", {"text_a": "a||", "text_b": "b"}, {"text_a": "a", "text_b": "||b"}),
    # The NULL sentinel.
    _case("null_position", {"text_a": None, "text_b": "x"}, {"text_a": "x", "text_b": None}),
    _case("null_or_empty_text", {"text_a": None, "text_b": "x"}, {"text_a": "", "text_b": "x"}),
    _case("null_or_zero", {"int_a": None, "int_b": 1}, {"int_a": 0, "int_b": 1}),
    _case("null_or_sentinel_text", {"text_a": None}, {"text_a": "__SODA_NULL__"}),
    # The width of the text an operand is cast to.
    _case("text_differs_after_30_chars", {"text_a": "a" * 30 + "1"}, {"text_a": "a" * 30 + "2"}),
    _case("text_differs_after_256_chars", {"text_a": "a" * 256 + "1"}, {"text_a": "a" * 256 + "2"}),
    _case(
        "second_operand_differs_after_30_chars",
        {"text_a": "k", "text_b": "b" * 30 + "1"},
        {"text_a": "k", "text_b": "b" * 30 + "2"},
    ),
    _case("multibyte_text_differs_after_30_bytes", {"text_a": "é" * 16 + "1"}, {"text_a": "é" * 16 + "2"}),
    _case("latin_text", {"text_a": "café"}, {"text_a": "cafö"}),
    _case("cjk_text", {"text_a": "日本"}, {"text_a": "中国"}),
    # Numbers rendered as text.
    _case("bigint_beyond_double_precision", {"big": 9007199254740993}, {"big": 9007199254740992}),
    _case("float_7_significant_digits", {"float_value": 123456.1}, {"float_value": 123456.2}),
    _case("float_17_significant_digits", {"float_value": 0.1}, {"float_value": 0.10000000000000002}),
    # 2**70 and 2**70 + 2**18: 32 characters at scale 9, and exact as doubles, which is how some sources
    # parse an insert literal.
    _case(
        "decimal_32_chars_wide",
        {"decimal_value": Decimal("1180591620717411303424.0")},
        {"decimal_value": Decimal("1180591620717411565568.0")},
    ),
    _case(
        "decimal_last_fractional_digit",
        {"decimal_value": Decimal("1.000000001")},
        {"decimal_value": Decimal("1.000000002")},
    ),
    # Dates and times rendered as text.
    _case("date_century", {"date_value": datetime.date(1926, 3, 1)}, {"date_value": datetime.date(2026, 3, 1)}),
    _case("date_day", {"date_value": datetime.date(2026, 3, 1)}, {"date_value": datetime.date(2026, 3, 2)}),
    _case(
        "timestamp_century",
        {"timestamp_value": datetime.datetime(1926, 3, 1, 10, 0, 1)},
        {"timestamp_value": datetime.datetime(2026, 3, 1, 10, 0, 1)},
    ),
    _case(
        "timestamp_seconds",
        {"timestamp_value": datetime.datetime(2026, 3, 1, 10, 0, 1)},
        {"timestamp_value": datetime.datetime(2026, 3, 1, 10, 0, 2)},
    ),
    _case(
        "timestamp_milliseconds",
        {"timestamp_value": datetime.datetime(2026, 3, 1, 10, 0, 0, 100000)},
        {"timestamp_value": datetime.datetime(2026, 3, 1, 10, 0, 0, 200000)},
    ),
    _case(
        "timestamp_microseconds",
        {"timestamp_value": datetime.datetime(2026, 3, 1, 10, 0, 0, 1)},
        {"timestamp_value": datetime.datetime(2026, 3, 1, 10, 0, 0, 2)},
    ),
    _case(
        "timestamp_tz_seconds",
        {"timestamp_tz_value": datetime.datetime(2026, 3, 1, 10, 0, 1, tzinfo=_UTC)},
        {"timestamp_tz_value": datetime.datetime(2026, 3, 1, 10, 0, 2, tzinfo=_UTC)},
    ),
    _case(
        "timestamp_tz_microseconds",
        {"timestamp_tz_value": datetime.datetime(2026, 3, 1, 10, 0, 0, 1, tzinfo=_UTC)},
        {"timestamp_tz_value": datetime.datetime(2026, 3, 1, 10, 0, 0, 2, tzinfo=_UTC)},
    ),
    _case(
        "timestamp_tz_same_wall_clock_other_offset",
        {"timestamp_tz_value": datetime.datetime(2026, 3, 1, 10, 0, 0, tzinfo=_PLUS_1)},
        {"timestamp_tz_value": datetime.datetime(2026, 3, 1, 10, 0, 0, tzinfo=_PLUS_2)},
    ),
    _case("time_seconds", {"time_value": datetime.time(10, 0, 1)}, {"time_value": datetime.time(10, 0, 2)}),
    _case(
        "time_microseconds",
        {"time_value": datetime.time(10, 0, 0, 1)},
        {"time_value": datetime.time(10, 0, 0, 2)},
    ),
    # Equal rows must still share a key, or every case above would pass on a hash that never repeats.
    _case("equal_rows", {"text_a": "x", "int_a": None}, {"text_a": "x", "int_a": None}, rows_differ=False),
]

_TABLE_COLUMNS: list[str] = [
    "case_id",
    "text_a",
    "text_b",
    "int_a",
    "int_b",
    "big",
    "float_value",
    "decimal_value",
    "date_value",
    "timestamp_value",
    "timestamp_tz_value",
    "time_value",
]


def _row(case: HashCase, values: dict) -> tuple:
    return tuple(case.case_id if column == "case_id" else values.get(column) for column in _TABLE_COLUMNS)


test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("combined_hash")
    .column_varchar("case_id", 64)
    .column_varchar("text_a", 400)
    .column_varchar("text_b", 400)
    .column_integer("int_a")
    .column_integer("int_b")
    .column_bigint("big")
    .column_double("float_value")
    .column_decimal("decimal_value", 31, 9)
    .column_date("date_value")
    .column_timestamp("timestamp_value", datetime_precision=6)
    .column_timestamp_tz("timestamp_tz_value", datetime_precision=6)
    .column_time("time_value")
    .rows(rows=[row for case in _CASES for row in (_row(case, case.values_a), _row(case, case.values_b))])
    .build()
)


def _skip_without_row_hashing(data_source_impl: DataSourceImpl) -> None:
    if not data_source_impl.supports_row_hashing:
        pytest.skip(f"{test_datasource} cannot express a row hash")


def _assert_keys(
    data_source_impl: DataSourceImpl,
    from_name: str,
    columns: list[str],
    rows_differ: bool,
    case_id: str,
    where: Optional[WHERE] = None,
) -> None:
    sql_dialect: SqlDialect = data_source_impl.sql_dialect
    operands = [COLUMN(column) for column in columns]
    exact_hash = COMBINED_HASH(operands)
    folding_hash = COMBINED_HASH(operands, comparison_mode="database-equality")
    select_sql = sql_dialect.build_select_sql(
        [
            SELECT([*operands, ALIAS(exact_hash, "exact_key"), ALIAS(folding_hash, "folding_key")]),
            FROM(from_name),
            *([where] if where else []),
        ]
    )
    rows = data_source_impl.execute_query(select_sql).rows

    assert len(rows) == 2, f"expected the case's two rows, got {rows}"
    stored_a, stored_b = (tuple(row[: len(columns)]) for row in rows)
    if rows_differ and stored_a == stored_b:
        pytest.skip(f"{test_datasource} stores both rows of {case_id} as {stored_a}")

    exact_a, exact_b = rows[0][-2], rows[1][-2]
    folding_a, folding_b = rows[0][-1], rows[1][-1]
    if rows_differ:
        assert exact_a != exact_b, (
            f"{test_datasource} gives the different rows {stored_a} and {stored_b} the same key {exact_a}\n"
            f"{sql_dialect.build_expression_sql(exact_hash)}"
        )
        assert folding_a != folding_b, (
            f"{test_datasource} gives the different rows {stored_a} and {stored_b} the same "
            f"database-equality key {folding_a}\n{sql_dialect.build_expression_sql(folding_hash)}"
        )
    else:
        assert exact_a == exact_b, f"{test_datasource} gives the equal rows {stored_a} two keys"
        assert folding_a == folding_b, f"{test_datasource} gives the equal rows {stored_a} two keys"


@pytest.mark.parametrize("case", _CASES, ids=[case.case_id for case in _CASES])
def test_combined_hash_tells_rows_apart(data_source_test_helper: DataSourceTestHelper, case: HashCase):
    data_source_impl: DataSourceImpl = data_source_test_helper.data_source_impl
    _skip_without_row_hashing(data_source_impl)
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    qualified_name = data_source_test_helper.get_qualified_name_from_test_table(test_table)

    _assert_keys(
        data_source_impl=data_source_impl,
        from_name=data_source_impl.sql_dialect.get_from_name_from_qualified_name(qualified_name),
        columns=case.columns,
        rows_differ=case.rows_differ,
        case_id=case.case_id,
        where=WHERE(EQ(COLUMN("case_id"), LITERAL(case.case_id))),
    )


@dataclass(frozen=True)
class NativeTypeCase:
    case_id: str
    native_type: str
    # SQL literals rather than Python values: these types have no SodaDataTypeName, and some need
    # vendor syntax to be stored at all (N'' for nvarchar).
    literal_a: str
    literal_b: str


_GUID_A = "'6F9619FF-8B86-D011-B42D-00C04FC964FF'"
_GUID_B = "'6F9619FF-8B86-D011-B42D-00C04FC964FE'"

# Column types the shared table above cannot create, but that customers key on.
_NATIVE_TYPE_CASES: dict[str, list[NativeTypeCase]] = {
    "sqlserver": [
        NativeTypeCase("uniqueidentifier", "uniqueidentifier", _GUID_A, _GUID_B),
        NativeTypeCase("datetime_seconds", "datetime", "'2026-03-01T10:00:01'", "'2026-03-01T10:00:02'"),
        NativeTypeCase("datetime_milliseconds", "datetime", "'2026-03-01T10:00:00.100'", "'2026-03-01T10:00:00.200'"),
        NativeTypeCase("money_cents_fraction", "money", "1.2345", "1.2346"),
        NativeTypeCase("real_7_significant_digits", "real", "123456.1", "123456.2"),
        NativeTypeCase("nvarchar_cjk", "nvarchar(100)", "N'日本'", "N'中国'"),
        NativeTypeCase("nvarchar_outside_code_page", "nvarchar(100)", "N'Łukasz'", "N'Lukasz'"),
        NativeTypeCase("nvarchar_differs_after_30_chars", "nvarchar(100)", f"N'{'a' * 30}1'", f"N'{'a' * 30}2'"),
        NativeTypeCase(
            "datetime2_microseconds",
            "datetime2(7)",
            "'2026-03-01T10:00:00.000001'",
            "'2026-03-01T10:00:00.000002'",
        ),
        NativeTypeCase(
            "datetimeoffset_microseconds",
            "datetimeoffset(7)",
            "'2026-03-01T10:00:00.000001+00:00'",
            "'2026-03-01T10:00:00.000002+00:00'",
        ),
        NativeTypeCase(
            "datetimeoffset_same_wall_clock_other_offset",
            "datetimeoffset(7)",
            "'2026-03-01T10:00:00+01:00'",
            "'2026-03-01T10:00:00+02:00'",
        ),
    ],
    "fabric": [
        NativeTypeCase("uniqueidentifier", "uniqueidentifier", _GUID_A, _GUID_B),
        NativeTypeCase("real_7_significant_digits", "real", "123456.1", "123456.2"),
        NativeTypeCase(
            "datetime2_microseconds",
            "datetime2(6)",
            "'2026-03-01T10:00:00.000001'",
            "'2026-03-01T10:00:00.000002'",
        ),
    ],
    "oracle": [
        NativeTypeCase(
            "date_time_of_day",
            "date",
            "TO_DATE('2026-03-01 10:00:01', 'YYYY-MM-DD HH24:MI:SS')",
            "TO_DATE('2026-03-01 10:00:02', 'YYYY-MM-DD HH24:MI:SS')",
        ),
        NativeTypeCase(
            "timestamp_with_time_zone_same_wall_clock_other_offset",
            "timestamp with time zone",
            "TIMESTAMP '2026-03-01 10:00:00 +01:00'",
            "TIMESTAMP '2026-03-01 10:00:00 +02:00'",
        ),
        NativeTypeCase("nvarchar2_cjk", "nvarchar2(100)", "N'日本'", "N'中国'"),
    ],
}
_NATIVE_TYPE_CASES["synapse"] = _NATIVE_TYPE_CASES["sqlserver"]

_native_cases_here: list[NativeTypeCase] = _NATIVE_TYPE_CASES.get(test_datasource, [])


@pytest.mark.skipif(not _native_cases_here, reason=f"no native column types listed for {test_datasource}")
@pytest.mark.parametrize("case", _native_cases_here, ids=[case.case_id for case in _native_cases_here])
def test_combined_hash_tells_rows_apart_on_native_types(
    data_source_test_helper: DataSourceTestHelper, case: NativeTypeCase
):
    data_source_impl: DataSourceImpl = data_source_test_helper.data_source_impl
    _skip_without_row_hashing(data_source_impl)
    sql_dialect: SqlDialect = data_source_impl.sql_dialect
    qualified_name = sql_dialect.qualify_dataset_name(
        data_source_test_helper.dataset_prefix, f"sodatest_hash_native_{case.case_id}"
    )

    drop_sql = sql_dialect.build_drop_table_sql(DROP_TABLE_IF_EXISTS(fully_qualified_table_name=qualified_name))
    data_source_impl.execute_update(drop_sql)
    try:
        data_source_impl.execute_update(
            sql_dialect.build_create_table_sql(
                CREATE_TABLE_IF_NOT_EXISTS(
                    fully_qualified_table_name=qualified_name,
                    columns=[CREATE_TABLE_COLUMN(name="v", type=SqlDataType(name=case.native_type), nullable=True)],
                )
            )
        )
        for literal in (case.literal_a, case.literal_b):
            data_source_impl.execute_update(
                f"INSERT INTO {qualified_name} ({sql_dialect.quote_default('v')}) VALUES ({literal})"
            )
        data_source_impl.data_source_connection.commit()

        _assert_keys(
            data_source_impl=data_source_impl,
            from_name=sql_dialect.get_from_name_from_qualified_name(qualified_name),
            columns=["v"],
            rows_differ=True,
            case_id=case.case_id,
        )
    finally:
        data_source_impl.execute_update(drop_sql)
