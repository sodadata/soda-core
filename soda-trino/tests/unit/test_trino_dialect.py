from datetime import datetime, timezone

import pytest
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_trino.common.data_sources.trino_data_source import TrinoSqlDialect


def test_random():
    sql_dialect: TrinoSqlDialect = TrinoSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == 'SELECT RANDOM()\nFROM "a"'


def test_literal_datetime_pads_year_to_four_digits():
    """C strftime("%Y") drops leading zeros for years < 1000 on glibc, producing
    TIMESTAMP literals Trino cannot parse; the year must always be 4 digits."""
    sql_dialect: TrinoSqlDialect = TrinoSqlDialect()
    assert sql_dialect.literal(datetime(200, 12, 17, 1, 2, 3, 456)) == "TIMESTAMP '0200-12-17 01:02:03.000456'"
    assert (
        sql_dialect.literal(datetime(200, 12, 17, 1, 2, 3, 456, tzinfo=timezone.utc))
        == "TIMESTAMP '0200-12-17 01:02:03.000456+00:00'"
    )


def test_time_delta_renders_date_diff_seconds_form():
    from datetime import datetime

    from soda_core.common.sql_ast import LITERAL, TIME_DELTA, SqlExpressionStr

    sql = TrinoSqlDialect().build_expression_sql(
        TIME_DELTA(LITERAL(datetime(2020, 6, 20)), SqlExpressionStr('"ts"'), "days", 1)
    )
    assert sql == "cast(floor(date_diff('second', TIMESTAMP '2020-06-20 00:00:00.000000', (\"ts\")) / 86400.0) as int)"


def test_time_delta_date_diff_count_2_hours():
    from datetime import datetime

    from soda_core.common.sql_ast import LITERAL, TIME_DELTA, SqlExpressionStr

    sql = TrinoSqlDialect().build_expression_sql(
        TIME_DELTA(LITERAL(datetime(2020, 6, 20)), SqlExpressionStr('"ts"'), "hours", 2)
    )
    assert sql == "cast(floor(date_diff('second', TIMESTAMP '2020-06-20 00:00:00.000000', (\"ts\")) / 7200.0) as int)"


def test_add_interval_renders_date_add_lowercase_unit():
    from datetime import datetime

    from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, SqlExpressionStr

    sql = TrinoSqlDialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(datetime(2020, 6, 20)), "days", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == "date_add('day', ((soda_partition__ + 1) * 1), TIMESTAMP '2020-06-20 00:00:00.000000')"


def test_add_interval_weeks_unit_name():
    from datetime import datetime

    from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, SqlExpressionStr

    sql = TrinoSqlDialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(datetime(2020, 6, 20)), "weeks", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == "date_add('week', ((soda_partition__ + 1) * 1), TIMESTAMP '2020-06-20 00:00:00.000000')"


def test_percentile_within_group_renders_approx_percentile():
    from soda_core.common.sql_ast import COLUMN, PERCENTILE_WITHIN_GROUP

    sql = TrinoSqlDialect().build_expression_sql(PERCENTILE_WITHIN_GROUP(COLUMN("c"), 0.25))
    assert sql == 'approx_percentile("c", 0.25)'


def test_supports_percentile_within_group_is_true():
    assert TrinoSqlDialect().supports_percentile_within_group() is True


@pytest.mark.parametrize(
    "raw_type, expected_name",
    [
        ("char(100)", "char"),
        ("decimal(10,2)", "decimal"),
        ("timestamp(3) with time zone", "timestamp with time zone"),
    ],
)
def test_column_metadata_keeps_the_raw_type_as_source_data_type(raw_type, expected_name):
    column = TrinoSqlDialect().build_column_metadatas_from_query_result(
        QueryResult(rows=[("c", raw_type)], columns=[("column_name",), ("data_type",)])
    )[0]

    assert column.sql_data_type.name == expected_name
    assert column.source_data_type == raw_type
