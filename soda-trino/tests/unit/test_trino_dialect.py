from datetime import datetime, timezone

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
