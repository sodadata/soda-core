from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from soda.sodacl.format_cfg import FormatCfg


def test_formats(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    assert_format_values(
        "integer",
        data_source_fixture,
        table_name,
        passing_values=["0", "1234567890", "-0", "- 1234567890", "+0", "+1"],
        failing_values=["", "a", " "],
    )

    assert_format_values(
        "positive integer",
        data_source_fixture,
        table_name,
        passing_values=["0", "1234567890", "+0", "+1"],
        failing_values=[
            "",
            "a",
            " ",
            "-0",
            "- 1234567890",
        ],
    )

    # def test_negative_integer_format(data_source_fixture: DataSourceFixture):
    #     table_name = data_source_fixture.ensure_test_table(customers_test_table)

    assert_format_values(
        "negative integer",
        data_source_fixture,
        table_name,
        passing_values=[
            "0",
            "-0",
            "- 1234567890",
        ],
        failing_values=["", "a", " ", "1234567890", "+0", "+1"],
    )

    # def test_percentage_format(data_source_fixture: DataSourceFixture):
    #     table_name = data_source_fixture.ensure_test_table(customers_test_table)

    assert_format_values(
        "percentage",
        data_source_fixture,
        table_name,
        passing_values=[
            "0%",
            " 0 % ",
            "- 0 %",
            "+ 0 %",
            "010%",
            "0.0 %",
            "0,0  %",
            ".0 %",
            ",0 %",
            "99.99%",
        ],
        failing_values=["", " ", "%", "a %", "0", "0.0"],
    )

    # def test_date_iso_format(data_source_fixture: DataSourceFixture):
    #     table_name = data_source_fixture.ensure_test_table(customers_test_table)

    assert_format_values(
        "date iso 8601",
        data_source_fixture,
        table_name,
        passing_values=[
            "2020-02-08",
            "2020-W06-5",
            "2020-039",
            "20200208",
            "2020W065",
            "2020W06",
            "2020039",
            "2020-02-08T09",
            "2020-02-08 09",
            "2020-02-08 09:30",
            "2020-02-08 09:30:26",
            "2020-02-08 09:30:26.123",
            "20200208T080910,123",
            "20200208T080910.123",
            "20200208T080910",
            "20200208T0809",
            "20200208T08",
            "2020-W06-5 09",
            "2020-039 09",
            "2020-02-08 09+07:00",
            "2020-02-08 09+07:00",
            "2020-02-08 09-0100",
            "2020-02-08 09Z",
            "2020-04-30",
            "2020-04-30T00:00:00.000",
        ],
        failing_values=[
            "",
            " ",
            "a",
            "9999-01-01",
            "2000-13-01",
            "2000-01-32",
        ],
    )


def assert_format_values(format, data_source_fixture: DataSourceFixture, table_name, passing_values, failing_values):
    format_regex = FormatCfg.default_formats[format]

    data_source = data_source_fixture.data_source
    qualified_table_name = data_source.qualified_table_name(table_name)

    values = []
    expressions = []
    expected_values = []
    for passing_value in passing_values:
        expressions.append(
            data_source.expr_regexp_like(
                f"'{passing_value}'",
                data_source.escape_regex(format_regex),
            )
        )
        values.append(passing_value)
        expected_values.append(True)
    for failing_value in failing_values:
        expressions.append(
            data_source.expr_regexp_like(
                f"'{failing_value}'",
                data_source.escape_regex(format_regex),
            )
        )
        values.append(failing_value)
        expected_values.append(False)

    expressions_sql = ",\n  ".join(expressions)
    sql = f"SELECT \n  {expressions_sql} FROM {qualified_table_name}"
    row = data_source_fixture._fetch_all(sql)[0]

    failures_messages = []
    for index, expected_value in enumerate(expected_values):
        actual_value = row[index]
        if actual_value != expected_value:
            if expected_values[index]:
                failures_messages.append(f'{values[index]} no valid "{format}", expected valid')
            else:
                failures_messages.append(f'{values[index]} valid "{format}", expected not valid')

    if failures_messages:
        raise AssertionError("\n".join(failures_messages))
