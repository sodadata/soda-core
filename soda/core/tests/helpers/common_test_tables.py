import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from random import choice, randint, uniform

from faker import Faker
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

utc = timezone.utc


def generate_customer_records(num_records):
    fake = Faker()
    start_date = datetime(2020, 6, 24, 0, 4, 10)
    records = []

    for i in range(num_records):
        backfill_day = i + 1 - num_records
        current_date = start_date + timedelta(days=backfill_day)
        current_date_with_tz = current_date.astimezone(utc)

        record = (
            fake.uuid4(),  # id
            Decimal(str(round(uniform(1.0, 1000.0), 2))),  # cst_size
            fake.word(),  # cst_size_txt
            randint(1, 10000),  # distance
            f"{randint(0, 100)}%",  # pct
            choice(["A", "B", "C", "D"]),  # cat
            choice(["PL", "BE", "NL", "US"]),  # country
            fake.zipcode(),  # zip
            fake.email(),  # email
            fake.date_this_century(),  # date_updated
            current_date,  # ts
            current_date_with_tz,  # ts_with_tz
        )
        records.append(record)

    return records


customers_test_table = TestTable(
    name="Customers",
    create_view=os.getenv("TEST_WITH_VIEWS", False),
    columns=[
        ("id", DataType.TEXT),
        ("cst_size", DataType.DECIMAL),
        ("cst_size_txt", DataType.TEXT),
        ("distance", DataType.INTEGER),
        ("pct", DataType.TEXT),
        ("cat", DataType.TEXT),
        ("country", DataType.TEXT),
        ("zip", DataType.TEXT),
        ("email", DataType.TEXT),
        ("date_updated", DataType.DATE),
        ("ts", DataType.TIMESTAMP),
        ("ts_with_tz", DataType.TIMESTAMP_TZ),
    ],
    # fmt: off
    values=[
        # TODO evolve this to a simple table data structure that can handle most of the basic test cases
        # I think the basic row count should be 10 or 20 so that It is predictable when reading this data
        ('ID1',  1,    "1",     0,    "- 28,42 %", "HIGH",   'BE', '2360', 'john.doe@example.com',    date(2020, 6, 23), datetime(2020, 6, 23, 0, 0, 10), datetime(2020, 6, 23, 0, 0, 10, tzinfo=utc)),
        ('ID2',  .5,   ".5",    -999, "+22,75 %",  "HIGH",   'BE', '2361', 'JOE.SMOE@EXAMPLE.COM',    date(2020, 6, 23), datetime(2020, 6, 23, 0, 1, 10), datetime(2020, 6, 23, 0, 1, 10, tzinfo=utc)),
        ('ID3',  -1.2, "-1.2",  5,    ".92 %",     "MEDIUM", 'BE', '2362', 'milan.lukáč@example.com', date(2020, 6, 23), datetime(2020, 6, 23, 0, 2, 10), datetime(2020, 6, 23, 0, 2, 10, tzinfo=utc)),
        ('ID4',  -.4,  "-.4",   10,   "0.26 %",    "LOW",    'BE', '2363', 'john.doe+1@ĚxamplÉ.com',  date(2020, 6, 23), datetime(2020, 6, 23, 0, 3, 10), datetime(2020, 6, 23, 0, 3, 10, tzinfo=utc)),
        ('ID5',  -3,   "-3",    999,  "18,32%",    None,     'BE', '2364', 'invalid@email',           date(2020, 6, 23), datetime(2020, 6, 23, 0, 4, 10), datetime(2020, 6, 23, 0, 4, 10, tzinfo=utc)),
        ('ID6',  5,    "5",     999,  "18,32%",    None,     'BE', '2365', ' ',                       date(2020, 6, 23), datetime(2020, 6, 23, 0, 5, 10), datetime(2020, 6, 23, 0, 5, 10, tzinfo=utc)),
        ('ID7',  6,    "6",     999,  "error",     None,     'NL', '2360', None,                      date(2020, 6, 24), datetime(2020, 6, 24, 0, 1, 10), datetime(2020, 6, 24, 0, 1, 10, tzinfo=utc)),
        ('ID8',  None, None,    999,  "No value",  None,     'NL', '2361', None,                      date(2020, 6, 24), datetime(2020, 6, 24, 0, 2, 10), datetime(2020, 6, 24, 0, 2, 10, tzinfo=utc)),
        ('ID9',  None, None,    999,  "N/A",       None,     'NL', '2362', None,                      date(2020, 6, 24), datetime(2020, 6, 24, 0, 3, 10), datetime(2020, 6, 24, 0, 3, 10, tzinfo=utc)),
        (None,   None, None,    None, None,        "HIGH",   'NL', '2363', None,                      date(2020, 6, 24), datetime(2020, 6, 24, 0, 4, 10), datetime(2020, 6, 24, 0, 4, 10, tzinfo=utc)),
    ]
    # fmt: on
)

customers_huge_test_table = TestTable(
    name="CustomersHuge",
    create_view=os.getenv("TEST_WITH_VIEWS", False),
    columns=[
        ("id", DataType.TEXT),
        ("cst_size", DataType.DECIMAL),
        ("cst_size_txt", DataType.TEXT),
        ("distance", DataType.INTEGER),
        ("pct", DataType.TEXT),
        ("cat", DataType.TEXT),
        ("country", DataType.TEXT),
        ("zip", DataType.TEXT),
        ("email", DataType.TEXT),
        ("date_updated", DataType.DATE),
        ("ts", DataType.TIMESTAMP),
        ("ts_with_tz", DataType.TIMESTAMP_TZ),
    ],
    values=generate_customer_records(120),
)

customers_dist_check_test_table = TestTable(
    name="CustomersDist",
    create_view=os.getenv("TEST_WITH_VIEWS", False),
    columns=[
        ("id", DataType.TEXT),
        ("cst_size", DataType.DECIMAL),
        ("full_null", DataType.TEXT),
    ],
    # fmt: off
    values=[
        # TODO evolve this to a simple table data structure that can handle most of the basic test cases
        # I think the basic row count should be 10 or 20 so that It is predictable when reading this data
        ('ID1',  1, None),
        ('ID2',  1, None),
        ('ID3',  2, None),
        ('ID4',  2, None),
        ('ID5',  3, None),
        ('ID6',  1, None),
        ('ID7',  2, None),
        ('ID8',  2, None),
        ('ID9',  3, None),
        (None,   1, None),
        ('ID1',  1, None),
        ('ID2',  1, None),
        ('ID3',  2, None),
        ('ID4',  2, None),
        ('ID5',  3, None),
        ('ID6',  1, None),
        ('ID7',  2, None),
        ('ID8',  2, None),
        ('ID9',  3, None),
        (None,   1, None),
        ('ID1',  1, None),
        ('ID2',  1, None),
        ('ID3',  2, None),
        ('ID4',  2, None),
        ('ID5',  3, None),
        ('ID6',  1, None),
        ('ID7',  2, None),
        ('ID8',  2, None),
        ('ID9',  3, None),
        (None,   1, None),
    ]
    # fmt: on
)

orders_test_table = TestTable(
    name="Orders",
    create_view=os.getenv("TEST_WITH_VIEWS", False),
    columns=[
        ("id", DataType.TEXT),
        ("customer_id_nok", DataType.TEXT),
        ("customer_id_ok", DataType.TEXT),
        ("customer_country", DataType.TEXT),
        ("customer_zip", DataType.TEXT),
        ("text", DataType.TEXT),
    ],
    values=[
        ("O1", "ID1", "ID1", "BE", "2360", "one"),
        ("O2", "ID99", "ID1", "BE", "2360", "two"),
        ("O3", "ID1", "ID2", "BE", "2000", "three"),
        ("O4", None, "ID1", "BE", None, "four"),
        ("O5", "ID98", "ID4", None, "2360", "five"),
        ("O6", "ID99", "ID1", "UK", "2360", "six"),
        (None, None, "ID3", None, None, "seven"),
    ],
)

raw_customers_test_table = TestTable(
    name="RAWCUSTOMERS",
    create_view=os.getenv("TEST_WITH_VIEWS", False),
    columns=customers_test_table.test_columns,
    values=customers_test_table.values,
)

customers_profiling = TestTable(
    name="CustomersProfiling",
    create_view=os.getenv("TEST_WITH_VIEWS", False),
    columns=[
        ("id", DataType.TEXT),
        ("cst_size", DataType.DECIMAL),
        ("cst_size_txt", DataType.TEXT),
        ("distance", DataType.INTEGER),
        ("pct", DataType.TEXT),
        ("cat", DataType.TEXT),
        ("country", DataType.TEXT),
        ("zip", DataType.TEXT),
        ("email", DataType.TEXT),
        ("date_updated", DataType.DATE),
        ("ts", DataType.TIMESTAMP),
        ("ts_with_tz", DataType.TIMESTAMP_TZ),
    ],
    # fmt: off
    values=[
        ('ID1',  1,    "1",     1,    "- 28,42 %", "HIGH",   'BE', '2360', 'john.doe@example.com',    date(2020, 6, 23), datetime(2020, 6, 23, 0, 0, 10), datetime(2020, 6, 23, 0, 0, 10, tzinfo=utc)),
        ('ID2',  .5,   ".5",    -999, "+22,75 %",  "HIGH",   'BE', '2361', 'JOE.SMOE@EXAMPLE.COM',    date(2020, 6, 23), datetime(2020, 6, 23, 0, 1, 10), datetime(2020, 6, 23, 0, 1, 10, tzinfo=utc)),
        ('ID3',  .5,   ".5",    10,    ".92 %",     "MEDIUM", 'BE', '2362', 'milan.lukáč@example.com', date(2020, 6, 23), datetime(2020, 6, 23, 0, 2, 10), datetime(2020, 6, 23, 0, 2, 10, tzinfo=utc)),
        ('ID4',  .5,   ".5",    10,   "0.26 %",    "LOW",    'BE', '2363', 'john.doe@ĚxamplÉ.com',    date(2020, 6, 23), datetime(2020, 6, 23, 0, 3, 10), datetime(2020, 6, 23, 0, 3, 10, tzinfo=utc)),

        ('ID6',  6.1,    "6",     999,  "18,32%",    None,     'BE', '2365', None,                      date(2020, 6, 23), datetime(2020, 6, 23, 0, 5, 10), datetime(2020, 6, 23, 0, 5, 10, tzinfo=utc)),
        ('ID7',  6.1,    "6",     999,  "error",     None,     'NL', '2360', None,                      date(2020, 6, 24), datetime(2020, 6, 24, 0, 1, 10), datetime(2020, 6, 24, 0, 1, 10, tzinfo=utc)),
        ('ID8',  None, None,    999,  "No value",  None,     'NL', '2361', None,                      date(2020, 6, 24), datetime(2020, 6, 24, 0, 2, 10), datetime(2020, 6, 24, 0, 2, 10, tzinfo=utc)),
        ('ID9',  None, None,    10,  "N/A",       None,     'NL', '2362', None,                      date(2020, 6, 24), datetime(2020, 6, 24, 0, 3, 10), datetime(2020, 6, 24, 0, 3, 10, tzinfo=utc)),
        ('ID10', None, None,    None,  "N/A",       None,     'NL', '2362', None,                      date(2020, 6, 24), datetime(2020, 6, 24, 0, 3, 10), datetime(2020, 6, 24, 0, 3, 10, tzinfo=utc)),
        (None,   None, None,    None, None,        "HIGH",   'BE', '2363', None,                      date(2020, 6, 24), datetime(2020, 6, 24, 0, 4, 10), datetime(2020, 6, 24, 0, 4, 10, tzinfo=utc)),
    ]
    # fmt: on
)
# Special table for edge case identifier, data types etc.
special_table = TestTable(
    name="SpecialTable",
    create_view=os.getenv("TEST_WITH_VIEWS", False),
    columns=[
        ("1", DataType.TEXT),
    ],
    # fmt: off
    values=[
        ("value_1",),
        ("value_2",),
    ],
    quote_names=True
    # fmt: on
)


customers_profiling_capitalized = TestTable(
    name="CustomersProfilingCapitalized",
    create_view=os.getenv("TEST_WITH_VIEWS", False),
    columns=[
        ("ITEMS_SOLD", DataType.INTEGER),
        ("CST_Size", DataType.INTEGER),
    ],
    values=[(1, 2), (2, 1), (0, 1), (2, 1), (6, 1), (6, 1), (3, 2)],
)

dro_categorical_test_table = TestTable(
    name="CategoricalDROTable",
    create_view=os.getenv("TEST_WITH_VIEWS", False),
    columns=[
        ("categorical_value", DataType.INTEGER),
    ],
    values=[
        (1,),
        (2,),
        (0,),
        (2,),
        (6,),
        (6,),
        (3,),
        (1,),
        (2,),
        (0,),
        (2,),
        (6,),
        (6,),
        (3,),
        (1,),
        (2,),
        (0,),
        (2,),
        (6,),
        (6,),
        (3,),
        (3,),
    ],
)

null_test_table = TestTable(
    name="NullTable",
    create_view=os.getenv("TEST_WITH_VIEWS", False),
    columns=[
        ("column_with_null_values", DataType.INTEGER),
    ],
    values=[(None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,), (None,)],
)
