from datetime import date, timezone

from helpers.test_table import TestTable
from soda.execution.data_type import DataType

utc = timezone.utc

contracts_test_table = TestTable(
    name="contracts",
    columns=[
        ("id", DataType.TEXT),
        ("size", DataType.DECIMAL),
        ("distance", DataType.INTEGER),
        ("created", DataType.DATE),
    ],
    # fmt: off
    values=[
        ('ID1',  1,    0,       date(2020, 6, 23)),
        ('N/A',  1,    None,    date(2020, 6, 23)),
        (None,   1,    None,    date(2020, 6, 23)),
    ]
    # fmt: on
)
