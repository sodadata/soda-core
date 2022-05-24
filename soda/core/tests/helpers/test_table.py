from __future__ import annotations

import hashlib

from soda.common.json_helper import JsonHelper
from tests.helpers.test_column import TestColumn


class TestTable:
    """
    Test infrastructure that includes all data for a test table: name, schema and row values.
    This is part of the main codebase because we want data_source implementation to start from a single
    class where methods must be implemented and can be customized.
    """

    __test__ = False
    __names = []

    def __init__(
        self,
        name: str,
        columns: list[tuple[str, str]] | list[TestColumn],
        values: list[tuple] = None,
        quote_names: bool = False,
    ):
        """
        name: logical name
        columns: tuples with column name and DataType's
        values: list of row value tuples
        """
        # Ensure unique table data names
        if name in TestTable.__names:
            raise AssertionError(f"Duplicate TableData name: {name}")
        TestTable.__names.append(name)

        self.name: str = name
        if len(columns) == 0 or isinstance(columns[0], TestColumn):
            self.test_columns: list[TestColumn] = columns
        else:
            self.test_columns: list[TestColumn] = [TestColumn(column[0], column[1]) for column in columns]

        self.values: list[tuple] = values
        self.quote_names: bool = quote_names
        self.unique_table_name = f"SODATEST_{name}_{self.__test_table_hash()}"
        # self.actual_name is the actual name in the data source and it is initialized in the TestTableManager.ensure_test_table
        self.actual_name = None

    def __test_table_hash(self):
        json_text = JsonHelper.to_json(
            [
                self.name,
                [test_column.to_hashable_json() for test_column in self.test_columns],
                list(self.values) if self.values else None,
            ]
        )
        hexdigest = hashlib.md5(json_text.encode()).hexdigest()
        return hexdigest[-8:]
