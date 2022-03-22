from __future__ import annotations

import hashlib

from soda.common.json_helper import JsonHelper
from soda.execution.data_type import DataType


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
        columns: list[tuple[str, DataType]],
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
        self.columns: list[tuple[str, DataType]] = columns
        self.values: list[tuple] = values
        self.quote_names: bool = quote_names
        self.unique_table_name = f"SODATEST_{name}_{self.__test_table_hash()}"

    def __test_table_hash(self):
        json_text = JsonHelper.to_json(
            [
                self.name,
                list(self.columns),
                list(self.values) if self.values else None,
            ]
        )
        hexdigest = hashlib.md5(json_text.encode()).hexdigest()
        return hexdigest[-8:]
