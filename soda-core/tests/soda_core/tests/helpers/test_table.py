from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from soda_core.tests.helpers.consistent_hash_builder import ConsistentHashBuilder


class TestDataType:
    """
    TestDataTypes contains data source-neutral constants for referring to the basic, common column data types.
    """
    __test__ = False

    TEXT = "text"
    INTEGER = "integer"
    DECIMAL = "decimal"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMP_TZ = "timestamptz"
    BOOLEAN = "boolean"


class TestColumnSpecification:
    __test__ = False

    def __init__(self, name: str, test_data_type: str):
        self.name: str = name
        self.test_data_type: str = test_data_type


class TestTableSpecificationBuilder:
    """
    See TestTableSpecification for documentation
    """
    __test__ = False

    # All test table names to verify that every test table name is used only once in the whole test suite.
    __names = []

    def __init__(self):
        self._table_purpose: str | None = None
        self._columns: list[TestColumnSpecification] = []
        self._rows: list[tuple] | None = None

    def table_purpose(self, table_purpose: str) -> TestTableSpecificationBuilder:
        """
        The middle section of the table name indicating the purpose.
        The full table name will be composed of
          * The "SODATEST_" prefix
          * The test table name
          * A hash of the table specification (for recreating tables only when the schema or data changes)
        """
        self._table_purpose = table_purpose
        return self

    def column(self, name: str, test_data_type: str) -> TestTableSpecificationBuilder:
        self._columns.append(TestColumnSpecification(name=name, test_data_type=test_data_type))
        return self

    def column_text(self, name) -> TestTableSpecificationBuilder:
        return self.column(name=name, test_data_type=TestDataType.TEXT)

    def column_integer(self, name) -> TestTableSpecificationBuilder:
        return self.column(name=name, test_data_type=TestDataType.INTEGER)

    def column_decimal(self, name) -> TestTableSpecificationBuilder:
        return self.column(name=name, test_data_type=TestDataType.DECIMAL)

    def column_date(self, name) -> TestTableSpecificationBuilder:
        return self.column(name=name, test_data_type=TestDataType.DATE)

    def column_time(self, name) -> TestTableSpecificationBuilder:
        return self.column(name=name, test_data_type=TestDataType.TIME)

    def column_timestamp(self, name) -> TestTableSpecificationBuilder:
        return self.column(name=name, test_data_type=TestDataType.TIMESTAMP)

    def column_timestamp_tz(self, name) -> TestTableSpecificationBuilder:
        return self.column(name=name, test_data_type=TestDataType.TIMESTAMP_TZ)

    def column_boolean(self, name) -> TestTableSpecificationBuilder:
        return self.column(name=name, test_data_type=TestDataType.BOOLEAN)

    def rows(self, rows: list[tuple]) -> TestTableSpecificationBuilder:
        """
        Python objects that will be translated to data source specific literals
        Usage:
            .rows([
                ('1', 'US'),
                ('2', 'US'),
                ('3', 'BE')
            ])
        """
        self._rows = rows
        return self

    def build(self) -> TestTableSpecification:
        assert isinstance(self._table_purpose, str)
        # Ensure unique table data names
        name_lower = self._table_purpose.lower()
        if name_lower in self.__names:
            raise AssertionError(
                f"Duplicate test table purpose detected: {self.table_purpose}.  In the codebase, the table_purpose "
                f"of every test table should be unique.  Search for \"{self.table_purpose}\" and you should find "
                f"multiple places in the test codebase where the same table_purpose is created."
            )
        self.__names.append(name_lower)
        unique_name = f"SODATEST_{self._table_purpose}_{self.__test_table_hash()}"
        return TestTableSpecification(
            name=self._table_purpose,
            columns=self._columns,
            row_values=self._rows,
            unique_name=unique_name
        )

    def __test_table_hash(self) -> str:
        consistent_hash_builder: ConsistentHashBuilder = ConsistentHashBuilder()
        consistent_hash_builder.add(self._table_purpose)
        consistent_hash_builder.add("columns")
        for test_column in self._columns:
            consistent_hash_builder.add(test_column.name)
            consistent_hash_builder.add(test_column.test_data_type)
        if isinstance(self._rows, Iterable):
            for row in self._rows:
                consistent_hash_builder.add("row")
                if isinstance(row, Iterable):
                    for value in row:
                        consistent_hash_builder.add(value)
        # TODO find out what this is for
        # os.getenv("TEST_TABLE_SEED", None),
        return consistent_hash_builder.get_hash()


@dataclass
class TestTableSpecification:
    """
    Data model for specifying a test tables that are lazy created and initialized in the data source before they can
    be used in tests. It includes all information from which a concrete test table can be created, including the
    records.

    To minimize runtime overhead the test infrastructure recycles tables as long as the test table specification
    stay the same in the codebase.  To accomplish this test table names are composed of 3 parts:
          * The "SODATEST_" prefix
          * The table purpose
          * A hash of the table specification

    If a test table doesn't exist, it is created as part of the ensure_test_table method.  Tables remain
    in the data source after the tests.

    If the data or schema changes in the code of a test table specification, then the hash of the table specification
    will change.  The new table SODATEST_{table_purpose}_{new_table_hash} will be created and the
    old table SODATEST_{table_purpose}_{old_table_hash} will be deleted.

    Multiple tests can leverage the same test table specification.  The convention is that all the tests in one
    test file use 1 test table specification.  That way, reusing the same table for multiple tests is still supported
    and the impact of changing a test table specification is limited to the file you're working in.
    """
    __test__ = False

    @staticmethod
    def builder() -> TestTableSpecificationBuilder:
        return TestTableSpecificationBuilder()

    name: str | None
    columns: list[TestColumnSpecification]
    row_values: list[tuple] | None
    unique_name: str | None


class TestTable:
    __test__ = False

    def __init__(self,
                 data_source_name: str,
                 database_name: str | None,
                 schema_name: str | None,
                 name: str,
                 unique_name: str,
                 qualified_name: str,
                 columns: list[TestColumn],
                 row_values: list[tuple] | None
                 ):
        self.data_source_name: str = data_source_name
        self.database_name: str = database_name
        self.schema_name: str = schema_name
        self.name: str = name
        self.unique_name: str = unique_name
        self.qualified_name: str = qualified_name
        self.columns: dict[str, TestColumn] = {
            column.name: column for column in columns
        }
        self.row_values: list[tuple] | None = row_values

    def data_type(self, column_name: str) -> str:
        return self.columns[column_name].data_type


class TestColumn:
    __test__ = False

    def __init__(self,
                 name: str,
                 test_data_type: str,
                 data_type: str
                 ):
        self.name: str = name

        # The test_data_type is the abstract data type as specified in the test code (data source neutral)
        self.test_data_type: str = test_data_type

        # The data_type is data source specific data type
        self.data_type: str = data_type
