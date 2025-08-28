from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from soda_core.common.consistent_hash_builder import ConsistentHashBuilder
from soda_core.common.metadata_types import (
    ColumnMetadata,
    SodaDataTypeName,
    SqlDataType,
)


class TestTableSpecificationBuilder:
    """
    See TestTableSpecification for documentation
    """

    __test__ = False

    # All test table names to verify that every test table name is used only once in the whole test suite.
    __names = []

    def __init__(self):
        self._table_purpose: Optional[str] = None
        self._columns: list[ColumnMetadata] = []
        self._rows: Optional[list[tuple]] = None

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

    def column(
        self,
        name: str,
        soda_data_type_name: SodaDataTypeName,
        character_maximum_length: Optional[int] = None,
        numeric_precision: Optional[int] = None,
        numeric_scale: Optional[int] = None,
        datetime_precision: Optional[int] = None,
    ) -> TestTableSpecificationBuilder:
        self._columns.append(
            ColumnMetadata(
                column_name=name,
                sql_data_type=SqlDataType(
                    name=soda_data_type_name,
                    character_maximum_length=character_maximum_length,
                    numeric_precision=numeric_precision,
                    numeric_scale=numeric_scale,
                    datetime_precision=datetime_precision,
                ),
            )
        )
        return self

    def column_varchar(
        self,
        name: str,
        character_maximum_length: Optional[int] = None,
    ) -> TestTableSpecificationBuilder:
        return self.column(
            name=name,
            soda_data_type_name=SodaDataTypeName.VARCHAR,
            character_maximum_length=character_maximum_length,
        )

    def column_integer(self, name) -> TestTableSpecificationBuilder:
        return self.column(name=name, soda_data_type_name=SodaDataTypeName.INTEGER)

    def column_numeric(
        self,
        name: str,
        numeric_precision: Optional[int] = None,
        numeric_scale: Optional[int] = None,
    ) -> TestTableSpecificationBuilder:
        return self.column(
            name=name,
            soda_data_type_name=SodaDataTypeName.NUMERIC,
            numeric_precision=numeric_precision,
            numeric_scale=numeric_scale,
        )

    def column_decimal(
        self,
        name: str,
        numeric_precision: Optional[int] = None,
        numeric_scale: Optional[int] = None,
    ) -> TestTableSpecificationBuilder:
        return self.column(
            name=name,
            soda_data_type_name=SodaDataTypeName.DECIMAL,
            numeric_precision=numeric_precision,
            numeric_scale=numeric_scale,
        )

    def column_date(self, name) -> TestTableSpecificationBuilder:
        return self.column(name=name, soda_data_type_name=SodaDataTypeName.DATE)

    def column_time(self, name) -> TestTableSpecificationBuilder:
        return self.column(name=name, soda_data_type_name=SodaDataTypeName.TIME)

    def column_timestamp(
        self,
        name: str,
        datetime_precision: Optional[int] = None,
    ) -> TestTableSpecificationBuilder:
        return self.column(
            name=name,
            soda_data_type_name=SodaDataTypeName.TIMESTAMP,
            datetime_precision=datetime_precision,
        )

    def column_timestamp_tz(
        self,
        name: str,
        datetime_precision: Optional[int] = None,
    ) -> TestTableSpecificationBuilder:
        return self.column(
            name=name,
            soda_data_type_name=SodaDataTypeName.TIMESTAMP_TZ,
            datetime_precision=datetime_precision,
        )

    def column_boolean(self, name) -> TestTableSpecificationBuilder:
        return self.column(name=name, soda_data_type_name=SodaDataTypeName.BOOLEAN)

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
                f"Duplicate test table purpose detected: {self._table_purpose}.  In the codebase, the table_purpose "
                f'of every test table should be unique.  Search for .table_purpose("{self._table_purpose}") and you '
                f"should find multiple places in the test codebase where the same table_purpose is created."
            )
        self.__names.append(name_lower)
        unique_name = f"SODATEST_{self._table_purpose}_{self.__test_table_hash()}"
        return TestTableSpecification(
            name=self._table_purpose, columns=self._columns, row_values=self._rows, unique_name=unique_name
        )

    def __test_table_hash(self) -> str:
        consistent_hash_builder: ConsistentHashBuilder = ConsistentHashBuilder()
        consistent_hash_builder.add(self._table_purpose)
        consistent_hash_builder.add("columns")
        for test_column in self._columns:
            consistent_hash_builder.add(test_column.column_name)
            consistent_hash_builder.add(test_column.sql_data_type.name)
            if test_column.sql_data_type:
                consistent_hash_builder.add(test_column.sql_data_type.character_maximum_length)
                consistent_hash_builder.add(test_column.sql_data_type.numeric_precision)
                consistent_hash_builder.add(test_column.sql_data_type.numeric_scale)
                consistent_hash_builder.add(test_column.sql_data_type.datetime_precision)
        if isinstance(self._rows, Iterable):
            for row in self._rows:
                consistent_hash_builder.add("row")
                if isinstance(row, Iterable):
                    for value in row:
                        consistent_hash_builder.add(value)
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

    name: Optional[str]
    columns: list[ColumnMetadata]
    row_values: Optional[list[tuple]]
    unique_name: Optional[str]


class TestTable:
    __test__ = False

    def __init__(
        self,
        data_source_name: str,
        dataset_prefix: list[str],
        code_name: str,
        unique_name: str,
        qualified_name: str,
        columns: list[TestColumn],
        row_values: Optional[list[tuple]],
    ):
        self.data_source_name: str = data_source_name
        self.dataset_prefix: list[str] = dataset_prefix
        # Name of the test table in the code.
        self.code_name: str = code_name
        # Name that includes the hash of the contents of the table
        self.unique_name: str = unique_name
        # Fully qualified SQL name of the table
        self.qualified_name: str = qualified_name
        self.columns: dict[str, TestColumn] = {column.name: column for column in columns}
        self.row_values: Optional[list[tuple]] = row_values

    def data_type(self, column_name: str) -> str:
        return self.columns[column_name].sql_data_type.name

    def get_dataset_qualified_name(self) -> str:
        slash_separated_prefixes: str = "/".join(self.dataset_prefix)
        return f"{self.data_source_name}/{slash_separated_prefixes}/{self.unique_name}"


class TestColumn:
    __test__ = False

    def __init__(self, name: str, sql_data_type: SqlDataType):
        self.name: str = name
        self.sql_data_type: SqlDataType = sql_data_type
