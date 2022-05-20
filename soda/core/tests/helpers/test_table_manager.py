import logging
import textwrap
from typing import List

from soda.common.lazy import Lazy
from tests.helpers.test_table import TestTable

logger = logging.getLogger(__name__)


class TestTableManager:

    __test__ = False

    def __init__(self, data_source: "DataSource"):
        self.__existing_table_names = Lazy()
        from soda.execution.data_source import DataSource

        self.data_source: DataSource = data_source

    def ensure_test_table(self, test_table: TestTable) -> str:
        """
        Returns a unique test table name with the given table data
        """
        existing_test_table_names = self._get_existing_test_table_names()
        existing_test_table_names_lower = [table_name.lower() for table_name in existing_test_table_names]
        if test_table.unique_table_name.lower() not in existing_test_table_names_lower:
            obsolete_table_names = [
                existing_test_table
                for existing_test_table in existing_test_table_names
                if existing_test_table.lower().startswith(f"sodatest_{test_table.name.lower()}_")
            ]
            if obsolete_table_names:
                for obsolete_table_name in obsolete_table_names:
                    self._drop_test_table(obsolete_table_name)
            self._create_and_insert_test_table(test_table)
            self.data_source.commit()

            # Run analyze table so that metadata works if applicable.
            self.data_source.analyze_table(test_table.unique_table_name)
        return test_table.unique_table_name

    def _get_existing_test_table_names(self):
        if not self.__existing_table_names.is_set():
            # the filter is applied case insensitive by converting to lower case
            self.__existing_table_names.set(self.data_source.get_table_names(filter="sodatest_%"))
        return self.__existing_table_names.get()

    def _drop_test_table(self, obsolete_table_name):
        create_table_sql = self._drop_test_table_sql(obsolete_table_name)
        self.update(create_table_sql)
        self._get_existing_test_table_names().remove(obsolete_table_name)

    def _drop_test_table_sql(self, table_name: str) -> str:
        quoted_table_name = self.data_source.quote_table_declaration(table_name)
        return f"DROP TABLE {quoted_table_name};"

    def _create_and_insert_test_table(self, test_table):
        create_table_sql = self._create_test_table_sql(test_table)
        self.update(create_table_sql)
        self._get_existing_test_table_names().append(test_table.unique_table_name)
        create_table_sql = self._insert_test_table_sql(test_table)
        if create_table_sql:
            self.update(create_table_sql)

    def _create_test_table_sql(self, test_table: TestTable) -> str:
        quoted_table_name = (
            self.data_source.quote_table_declaration(test_table.unique_table_name)
            if test_table.quote_names
            else test_table.unique_table_name
        )
        fully_qualified_table_name = self.data_source.prefix_table(quoted_table_name)
        columns = test_table.columns
        if test_table.quote_names:
            columns = [
                (
                    self.data_source.quote_column_declaration(column[0]),
                    column[1],
                )
                for column in columns
            ]
        columns_sql = ",\n".join(
            [f"  {column[0]} {self.data_source.get_sql_type_for_create_table(column[1])}" for column in columns]
        )
        return f"CREATE TABLE {fully_qualified_table_name} ( \n" f"{columns_sql}\n" f");"

    def _insert_test_table_sql(self, test_table: TestTable) -> str:
        if test_table.values:
            quoted_table_name = (
                self.data_source.quote_table(test_table.unique_table_name)
                if test_table.quote_names
                else test_table.unique_table_name
            )
            fully_qualified_table_name = self.data_source.prefix_table(quoted_table_name)

            def sql_test_table_row(row):
                return ",".join(self.data_source.literal(value) for value in row)

            rows_sql = ",\n".join([f"  ({sql_test_table_row(row)})" for row in test_table.values])
            return f"INSERT INTO {fully_qualified_table_name} VALUES \n" f"{rows_sql};"

    def fetch_all(self, sql: str) -> List[tuple]:
        cursor = self.data_source.connection.cursor()
        try:
            sql_indented = textwrap.indent(text=sql, prefix="  #   ")
            logger.debug(f"  # Test data handler fetchall: \n{sql_indented}")
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()

    def update(self, sql: str) -> object:
        cursor = self.data_source.connection.cursor()
        try:
            sql_indented = textwrap.indent(text=sql, prefix="  #   ")
            logger.debug(f"  # Test data handler update: \n{sql_indented}")
            updates = cursor.execute(sql)
            return updates
        finally:
            cursor.close()
