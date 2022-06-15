import logging
import os
import re
import textwrap
from typing import List

from soda.common.lazy import Lazy
from tests.helpers.test_column import TestColumn
from tests.helpers.test_table import TestTable

logger = logging.getLogger(__name__)


class TestTableManager:

    __test__ = False

    def __init__(self, data_source: "DataSource"):
        self.__existing_table_names = Lazy()
        from soda.execution.data_source import DataSource

        self.data_source: DataSource = data_source
        self.schema_name: str = self._create_schema_name()
        self.drop_schema_enabled = os.getenv("GITHUB_ACTIONS") or self.data_source.type != 'postgres'
        self._initialize_schema()

    def _create_schema_name(self):
        schema_name_parts = [
            "sodatest",
            os.getenv("GITHUB_HEAD_REF", "local_dev")
        ]
        schema_name_raw = "_".join(schema_name_parts)
        schema_name = re.sub("[^0-9a-zA-Z]+", "_", schema_name_raw)
        schema_name = self.data_source.default_casify_table_name(schema_name)
        return schema_name

    def _initialize_schema(self):
        if self.drop_schema_enabled:
            self.drop_schema_if_exists()
        self._create_schema_if_exists()

    def _create_schema_if_exists(self):
        create_schema_if_exists_sql = self._create_schema_if_exists_sql()
        self.update(create_schema_if_exists_sql)
        self.data_source.schema = self.schema_name
        self.data_source.table_prefix = f"{self.schema_name}"

    def _create_schema_if_exists_sql(self):
        return f"CREATE SCHEMA IF NOT EXISTS {self.schema_name} AUTHORIZATION CURRENT_USER"

    def drop_schema_if_exists(self):
        if self.drop_schema_enabled:
            drop_schema_if_exists_sql = self._drop_schema_if_exists_sql()
            self.update(drop_schema_if_exists_sql)

    def _drop_schema_if_exists_sql(self):
        return f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE"

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

            # TODO investigate if this is really needed
            self.data_source.analyze_table(test_table.unique_table_name)

        return test_table.unique_table_name

    def _get_existing_test_table_names(self):
        if not self.__existing_table_names.is_set():
            sql = self.data_source.sql_find_table_names(filter="sodatest_%")
            rows = self.fetch_all(sql)
            table_names = [row[0] for row in rows]
            self.__existing_table_names.set(table_names)
        return self.__existing_table_names.get()

    def _create_and_insert_test_table(self, test_table):
        create_table_sql = self._create_test_table_sql(test_table)
        self.update(create_table_sql)
        self._get_existing_test_table_names().append(test_table.unique_table_name)
        insert_table_sql = self._insert_test_table_sql(test_table)
        if insert_table_sql:
            self.update(insert_table_sql)

    def _create_test_table_sql(self, test_table: TestTable) -> str:
        table_name = test_table.unique_table_name
        if test_table.quote_names:
            table_name = self.data_source.quote_table_declaration(table_name)
        qualified_table_name = self.data_source.qualified_table_name(table_name)
        test_columns = test_table.test_columns
        if test_table.quote_names:
            test_columns = [
                TestColumn(
                    name=self.data_source.quote_column_declaration(test_column.name), data_type=test_column.data_type
                )
                for test_column in test_columns
            ]
        columns_sql = ",\n".join(
            [
                f"  {test_column.name} {self.data_source.get_sql_type_for_create_table(test_column.data_type)}"
                for test_column in test_columns
            ]
        )

        sql = f"CREATE TABLE {qualified_table_name} ( \n{columns_sql} \n)"

        # TODO: a bit of a hack, but there is no need to build anything around data source for inserting for now.
        if self.data_source.type == "athena":
            sql += f"LOCATION '{self.data_source.athena_staging_dir}/data/{qualified_table_name}/' "

        return sql

    def _insert_test_table_sql(self, test_table: TestTable) -> str:
        if test_table.values:
            quoted_table_name = (
                self.data_source.quote_table(test_table.unique_table_name)
                if test_table.quote_names
                else test_table.unique_table_name
            )
            qualified_table_name = self.data_source.qualified_table_name(quoted_table_name)

            def sql_test_table_row(row):
                return ",".join(self.data_source.literal(value) for value in row)

            rows_sql = ",\n".join([f"  ({sql_test_table_row(row)})" for row in test_table.values])
            return f"INSERT INTO {qualified_table_name} VALUES \n" f"{rows_sql};"

    def _drop_test_table(self, table_name):
        qualified_table_name = self.data_source.qualified_table_name(table_name)
        self.update(f"DROP TABLE {qualified_table_name} IF EXISTS;")
        self._get_existing_test_table_names().remove(table_name)

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
            self.data_source.commit()
            return updates
        finally:
            cursor.close()
