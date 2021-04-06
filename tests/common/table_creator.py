from typing import List

from sodasql.scan.db import sql_update


class DataType:
    VARCHAR = 'VARCHAR(255)'
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DECIMAL = "REAL"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"


class ColumnValues:
    def __init__(self, column_name: str, data_type: str, table_creator):
        self.column_name = column_name
        self.data_type = data_type
        self.table_creator = table_creator

    def insert(self, *values):
        if self.table_creator.column_literals is None:
            self.table_creator.column_literals = []
            for i in range(0, len(values)):
                self.table_creator.column_literals.append([])
        else:
            initial_row_count = len(self.table_creator.column_literals)
            row_count = len(values)
            assert row_count == initial_row_count, f'Expected {initial_row_count} values, ' \
                                                   f'but column {self.column_name} has {row_count}'

        for i in range(0, len(values)):
            value = values[i]
            column_literals = self.table_creator.column_literals[i]
            value_literal = self.table_creator.dialect.literal(value)
            column_literals.append(value_literal)


class TableCreator:

    def __init__(self, warehouse_fixture, table_name: str):
        self.warehouse_fixture = warehouse_fixture
        self.dialect = warehouse_fixture.dialect
        self.quoted_table_name = self.dialect.quote_identifier(table_name)
        self.quoted_table_name_declaration = self.dialect.quote_identifier_declaration(table_name)
        self.columns: List[str] = []
        self.column_literals: List[List[str]] = None

    def column(self, column_name: str, data_type: str) -> ColumnValues:
        quoted_column_name = self.dialect.quote_identifier_declaration(column_name)
        data_type_ddl = self.data_type_ddl(data_type)
        self.columns.append(f'{quoted_column_name} {data_type_ddl}')
        return ColumnValues(column_name, data_type, self)

    def recreate(self):
        self.drop_table()
        column_declarations = ',\n  '.join(self.columns)
        self.create_table(column_declarations)
        rows_list = ['( ' + (', '.join(column_literal)) + ' )' for column_literal in self.column_literals]
        rows_text = ',\n  '.join(rows_list)
        self.insert_rows(rows_text)

    def drop_table(self):
        self.sql_update(
            f"DROP TABLE IF EXISTS {self.quoted_table_name};")

    def create_table(self, column_declarations: str):
        self.sql_update(
            f"CREATE TABLE {self.quoted_table_name_declaration} ( \n"
            f"  {column_declarations} );")

    def insert_rows(self, rows_text: str):
        self.sql_update(
            f"INSERT INTO {self.quoted_table_name} VALUES \n"
            f"  {rows_text};")

    def sql_update(self, sql: str):
        connection = self.warehouse_fixture.warehouse.connection
        sql_update(connection, sql)

    def data_type_ddl(self, data_type: str):
        return data_type
