#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import uuid
from collections import OrderedDict
from datetime import date, timedelta
from typing import List, Optional

from faker import Faker

from sodasql.dialects.postgres_dialect import PostgresDialect
from sodasql.scan.dialect import Dialect, KEY_WAREHOUSE_TYPE
from sodasql.scan.dialect_parser import DialectParser
from tests.common.sql_test_case import TARGET_SNOWFLAKE, TARGET_POSTGRES, TARGET_REDSHIFT, TARGET_ATHENA, \
    TARGET_BIGQUERY
from tests.common.warehouse_fixture import WarehouseFixture

faker = Faker()
Faker.seed(faker.random_int())


class Column:
    def __init__(self):
        class_name = str(type(self))
        self.name = class_name[class_name.rfind('.')+1:-2].upper()
        self.type = None
        self.dialect: Optional[Dialect] = None

    def declaration(self) -> str:
        return f'{self.name} {self.get_data_type()}'

    def value(self, date_index, row_index, date):
        pass

    def get_data_type(self):
        pass


class Id(Column):
    def get_data_type(self):
        return dialect.data_type_varchar_255

    def value(self, date_index, row_index, date):
        return f"'{str(uuid.uuid1())}'"


class Name(Column):
    def get_data_type(self):
        return dialect.data_type_varchar_255

    def value(self, date_index, row_index, date):
        return f"'{faker.name()}'"


class Size(Column):
    def get_data_type(self):
        return dialect.data_type_integer

    def value(self, date_index, row_index, date):
        return str(faker.random_int(min=1000, max=9999, step=1))


class Date(Column):
    def get_data_type(self):
        return dialect.data_type_date

    def value(self, date_index, row_index, date):
        return self.dialect.literal_date(date)


class FeePct(Column):
    def get_data_type(self):
        return dialect.data_type_varchar_255

    def value(self, date_index, row_index, date):
        min = int(10 + date_index * 1)
        max = int(30 + date_index * 2)
        return f"'{faker.random_int(min=min, max=max, step=1)},{faker.random_digit()}{faker.random_digit()} %'"


class Country(Column):
    elements = OrderedDict([
        ("US", 0.45),
        ("UK", 0.35),
        ("Spain", 0.15),
        ("Netherlands", 0.05) ])
    elements_without_uk = OrderedDict([
        ("US", 0.45),
        ("Spain", 0.15),
        ("Netherlands", 0.05) ])

    def get_data_type(self):
        return dialect.data_type_varchar_255

    def value(self, date_index, row_index, date):
        elements = Country.elements if date_index != 2 else Country.elements_without_uk
        return f"'{faker.random_element(elements=elements)}'"


class Generator:

    def __init__(self, columns: List[Column], days: int = 7):
        self.days = days
        self.columns = columns
        self.date = date.today() - timedelta(days=self.days-1)
        self.dialect = None
        self.table_name = 'DEMODATA'

    def generate(self, dialect: Dialect):
        self.dialect = dialect
        for column in self.columns:
            column.dialect = dialect
        self.create_table()
        self.insert_rows()

    def create_table(self):
        column_declarations = map(lambda x: x.declaration(), self.columns)
        self.print_sql(self.dialect.sql_drop_table(self.table_name))
        self.print_sql(self.dialect.sql_create_table(self.table_name, column_declarations))

    def insert_rows(self):
        for date_index in range(0, self.days):
            row_count = self.get_row_count(date_index)
            rows = []
            for row_index in range(0, row_count):
                row_values = []
                for column in self.columns:
                    row_value = column.value(date_index, row_index, self.date)
                    row_values.append(row_value)
                rows.append('( ' + (', '.join(row_values)) + ' )')

            self.print_sql(self.dialect.sql_insert_into(self.table_name, rows))
            self.date = self.date + timedelta(days=1)

    def print_sql(self, sql):
        print(f'{sql};\n')

    def get_row_count(self, date_index: int) -> int:
        return 5 + date_index + faker.random_int(min=0, max=2)


def create_dialect(target):
    return Dialect.create(DialectParser({KEY_WAREHOUSE_TYPE: target}))


if __name__ == "__main__":
    generator = Generator(columns=[
            Id(),
            Name(),
            Size(),
            Date(),
            FeePct(),
            Country()])

    targets = [TARGET_POSTGRES] # TARGET_ATHENA, TARGET_SNOWFLAKE, TARGET_REDSHIFT, TARGET_ATHENA, TARGET_BIGQUERY]

    for target in targets:
        dialect: Dialect = create_dialect(target)
        generator.generate(dialect)
