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
from typing import List

from faker import Faker

faker = Faker()
Faker.seed(faker.random_int())


class Column:
    def __init__(self, sql_type: str):
        class_name = str(type(self))
        self.name = class_name[class_name.rfind('.')+1:-2].upper()
        self.type = sql_type

    def declaration(self) -> str:
        return f'{self.name} {self.type}'

    def value(self, date_index, row_index, date):
        pass


class Id(Column):
    def __init__(self):
        super().__init__('VARCHAR(255)')

    def value(self, date_index, row_index, date):
        return f"'{str(uuid.uuid1())}'"

class Name(Column):
    def __init__(self):
        super().__init__('VARCHAR(255)')

    def value(self, date_index, row_index, date):
        return f"'{faker.name()}'"


class Size(Column):
    def __init__(self):
        super().__init__('INT')

    def value(self, date_index, row_index, date):
        return str(faker.random_int(min=1000, max=9999, step=1))


class Date(Column):
    def __init__(self):
        super().__init__('DATE')

    def value(self, date_index, row_index, date):
        return ("DATE '" +
                date.strftime("%Y-%m-%d") +
                "'")


class FeePct(Column):
    def __init__(self):
        super().__init__('VARCHAR(255)')

    def value(self, date_index, row_index, date):
        min = int(10 + date_index * 1)
        max = int(30 + date_index * 2)
        return f"'{faker.random_int(min=min, max=max, step=1)},{faker.random_digit()}{faker.random_digit()} %'"


class Country(Column):
    elements = OrderedDict([
        ("US", 0.45),
        ("UK", 0.35),
        ("Spain", 0.15),
        ("Netherlands", 0.05), ])
    elements_without_uk = OrderedDict([
        ("US", 0.45),
        ("Spain", 0.15),
        ("Netherlands", 0.05), ])

    def __init__(self):
        super().__init__('VARCHAR(255)')

    def value(self, date_index, row_index, date):
        elements = Country.elements if date_index != 2 else Country.elements_without_uk
        return f"'{faker.random_element(elements=elements)}'"


class Generator:

    def __init__(self, columns: List[Column], days: int = 7):
        self.days = days
        self.columns = columns
        self.date = date.today() - timedelta(days=self.days-1)

    def generate(self):
        self.create_table()
        self.insert_rows()

    def create_table(self):
        column_declarations = map(lambda x : x.declaration(), self.columns)
        self.execute(f'DROP TABLE IF EXISTS DEMODATA CASCADE')
        self.execute(f'CREATE TABLE DEMODATA (\n  ' +
                     (',\n  '.join(column_declarations)) +
                     f'\n)')

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

            self.execute('INSERT INTO DEMODATA VALUES \n  ' +
                         (',\n  '.join(rows)))
            self.date = self.date + timedelta(days=1)

    def execute(self, sql):
        print(f'{sql};\n')

    def get_row_count(self, date_index: int) -> int:
        return 5 + date_index + faker.random_int(min=0, max=2)


if __name__ == "__main__":
    generator = Generator(columns=[
            Id(),
            Name(),
            Size(),
            Date(),
            FeePct(),
            Country()])
    generator.generate()
