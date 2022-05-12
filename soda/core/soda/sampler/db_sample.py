from typing import Tuple

from soda.sampler.sample import Sample
from soda.sampler.sample_schema import SampleColumn, SampleSchema


class DbSample(Sample):
    def __init__(self, cursor, data_source):
        self.cursor = cursor
        self.data_source = data_source

    def get_rows(self) -> Tuple[Tuple]:
        return self.cursor.fetchall()

    def get_schema(self) -> SampleSchema:
        return self._convert_python_db_schema_to_sample_schema(self.cursor.description)

    def _convert_python_db_schema_to_sample_schema(self, description) -> SampleSchema:
        return SampleSchema(
            columns=[
                self._convert_python_db_column_to_sample_column(python_db_column) for python_db_column in description
            ]
        )

    def _convert_python_db_column_to_sample_column(self, python_db_column):
        type_code = python_db_column[1]
        type_name = self.data_source.get_type_name(type_code)
        return SampleColumn(name=python_db_column[0], type=type_name)
