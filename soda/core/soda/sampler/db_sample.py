from typing import Tuple

from soda.common.memory_safe_cursor_fetcher import MemorySafeCursorFetcher
from soda.sampler.sample import Sample
from soda.sampler.sample_schema import SampleColumn, SampleSchema


class DbSample(Sample):
    def __init__(self, cursor, data_source, limit=None):
        self.cursor = cursor
        self.safe_fetcher = MemorySafeCursorFetcher(cursor)
        self.data_source = data_source
        self.rows = None
        self._limit = limit

    def get_rows(self) -> Tuple[Tuple]:
        return self.safe_fetcher.get_rows()

    def get_rows_count(self) -> int:
        return self.safe_fetcher.get_row_count()

    def get_schema(self) -> SampleSchema:
        return self._convert_python_db_schema_to_sample_schema(self.cursor.description)

    def _convert_python_db_schema_to_sample_schema(self, dbapi_description) -> SampleSchema:
        columns = SampleColumn.create_sample_columns(dbapi_description, self.data_source)
        return SampleSchema(columns=columns)
