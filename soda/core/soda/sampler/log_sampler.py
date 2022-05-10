import logging
from typing import Optional, Tuple

from soda.common.logs import Logs
from soda.execution.query import Query
from soda.sampler.sampler import Sampler
from soda.sampler.storage_ref import StorageRef

logger = logging.getLogger(__name__)


class LogSampler(Sampler):

    def store_sample(self, cursor, sample_name: str, query: str, logs: Logs) -> Optional[StorageRef]:
        table_text, column_count, row_count = self.pretty_print(cursor, logs)
        logs.info(f"Sample {sample_name}:\n{table_text}")
        return StorageRef(
            provider="console",
            column_count=column_count,
            total_row_count=row_count,
            stored_row_count=row_count,
            reference=f'Search in the console for "Sample {sample_name}"',
        )

    def pretty_print(self, cursor, logs: Logs, max_column_length: int = 25) -> Tuple[str, int, int]:
        def stringify(value, quote_strings):
            if isinstance(value, str):
                return f"'{value}'" if quote_strings else value
            if value is None:
                return ""
            return str(value)

        def maxify(value):
            return value if len(value) <= max_column_length else f"{value[:max_column_length - 3]}..."

        def serialize_row(row, quote_strings: bool = False):
            return [maxify(stringify(value, quote_strings)) for value in row]

        names = []
        lengths = []
        rules = []
        rows = [serialize_row(row, quote_strings=True) for row in cursor.fetchall()]
        column_names = serialize_row([column_description[0] for column_description in cursor.description])
        for column_name in column_names:
            names.append(column_name)
            lengths.append(len(column_name))
        for column_index in range(len(lengths)):
            rls = [len(row[column_index]) for row in rows if row[column_index]]
            lengths[column_index] = max([lengths[column_index]] + rls)
            rules.append("-" * lengths[column_index])
        format = " ".join(["%%-%ss" % l for l in lengths])
        result = [format % tuple(names)]
        result.append(format % tuple(rules))
        for row in rows:
            result.append(format % tuple(row))
        return "\n".join(result), len(names), len(rows)
