import logging
from typing import Tuple

from soda.sampler.sample_context import SampleContext
from soda.sampler.sample_ref import SampleRef
from soda.sampler.sampler import Sampler

logger = logging.getLogger(__name__)


class LogSampler(Sampler):
    def store_sample(self, sample_context: SampleContext) -> SampleRef:
        table_text, column_count, row_count = self.pretty_print(sample_context)
        sample_name = sample_context.sample_name
        sample_context.logs.info(f"Sample {sample_name}:\n{table_text}")
        return SampleRef(
            name=sample_name,
            schema=sample_context.sample.get_schema(),
            total_row_count=row_count,
            stored_row_count=row_count,
            type="log",
            message=f'Search in the console for "Sample {sample_name}"',
        )

    def pretty_print(self, sample_context: SampleContext, max_column_length: int = 25) -> Tuple[str, int, int]:
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

        rows = sample_context.sample.get_rows()
        rows = [serialize_row(row, quote_strings=True) for row in rows]

        sample_columns = sample_context.sample.get_schema().columns
        column_names = serialize_row([sample_column.name for sample_column in sample_columns])
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
