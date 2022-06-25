import logging

from soda.common.table_printer import TablePrinter
from soda.sampler.sample_context import SampleContext
from soda.sampler.sample_ref import SampleRef
from soda.sampler.sampler import Sampler

logger = logging.getLogger(__name__)


class LogSampler(Sampler):
    def store_sample(self, sample_context: SampleContext) -> SampleRef:
        rows = sample_context.sample.get_rows()
        columns = sample_context.sample.get_schema().columns
        column_names = [column.name for column in columns]
        table_text = TablePrinter(rows=rows, column_names=column_names).get_table()
        row_count = len(rows)
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
