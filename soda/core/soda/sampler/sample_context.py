from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from soda.common.logs import Logs
from soda.sampler.sample import Sample


@dataclass
class SampleContext:

    sample: Sample
    sample_name: str
    query: str
    data_source: "DataSource"
    partition: Optional["Partition"]
    column: Optional["Column"]
    scan: "Scan"
    logs: "Logs"

    def get_scan_folder_name(self):
        parts = [
            self.scan._scan_definition_name,
            self.scan._data_timestamp.strftime("%Y%m%d%H%M%S"),
            datetime.now().strftime("%Y%m%d%H%M%S"),
        ]
        return "_".join([part for part in parts if part])

    def get_sample_file_name(self):
        parts = [
            self.partition.table.table_name if self.partition else None,
            self.partition.partition_name if self.partition else None,
            self.column.column_name if self.column else None,
            self.sample_name,
        ]
        return "_".join([part for part in parts if part])
