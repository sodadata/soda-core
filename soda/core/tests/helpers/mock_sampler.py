from dataclasses import dataclass
from typing import Optional, Tuple

from soda.common.logs import Logs
from soda.sampler.sample_ref import SampleRef
from soda.sampler.sampler import Sampler


@dataclass
class Sample:
    rows: Tuple[Tuple]
    sample_ref: SampleRef
    query: str


class MockSampler(Sampler):
    def __init__(self):
        self.samples = []

    def store_sample(self, cursor, sample_name: str, query: str, logs: Logs) -> Optional[SampleRef]:
        rows = cursor.fetchall()
        row_count = len(rows)
        column_count = len(rows[0]) if row_count > 0 else 0

        sample_ref = SampleRef(
            name=sample_name,
            column_count=column_count,
            total_row_count=row_count,
            stored_row_count=row_count,
            type="mock",
            soda_cloud_file_id=soda_cloud_file_id,
        )
        self.samples.append(Sample(rows=rows, sample_ref=sample_ref, query=query))
        return sample_ref
