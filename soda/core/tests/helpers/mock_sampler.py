from dataclasses import dataclass
from typing import Tuple, Optional

from soda.common.logs import Logs
from soda.sampler.sampler import Sampler
from soda.sampler.storage_ref import StorageRef
from soda.soda_cloud.soda_cloud import SodaCloud


@dataclass
class Sample:
    rows: Tuple[Tuple]
    storage_ref: StorageRef
    query: str


class MockSampler(Sampler):

    def __init__(self):
        self.samples = []

    def store_sample(self, cursor, sample_name: str, query: str, soda_cloud: SodaCloud, logs: Logs) -> Optional[StorageRef]:
        rows = cursor.fetchall()
        row_count = len(rows)
        column_count = len(rows[0]) if row_count > 0 else 0
        storage_ref = StorageRef(
            provider="mock",
            sample_name=sample_name,
            column_count=column_count,
            total_row_count=row_count,
            stored_row_count=row_count,
            reference=f'In-memory mock sample {sample_name}',
        )
        soda_cloud.upload_sample(rows, storage_ref)
        self.samples.append(Sample(rows=rows, storage_ref=storage_ref, query=query))
        return storage_ref
