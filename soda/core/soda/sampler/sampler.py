from abc import ABC
from typing import Optional

from soda.execution.query import Query
from soda.sampler.storage_ref import StorageRef


class Sampler(ABC):
    def __init__(self):
        # Initialized in the scan.execute
        self.logs = None

    def store_sample(self, cursor, query: Optional[Query]) -> Optional[StorageRef]:
        pass
