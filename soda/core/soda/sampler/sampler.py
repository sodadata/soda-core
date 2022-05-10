from abc import ABC, abstractmethod
from typing import Optional

from soda.common.logs import Logs
from soda.execution.query import Query
from soda.sampler.storage_ref import StorageRef


class Sampler(ABC):

    @abstractmethod
    def store_sample(self, cursor, sample_name: str, query: str, logs: Logs) -> Optional[StorageRef]:
        pass
