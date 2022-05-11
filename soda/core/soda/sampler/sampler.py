from abc import ABC, abstractmethod
from typing import Optional

from soda.common.logs import Logs
from soda.execution.query import Query
from soda.sampler.sample_context import SampleContext
from soda.sampler.sample_ref import SampleRef


class Sampler(ABC):

    @abstractmethod
    def store_sample(self, sample_context: SampleContext) -> SampleRef:
        pass
