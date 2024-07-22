from abc import ABC, abstractmethod
from typing import Tuple

from soda.sampler.sample_schema import SampleSchema


class Sample(ABC):
    @abstractmethod
    def get_rows(self) -> Tuple[Tuple]:
        # get_rows should return a sufficient number of rows to fulfill
        # the sample request, but is not guaranteed to return all rows.
        # Leverage
        pass

    @abstractmethod
    def get_rows_count(self) -> int:
        # Returns total number of rows involved with the sampler. This number
        # can be higher than the total number of rows returned by get_rows.
        pass

    @abstractmethod
    def get_schema(self) -> SampleSchema:
        pass
