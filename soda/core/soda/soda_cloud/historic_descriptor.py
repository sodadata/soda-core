from abc import ABC
from dataclasses import dataclass
from typing import Optional


class HistoricDescriptor(ABC):
    pass


@dataclass(frozen=True)
class HistoricMeasurementsDescriptor(HistoricDescriptor):
    metric_identity: Optional[str]
    limit: Optional[int] = 100


@dataclass(frozen=True)
class HistoricCheckResultsDescriptor(HistoricDescriptor):
    check_identity: Optional[str]
    limit: Optional[int] = 100
