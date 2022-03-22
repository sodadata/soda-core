from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class Feedback:
    is_anomaly: bool
    is_correctly_classified: bool


@dataclass
class TimedValue:
    timestamp: datetime
    value: float
    feedback: Optional[Feedback]


@dataclass
class AnomalyInput:
    timed_values: List[TimedValue]


@dataclass
class AnomalyOutput:
    is_anomaly: bool
    anomaly_score: float


class AnomalyDetector(ABC):
    @abstractmethod
    def evaluate(self, anomaly_input: AnomalyInput) -> AnomalyOutput:
        pass
