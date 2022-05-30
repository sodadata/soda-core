from dataclasses import dataclass
from enum import Enum
from typing import Union


class CheckOutcome(Enum):

    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"

    def __str__(self):
        return self.name


@dataclass
class QueryFailed:
    message: str
    severity: str


@dataclass
class NotEnoughHistory:
    message: str
    severity: str


@dataclass
class CheckOutcomeReasons:
    queryFailed: Union[QueryFailed, None] = None
    notEnoughHistory: Union[NotEnoughHistory, None] = None
