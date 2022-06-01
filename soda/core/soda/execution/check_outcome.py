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
class ReasonCodes:
    message: str
    severity: str

@dataclass
class QueryFailed(ReasonCodes):
    ...

@dataclass
class NotEnoughHistory(ReasonCodes):
    ...

@dataclass
class ParserFailed(ReasonCodes):
    ...

@dataclass
class CheckOutcomeReasons:
    parserFailed: Union[ParserFailed, None] = None
    queryFailed: Union[QueryFailed, None] = None
    notEnoughHistory: Union[NotEnoughHistory, None] = None
