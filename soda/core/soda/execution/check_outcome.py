from dataclasses import dataclass
from enum import Enum
from typing import Union


class CheckOutcome(Enum):

    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"

    def __str__(self):
        return self.name
