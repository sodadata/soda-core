import datetime
from typing import Optional

import pytest


class TimeMachineDatetime(datetime.datetime):
    NOW: Optional[datetime.datetime] = None

    @classmethod
    def now(cls, tz=None) -> datetime.datetime:
        return cls.NOW if cls.NOW else super().now(tz)


datetime.datetime = TimeMachineDatetime


class TimeMachine:
    def set_now(self, now: datetime.datetime):
        TimeMachineDatetime.NOW = now


@pytest.fixture(scope="function")
def time_machine() -> TimeMachine:
    try:
        yield TimeMachine()
    finally:
        TimeMachineDatetime.NOW = None
