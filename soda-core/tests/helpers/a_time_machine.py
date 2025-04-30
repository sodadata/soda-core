import datetime
from typing import Optional

import pytest


class TimeMachineDatetime(datetime.datetime):
    """
    Turns out that datetime.datetime.now cannot be mocked because it has native impl.
    And it is possible to overwrite datetime.datetime with a class that inherits from datetime.datetime.
    Especially the ordering of imports in the conftest.py is important. I renamed the file to a_time_machine.py so
    that precommit would order the imports in the right order.
    This solution requires that datetime.datetime is replaced with the class
    TimeMachineDatetime(datetime.datetime) ** before ** any import datetime from datetime is perfomed by the
    production code.
    """
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
