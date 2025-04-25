from __future__ import annotations

from datetime import datetime
from typing import Optional


class CurrentTime:
    frozen_now: Optional[datetime] = None

    @classmethod
    def now(cls) -> datetime:
        return datetime.now() if cls.frozen_now is None else cls.frozen_now

    @classmethod
    def freeze_now(cls, frozen_now: datetime) -> FrozenTimeContext:
        """
        Purpose: freeze time in test cases
        with CurrentTime.freeze_now(datetime(...)):
          # use CurrentTime.now()
        """
        frozen_time_context = FrozenTimeContext(original_frozen_now=cls.frozen_now)
        cls.frozen_now = frozen_now
        return frozen_time_context


class FrozenTimeContext:
    def __init__(self, original_frozen_now: Optional[datetime]):
        self.original_frozen_now: Optional[datetime] = original_frozen_now

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        CurrentTime.frozen_now = self.original_frozen_now
