from __future__ import annotations

import numbers
from datetime import date, datetime
from typing import Any
from pydantic import BaseModel, Field


class CheckAttributes(BaseModel, populate_by_name=True):
    check_attributes: list[CheckAttribute] = Field(..., alias="resourceAttributes")


class CheckAttribute(BaseModel):
    name: str
    value: Any

    @classmethod
    def from_raw(cls, name: str, value: Any) -> "CheckAttribute":
        return cls(name=name, value=cls._format_value(value))

    @staticmethod
    def _format_value(value: Any) -> Any:
        if isinstance(value, bool):
            return value
        if not isinstance(value, datetime) and isinstance(value, date):
            value = datetime.combine(value, datetime.min.time())
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=datetime.now().astimezone().tzinfo)
            return value.isoformat()
        if isinstance(value, numbers.Number):
            return str(value)
        return value
