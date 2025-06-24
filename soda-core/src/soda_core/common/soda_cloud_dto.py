from __future__ import annotations

import numbers
from datetime import date, datetime
from numbers import Number
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class SodaCloudDiagnostics(BaseModel):
    schema_: Optional[SodaCloudSchemaDiagnostics] = Field(..., alias="schema")
    metricValues: Optional[SodaCloudMetricValuesDiagnostics] = None
    freshness: Optional[SodaCloudFreshnessDiagnostics] = None
    value: Optional[Number] = None
    fail: Optional[SodaCloudThresholdDiagnostic] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class SodaCloudThresholdDiagnostic(BaseModel):
    greaterThan: Optional[Number]
    greaterThanOrEqual: Optional[Number]
    lessThan: Optional[Number]
    lessThanOrEqual: Optional[Number]

    model_config = ConfigDict(arbitrary_types_allowed=True)


class SodaCloudMetricValuesDiagnostics(BaseModel):
    thresholdMetricName: Optional[str]
    values: dict[str, Number]

    model_config = ConfigDict(arbitrary_types_allowed=True)


class SodaCloudSchemaDiagnostics(BaseModel):
    expectedColumns: list[SodaCloudSchemaColumnInfo]
    actualColumns: list[SodaCloudSchemaColumnInfo]
    expectedColumnNamesNotActual: list[str]
    actualColumnNamesNotExpected: list[str]
    columnDataTypeMismatches: list[SodaCloudSchemaDataTypeMismatch]
    areColumnsOutOfOrder: Optional[bool]


class SodaCloudSchemaDataTypeMismatch(BaseModel):
    column: str
    expectedDataType: Optional[str] = None
    actualDataType: Optional[str] = None
    expectedCharacterMaximumLength: Optional[int] = None
    actualCharacterMaximumLength: Optional[int] = None


class SodaCloudSchemaColumnInfo(BaseModel):
    name: str
    dataType: Optional[str] = None
    characterMaximumLength: Optional[int] = None


class SodaCloudFreshnessDiagnostics(BaseModel):
    maxTimestamp: Optional[datetime]
    maxTimestampUtc: Optional[datetime]
    dataTimestamp: datetime
    dataTimestampUtc: datetime
    freshness: Optional[str] = None
    freshnessInSeconds: Optional[int] = None
    unit: Optional[str] = None


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
