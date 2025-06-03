from __future__ import annotations

from datetime import datetime
from numbers import Number
from typing import Optional

from pydantic import BaseModel, ConfigDict


class SodaCloudDiagnostics(BaseModel):
    schema: Optional[SodaCloudSchemaDiagnostics] = None
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
    values: dict[str, float]


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
