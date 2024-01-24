import datetime
from typing import List, Optional

import pandas as pd
from pydantic import BaseModel, field_validator


class DetectorMessageComponent(BaseModel):
    """Defines the error code data object from freq detection."""

    log_message: str
    severity: str
    error_code_int: int
    error_code_str: str


class FreqDetectionResult(BaseModel):
    """Frequency Detection Result data model."""

    inferred_frequency: Optional[str]
    df: pd.DataFrame
    freq_detection_strategy: str
    error_code_int: int
    error_code: str
    error_severity: str
    error_message: str

    model_config = {
        "arbitrary_types_allowed": True,
    }


class UserFeedback(BaseModel):
    """Validation model for user feedback data dict in payload."""

    isCorrectlyClassified: Optional[bool] = None
    isAnomaly: Optional[bool] = None
    reason: Optional[str] = None
    freeTextReason: Optional[str] = None
    skipMeasurements: Optional[str] = None

    @field_validator("skipMeasurements")
    @classmethod
    def check_accepted_values_skip_measurements(cls, v: str) -> str:
        accepted_values = ["this", "previous", "previousAndThis", None]
        assert v in accepted_values, f"skip_measurements must be one of {accepted_values}, but '{v}' was provided."
        return v


class SeverityLevelAreas(BaseModel):
    """Validates severity levels dicts."""

    greaterThanOrEqual: Optional[float] = None
    lessThanOrEqual: Optional[float] = None


class AnomalyDiagnostics(BaseModel):
    value: Optional[float] = None
    fail: Optional[SeverityLevelAreas] = None
    warn: Optional[SeverityLevelAreas] = None
    anomalyProbability: Optional[float] = None
    anomalyPredictedValue: Optional[float] = None
    anomalyErrorSeverity: str = "pass"
    anomalyErrorCode: str = ""
    anomalyErrorMessage: str = ""


class LocationModel(BaseModel):
    filePath: Optional[str] = None
    line: Optional[int] = None
    col: Optional[int] = None


# some of those fields might end up being ignored down the line by ADS
class AnomalyResult(BaseModel):
    identity: Optional[str] = None
    measurementId: Optional[str] = None
    type: Optional[str] = None
    definition: Optional[str] = None
    location: LocationModel = LocationModel()
    metrics: Optional[List[str]] = None
    dataSource: Optional[str] = None
    table: Optional[str] = None
    partition: Optional[str] = None
    column: Optional[str] = None
    outcome: Optional[str] = None
    diagnostics: AnomalyDiagnostics = AnomalyDiagnostics()
    feedback: Optional[UserFeedback] = UserFeedback()


class AnomalyHistoricalCheckResults(BaseModel):
    results: List[AnomalyResult]


class AnomalyHistoricalMeasurement(BaseModel):
    id: str
    identity: str
    value: float
    dataTime: datetime.datetime


class AnomalyHistoricalMeasurements(BaseModel):
    results: List[AnomalyHistoricalMeasurement]
