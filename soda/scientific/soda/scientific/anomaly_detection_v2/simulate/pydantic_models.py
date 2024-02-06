from typing import List, Optional

from pydantic import BaseModel, Field, model_validator


class EvaluateOutput(BaseModel):
    ds: str = Field(alias="dataTime")
    value: Optional[float] = Field(None, alias="value")
    yhat: Optional[float] = Field(None, alias="anomalyPredictedValue")
    level: Optional[str] = None
    warn_lower_bound: Optional[float] = None
    warn_upper_bound: Optional[float] = None
    fail_lower_bound: Optional[float] = None
    fail_upper_bound: Optional[float] = None
    label: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def extract_uncertainty_bounds(cls, values) -> dict:
        warn_bounds = values.get("warn")
        if warn_bounds:
            values["warn_lower_bound"] = warn_bounds["lessThanOrEqual"]
            values["warn_upper_bound"] = warn_bounds["greaterThanOrEqual"]

        fail_bounds = values.get("fail")
        if fail_bounds:
            values["fail_lower_bound"] = fail_bounds["lessThanOrEqual"]
            values["fail_upper_bound"] = fail_bounds["greaterThanOrEqual"]

        return values


class AnomalyDetectionResults(BaseModel):
    results: List[EvaluateOutput]
