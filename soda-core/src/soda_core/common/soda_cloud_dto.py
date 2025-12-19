from __future__ import annotations

import numbers
from datetime import date, datetime
from enum import Enum
from typing import Annotated, Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class SamplerType(str, Enum):
    ABSOLUTE_LIMIT = "absoluteLimit"


class CheckAttributes(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    check_attributes: list[CheckAttribute] = Field(..., alias="resourceAttributes")


class CheckAttribute(BaseModel):
    name: str
    value: Any

    @classmethod
    def from_raw(cls, name: str, value: Any) -> "CheckAttribute":
        return cls(name=name, value=cls._format_value(value))

    @staticmethod
    def _format_value(value: Any) -> Any:
        # Handle boolean strings explicitly to avoid misinterpretation
        if isinstance(value, str):
            if value.lower() == "true":
                return True
            elif value.lower() == "false":
                return False
        if isinstance(value, bool):
            return bool(value)
        if not isinstance(value, datetime) and isinstance(value, date):
            value = datetime.combine(value, datetime.min.time())
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=datetime.now().astimezone().tzinfo)
            return value.isoformat()
        if isinstance(value, numbers.Number):
            return str(value)
        return value


class DatasetConfigurationsDTO(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    dataset_configurations: list[DatasetConfigurationDTO] = Field(..., alias="datasetConfigurations")


class ComputeWarehouseOverrideDTO(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    name: str = Field(..., alias="name")


class DatasetConfigurationDTO(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    collect_failed_rows: Optional[bool] = Field(None, alias="collectFailedRows")
    dataset_qualified_name: Optional[str] = Field(None, alias="datasetQualifiedName")

    # MM and profiling skipped for now, extra=allow will take care of it, this needs to be added as a full structure later
    # metric_monitoring_configuration: Optional[list[dict[str, Any]]] = Field(None, alias="metricMonitoringConfiguration")
    # profiling_configuration: Optional[dict[str, Any]] = Field(None, alias="profilingConfiguration")

    samples_columns: Optional[list[str]] = Field(None, alias="samplesColumns")
    table: Optional[str] = Field(None, alias="table")
    test_row_sampler_configuration: Optional[TestRowSamplerConfigurationDTO] = Field(
        None, alias="testRowSamplerConfiguration"
    )
    compute_warehouse_override: Optional[ComputeWarehouseOverrideDTO] = Field(None, alias="computeWarehouseOverride")


class TestRowSamplerAbsoluteLimitDTO(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    type: Literal[SamplerType.ABSOLUTE_LIMIT] = Field(..., alias="type")
    limit: int = Field(..., alias="limit")

    def __str__(self):
        return f"AbsoluteLimitRowSampler(limit={self.limit})"


RowSampler = Annotated[
    Union[TestRowSamplerAbsoluteLimitDTO,],
    Field(discriminator="type"),
]


class TestRowSamplerConfigurationDTO(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    enabled: bool = Field(..., alias="enabled")
    test_row_sampler: Optional[RowSampler] = Field(None, alias="testRowSampler")


class RequestDatasetsConfigurationDTO(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: Literal["sodaCoreDatasetsConfiguration"] = Field(
        "sodaCoreDatasetsConfiguration",
        alias="type",
        frozen=True,
    )
    version: Literal[4] = Field(4, alias="version", frozen=True)
    datasets: list[RequestDatasetsConfigurationDatasetDTO] = Field(..., alias="datasets")


class RequestDatasetsConfigurationDatasetDTO(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    data_source: str = Field(..., alias="dataSource")
    dataset_qualified_name: str = Field(..., alias="datasetQualifiedName")
    table: str = Field(..., alias="table")
