import abc
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

DEFAULT_SCHEMA = "failed_rows"


class FailedRowsStrategy(str, Enum):
    """Enum for failed rows strategy"""

    NONE = "none"
    STORE_DIAGNOSTICS = "store_diagnostics"
    STORE_KEYS = "store_keys"
    STORE_DATA = "store_data"


class FailedRowsConfigBase(
    BaseModel,
    abc.ABC,
    frozen=True,
    extra="forbid",
    validate_by_name=True,  # Allow to use both field names and aliases when populating from dict
):
    enabled: Optional[bool] = Field(
        None,
        description="Enable or disable the storage of failed rows. " "If set to false, failed rows will not be stored.",
    )


class FailedRowsConfigOrganisation(FailedRowsConfigBase):
    """Top-level configuration for failed rows, used on Soda Cloud."""

    # Overrides the `enabled` field on other levels as Cloud config will return a bool always.
    enabled: bool = Field(
        ...,
        description="Enable or disable the storage of failed rows. " "If set to false, failed rows will not be stored.",
    )

    path_default: Optional[str] = Field(
        "{{ data_source.database }}/{DEFAULT_SCHEMA}",  # TODO: revisit
        description="Path to the warehouse location where failed rows will be stored. ",
    )
    enabled_by_default: bool = Field(
        True,
        description="Enable or disable the storage of failed rows by default. Does not override the `enabled` setting if `enabled` is set to false."
        "If set to false, failed rows will not be stored unless explicitly enabled in the contract or check.",
    )
    strategy_default: FailedRowsStrategy = Field(
        FailedRowsStrategy.STORE_DIAGNOSTICS, description="Default strategy for storing failed rows."
    )


class FailedRowsConfigDatasource(FailedRowsConfigBase):
    """Top-level configuration for failed rows, on data source level."""

    path: Optional[str] = Field(None, description="Path to the warehouse location where failed rows will be stored.")
    enabled_by_default: Optional[bool] = Field(
        True,
        description="Enable or disable the storage of failed rows by default. Does not override the `enabled` setting if `enabled` is set to false."
        "If set to false, failed rows will not be stored unless explicitly enabled in the contract or check.",
    )
    strategy_default: Optional[FailedRowsStrategy] = Field(
        FailedRowsStrategy.STORE_DIAGNOSTICS, description="Default strategy for storing failed rows."
    )


class FailedRowsConfigContract(FailedRowsConfigBase):
    """Configuration for failed rows at the contract level."""

    path: Optional[str] = Field(None, description="Path to the warehouse location where failed rows will be stored.")
    strategy: Optional[FailedRowsStrategy] = Field(None, description="Strategy for storing failed rows.")


class FailedRowsConfigCheck(FailedRowsConfigBase):
    """Configuration for failed rows at the check level."""

    strategy: Optional[FailedRowsStrategy] = Field(None, description="Strategy for storing failed rows.")
