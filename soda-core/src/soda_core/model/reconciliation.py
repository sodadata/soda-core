from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ReconciliationDatasetYaml(
    BaseModel,
    extra="forbid",
    frozen=True,
):
    dataset: str = Field(..., description="The dqn of the dataset to be reconciled.")
    filter: str | None = Field(None, description="Optional filter to apply to the dataset.")


class ReconciliationYaml(
    BaseModel,
    extra="forbid",
    frozen=True,
    arbitrary_types_allowed=True,  # Allow CheckYaml to be used in the checks list
):
    """
    Represents a reconciliation block for Contracts.
    """

    source: ReconciliationDatasetYaml = Field(
        ...,
        description="The source dataset for reconciliation, which includes the dataset name and an optional filter.",
    )

    # TODO: using "any" here is a workaround for circular imports. CheckYaml should be used.
    checks: list[Any] = Field(
        default_factory=list,
        description="A list of checks to be performed on the reconciliation.",
    )
