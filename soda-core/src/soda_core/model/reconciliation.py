from __future__ import annotations

from abc import ABC


class ReconciliationDatasetBase(ABC):
    dataset: str
    filter: str


class ReconciliationBase(ABC):
    source: ReconciliationDatasetBase
    checks: list["CheckYaml"]
