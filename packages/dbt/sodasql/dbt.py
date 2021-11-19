"""DBT integeration"""

from __future__ import annotations

import dataclasses
import enum
from typing import Any


@dataclasses.dataclass(frozen=True)
class Node:
    """
    A manifest node.

    Source
    ------
    https://schemas.getdbt.com/dbt/manifest/v3/index.html#nodes
    """
    refs: list[list[str]]
    compiled_sql: str
    unique_id: str


def parse_manifest(manifest: dict[str, Any]) -> dict[str, Node]:
    """
    Parse the manifest.

    Only V3 manifest is supported.

    Parameters
    ----------
    run_results : dict[str, Any]
        The raw manifest.

    Returns
    -------
    out : dict[str, Node]
        The parsed manifest.

    Raises
    ------
    NotImplementedError :
        If the dbt schema is not equal to the V3 manifest

    Source
    ------
    https://docs.getdbt.com/reference/artifacts/manifest-json
    """
    dbt_v3_schema = "https://schemas.getdbt.com/dbt/manifest/v3.json"
    if manifest["metadata"]["dbt_schema_version"] != dbt_v3_schema:
        raise NotImplementedError("Dbt manifest parsing only supported for V3 schema.")

    nodes = {node_name: Node(**node) for node_name, node in manifest["node"].items()}
    return nodes


@enum.unique
class Status(str, enum.Enum):
    """
    Result status.

    Source
    ------
    https://schemas.getdbt.com/dbt/run-results/v3/index.html#results_items_status
    """

    SUCCES = "succes"
    ERROR = "error"
    SKIPPED = "skipped"
    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"
    RUNTIME_ERROR = "runtime error"


@dataclasses.dataclass(frozen=True)
class Timing:
    """
    Result timing.

    Source
    ------
    https://schemas.getdbt.com/dbt/run-results/v3/index.html#results_items_timing
    """

    name: str
    started_at: str
    completed_at: str


@dataclasses.dataclass(frozen=True)
class Result:
    """
    Results in run results.

    Source
    ------
    https://schemas.getdbt.com/dbt/run-results/v3/index.html#results
    """

    status: Status
    timing: list[Timing]
    thread_id: str
    execution_time: float
    adapter_response: dict[str, Any]
    message: str | None
    failures: int | None
    unique_id: str


def parse_run_results(run_results: dict[str, Any]) -> list[Result]:
    """
    Parse the run results.

    Only V3 run results is supported.

    Parameters
    ----------
    run_results : dict[str, Any]
        The raw run results.

    Returns
    -------
    out : list[Result]
        The parsed run results.

    Raises
    ------
    NotImplementedError :
        If the dbt schema is not equal to the V3 run results.

    Source
    ------
    https://docs.getdbt.com/reference/artifacts/run-results-json
    """
    dbt_v3_schema = "https://schemas.getdbt.com/dbt/run-results/v3.json"
    if run_results["metadata"]["dbt_schema_version"] != dbt_v3_schema:
        raise NotImplementedError(
            "Dbt run results parsing only supported for V3 schema."
        )

    parsed_run_results = [Result(**result) for result in run_results["results"]]
    return parsed_run_results
