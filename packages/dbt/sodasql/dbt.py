"""DBT integeration"""

from __future__ import annotations

from typing import Any

from dbt.contracts.graph.compiled import ParsedModelNode, CompiledSchemaTestNode
from dbt.contracts.results import RunResultOutput
from dbt.node_types import NodeType


def parse_manifest(
    manifest: dict[str, Any]
) -> tuple[dict[str, ParsedModelNode], dict[str, CompiledSchemaTestNode]]:
    """
    Parse the manifest.

    Only V3 manifest is supported.

    Parameters
    ----------
    run_results : dict[str, Any]
        The raw manifest.

    Returns
    -------
    out : tuple[dict[str, ParsedModelNode], dict[str, CompiledSchemaTestNode]]
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

    model_nodes = {
        node_name: ParsedModelNode(**node)
        for node_name, node in manifest["nodes"].items()
        if node["resource_type"] == NodeType.Model
    }
    test_nodes = {
        node_name: CompiledSchemaTestNode(**node)
        for node_name, node in manifest["nodes"].items()
        if node["resource_type"] == NodeType.Test
    }
    return model_nodes, test_nodes


def parse_run_results(run_results: dict[str, Any]) -> list[RunResultOutput]:
    """
    Parse the run results.

    Only V3 run results is supported.

    Parameters
    ----------
    run_results : dict[str, Any]
        The raw run results.

    Returns
    -------
    out : list[RunResultOutput]
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

    parsed_run_results = [
        RunResultOutput(**result) for result in run_results["results"]
    ]
    return parsed_run_results
