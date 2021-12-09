"""
CLI commands to ingest test results from various sources into the Soda cloud.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import Iterator

from sodasql.__version__ import SODA_SQL_VERSION
from sodasql.scan.parser import Parser
from sodasql.scan.scan_builder import (
    build_warehouse_yml_parser,
    create_soda_server_client,
)
from sodasql.scan.test import Test
from sodasql.scan.test_result import TestResult



def create_dbt_run_result_to_test_result_mapping(
    test_nodes: dict[str, CompiledSchemaTestNode],
    run_results: list[RunResultOutput],
):
    """
    Map run results to test results.

    Parameters
    ----------
    model_nodes : Dict[str: ParsedModelNode]
        The parsed model nodes.
    test_nodes : Dict[str: CompiledSchemaTestNode]
        The compiled schema test nodes.
    run_results : List[RunResultOutput]
        The run results.

    Returns
    -------
    out : Dict[str, set[ParseModelNode]]
        A mapping from run result to test result.
    """
    from dbt.contracts.results import TestStatus
    from dbt.contracts.graph.compiled import CompiledSchemaTestNode

    dbt_tests_with_soda_test = {
        test_node.unique_id:
        Test(
            id=Parser.create_test_id(
                test_expression=test_node.compiled_sql if isinstance(test_node, CompiledSchemaTestNode) else None,
                test_name=test_node.unique_id,
                test_index=index,
                context_column_name=test_node.column_name,
                context_sql_metric_name=None,
                context_sql_metric_index=None,
            ),
            title=f"dbt test - number of failures for {test_node.unique_id}",
            expression=test_node.compiled_sql if isinstance(test_node, CompiledSchemaTestNode) else None,
            metrics=None,
            column=test_node.column_name,
        )
        for index, test_node in enumerate(test_nodes.values())
    }

    tests_with_test_result = {
        run_result.unique_id:
        TestResult(
            dbt_tests_with_soda_test[run_result.unique_id],
            passed=run_result.status == TestStatus.Pass,
            skipped=run_result.status == TestStatus.Skipped,
            values={"failures": run_result.failures},
        )
        for run_result in run_results
    }
    return tests_with_test_result


def create_dbt_test_results_iterator(
    manifest_file: Path, run_results_file: Path
) -> Iterator[tuple[str, list[TestResult]]]:
    """
    Create an iterator for the dbt test results.

    Parameters
    ----------
    manifest : dict
        The path to the manifest file.
    run_results : Path
        The path to the run results file.

    Returns
    -------
    out : Iterator[tuple[str, list[TestResult]]]
        The table name and its corresponding test results.
    """
    try:
        from sodasql import dbt as soda_dbt
    except ImportError as e:
        raise RuntimeError(
            "Soda SQL dbt extension is not installed: $ pip install soda-sql-dbt"
        ) from e

    with manifest_file.open("r") as file:
        manifest = json.load(file)
    with run_results_file.open("r") as file:
        run_results = json.load(file)

    model_nodes, test_nodes = soda_dbt.parse_manifest(manifest)
    parsed_run_results = soda_dbt.parse_run_results(run_results)
    tests_with_test_result = create_dbt_run_result_to_test_result_mapping(
        test_nodes, parsed_run_results
    )
    models_with_tests = soda_dbt.create_models_to_tests_mapping(
        model_nodes, test_nodes, parsed_run_results
    )

    for model_unique_id, test_unique_ids in models_with_tests.items():
        table_name = model_nodes[model_unique_id].alias
        test_results = [
            tests_with_test_result[test_unique_id]
            for test_unique_id in test_unique_ids
        ]

        yield table_name, test_results


def ingest(
    tool: str,
    warehouse_yml_file: str,
    dbt_manifest: Path | None = None,
    dbt_run_results: Path | None = None,
) -> None:
    """
    Ingest test information from different tools.

    Arguments
    ---------
    tool : str {'dbt'}
        The tool name.
    warehouse_yml_file : str
        The warehouse yml file.
    dbt_manifest : Optional[Path]
        The path to the dbt manifest.
    dbt_run_results : Optional[Path]
        The path to the dbt run results.

    Raises
    ------
    ValueError :
       If the tool is unrecognized.
    """
    logger = logging.getLogger(__name__)
    logger.info(SODA_SQL_VERSION)

    warehouse_yml_parser = build_warehouse_yml_parser(warehouse_yml_file)
    warehouse_yml = warehouse_yml_parser.warehouse_yml

    soda_server_client = create_soda_server_client(warehouse_yml)
    if not soda_server_client.api_key_id or not soda_server_client.api_key_secret:
        raise ValueError("Missing Soda cloud api key id and/or secret.")

    if tool == "dbt":
        if dbt_manifest is None:
            raise ValueError(f"Dbt manifest is required: {dbt_manifest}")
        if dbt_run_results is None:
            raise ValueError(f"Dbt run results is required: {dbt_run_results}")
        test_results_iterator = create_dbt_test_results_iterator(dbt_manifest, dbt_run_results)
    else:
        raise ValueError(f"Unknown tool: {tool}")

    for table_name, test_results in test_results_iterator:
        start_scan_response = soda_server_client.scan_start(
            warehouse_yml.name,
            warehouse_yml.dialect.type,
            table_name=table_name,
            scan_yml_columns=None,
            scan_time=dt.datetime.now().isoformat(),
            origin=os.environ.get("SODA_SCAN_ORIGIN", "external")
        )

        test_results_jsons = [test_result.to_dict() for test_result in test_results]
        soda_server_client.scan_test_results(
            start_scan_response["scanReference"], test_results_jsons
        )

        soda_server_client.scan_ended(start_scan_response["scanReference"])
