"""
CLI commands to ingest test results from various sources into the Soda cloud.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from pathlib import Path

from sodasql.__version__ import SODA_SQL_VERSION
from sodasql.scan.parser import Parser
from sodasql.scan.scan_builder import (
    build_warehouse_yml_parser,
    create_soda_server_client,
)
from sodasql.scan.test import Test
from sodasql.scan.test_result import TestResult


def ingest(
    tool: str,
    warehouse_yml_file: str,
    dbt_manifest: Path | None = None,
    dbt_run_results: Path | None = None,
):
    """
    Ingest test information from different tools.

    Arguments
    ---------
    tool : str
        The tool name.
    warehouse_yml_file : str
        The warehouse yml file.
    dbt_manifest : Optional[Path]
        The path to the dbt manifest.
    dbt_run_results : Optional[Path]
        The path to the dbt run results.
    """
    logger = logging.getLogger(__name__)
    logger.info(SODA_SQL_VERSION)

    if tool == "dbt":
        try:
            from dbt.contracts.results import TestStatus
            from dbt.contracts.graph.compiled import CompiledSchemaTestNode
            from sodasql import dbt as soda_dbt
        except ImportError as e:
            raise RuntimeError(
                "Soda SQL dbt extension is not installed: $ pip install soda-sql-dbt"
            ) from e
        if dbt_manifest is None:
            raise ValueError(f"Dbt manifest is required: {dbt_manifest}")
        if dbt_run_results is None:
            raise ValueError(f"Dbt run results is required: {dbt_run_results}")

        warehouse_yml_parser = build_warehouse_yml_parser(warehouse_yml_file)
        warehouse_yml = warehouse_yml_parser.warehouse_yml

        soda_server_client = create_soda_server_client(warehouse_yml)
        if not soda_server_client.api_key_id or not soda_server_client.api_key_secret:
            raise ValueError("Missing Soda cloud api key id and/or secret.")

        with dbt_manifest.open("r") as manifest:
            model_nodes, test_nodes = soda_dbt.parse_manifest(json.load(manifest))
        with dbt_run_results.open("r") as run_results:
            parsed_run_results = soda_dbt.parse_run_results(json.load(run_results))

        models_with_tests = soda_dbt.create_models_to_tests_mapping(
            model_nodes, test_nodes, parsed_run_results
        )

        tests_in_run_results = [
            (run_result, test_nodes[run_result.unique_id])
            for run_result in parsed_run_results
            if run_result.unique_id in test_nodes.keys()
        ]

        tests = [
            Test(
                id=Parser.create_test_id(
                    test_expression=test_node.compiled_sql if isinstance(test_node, CompiledSchemaTestNode) else None,
                    test_name=test_run_results.unique_id,
                    test_index=index,
                    context_column_name=test_node.column_name,
                    context_sql_metric_name=None,
                    context_sql_metric_index=None,
                ),
                title=f"dbt test - number of failures for {test_run_results.unique_id}",
                expression=test_node.compiled_sql if isinstance(test_node, CompiledSchemaTestNode) else None,
                metrics=None,
                column=test_node.column_name,
            )
            for index, (test_run_results, test_node) in enumerate(tests_in_run_results)
        ]

        test_results = {
            test_run_results.unique_id:
            TestResult(
                test,
                passed=test_run_results.status == TestStatus.Pass,
                skipped=test_run_results.status == TestStatus.Skipped,
                values={"failures": test_run_results.failures},
            )
            for (test_run_results, _), test in zip(tests_in_run_results, tests)
        }

        for model_unique_id, test_unique_ids in models_with_tests.items():
            start_scan_response = soda_server_client.scan_start(
                warehouse_yml.name,
                warehouse_yml.dialect.type,
                table_name=model_nodes[model_unique_id].alias,
                scan_yml_columns=None,
                scan_time=dt.datetime.now().isoformat(),
                origin=os.environ.get("SODA_SCAN_ORIGIN", "external")
            )

            test_result_jsons = [
                test_results[test_unique_id].to_dict()
                for test_unique_id in test_unique_ids
            ]
            soda_server_client.scan_test_results(
                start_scan_response["scanReference"], test_result_jsons
            )

            soda_server_client.scan_ended(start_scan_response["scanReference"])
