from __future__ import annotations

from typing import Optional

from requests import Response
from soda_core.common.soda_cloud import SodaCloud


def build_discovery_payload(dqns: list[str], data_source_name: str, scan_definition_name: str) -> dict:
    """DQN-only sodaCoreInsertScanResults body for v4 discovery. Mirrors the non-routing
    fields v3 emits for BE DTO deserialization safety."""
    return {
        "type": "sodaCoreInsertScanResults",
        "version": "4",
        "scanType": None,
        "definitionName": scan_definition_name,
        "defaultDataSource": data_source_name,
        "metadata": [{"datasetQualifiedName": dqn} for dqn in dqns],
        "checks": [],
        "metrics": [],
        "profiling": [],
        "automatedMonitoringChecks": [],
    }


def send_discovery_results(soda_cloud: SodaCloud, payload: dict) -> Optional[Response]:
    return soda_cloud._execute_command(command_json_dict=payload, request_log_name="send_discovery_results")
