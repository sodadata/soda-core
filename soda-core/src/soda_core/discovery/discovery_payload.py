from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

from soda_core.common.datetime_conversions import (
    convert_datetime_to_str,
    convert_str_to_datetime,
)
from soda_core.common.soda_cloud_dto import SodaCoreInsertScanResultsDTO


def resolve_data_timestamp(default: datetime) -> datetime:
    """The data timestamp for the scan: ``SODA_SCAN_DATA_TIMESTAMP`` (set by the
    orchestrator for managed runs) when present and parseable, else ``default``."""
    data_timestamp_str: Optional[str] = os.environ.get("SODA_SCAN_DATA_TIMESTAMP")
    if data_timestamp_str:
        parsed: Optional[datetime] = convert_str_to_datetime(data_timestamp_str)
        if parsed is not None:
            return parsed
    return default


def build_discovery_payload(
    dqns: list[str],
    data_source_name: str,
    scan_definition_name: str,
    data_timestamp: Optional[datetime] = None,
    scan_start_timestamp: Optional[datetime] = None,
    scan_end_timestamp: Optional[datetime] = None,
) -> SodaCoreInsertScanResultsDTO:
    """Build a DQN-only sodaCoreInsertScanResults body for discovery.

    ``dataTimestamp``, ``scanStartTimestamp``, ``scanEndTimestamp`` and ``hasErrors``
    are @NotNull-validated by the backend command, so they are always emitted:
    missing timestamps default to now (UTC) and the data timestamp falls back to
    ``SODA_SCAN_DATA_TIMESTAMP`` / scan start. ``hasErrors`` is False because the
    discovery handler only sends the payload after a successful discovery run.
    Timestamps are serialized like soda-core's ``to_jsonnable`` (ISO-8601 UTC).
    Sent via ``SodaCloud.insert_scan_results``."""
    now: datetime = datetime.now(timezone.utc)
    scan_start_timestamp = scan_start_timestamp or now
    scan_end_timestamp = scan_end_timestamp or now
    data_timestamp = data_timestamp or resolve_data_timestamp(default=scan_start_timestamp)
    return {
        "scanId": os.environ.get("SODA_SCAN_ID", None),
        "type": "sodaCoreInsertScanResults",
        "version": "4",
        "scanType": None,
        "definitionName": scan_definition_name,
        "defaultDataSource": data_source_name,
        "dataTimestamp": convert_datetime_to_str(data_timestamp),
        "scanStartTimestamp": convert_datetime_to_str(scan_start_timestamp),
        "scanEndTimestamp": convert_datetime_to_str(scan_end_timestamp),
        "hasErrors": False,
        "metadata": [{"datasetQualifiedName": dqn} for dqn in dqns],
        "checks": [],
        "metrics": [],
        "profiling": [],
        "automatedMonitoringChecks": [],
    }
