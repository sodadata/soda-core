"""Per-item verification loop with shared data-source connection.

Universal executor for check-collection items (contracts, data standards,
mixed). Each item is verified inside its own try/except block; on failure
the item gets an ERROR-status placeholder result and the loop continues.

The single-input contract caller (``ContractVerificationSession.execute``
with a 1-element ``contract_yaml_sources`` list) sets
``abort_on_first_error=True`` to preserve the historical re-raise contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from soda_core.check_collections.base import (
    CheckCollectionImpl,
    CheckCollectionResult,
    CheckCollectionSessionResult,
)
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import CheckCollectionYamlSource


@dataclass(frozen=True)
class CheckCollectionItem:
    """One item to verify in a session.

    Plain tuple-like; explicit ``impl_class`` reference (no ``kind`` string).
    """

    impl_class: type[CheckCollectionImpl]
    yaml_source: CheckCollectionYamlSource
    collection_id: Optional[str] = None


def execute_check_collections(
    items: list[CheckCollectionItem],
    data_source_impl: Optional[DataSourceImpl],
    soda_cloud_impl: Optional[SodaCloud] = None,
    publish_results: bool = False,
    only_validate_without_execute: bool = False,
    variables: Optional[dict[str, str]] = None,
    data_timestamp: Optional[str] = None,
    all_data_source_impls: Optional[dict[str, DataSourceImpl]] = None,
    check_selectors: Optional[list] = None,
    dwh_data_source_file_path: Optional[str] = None,
    abort_on_first_error: bool = False,
    logs: Optional[Logs] = None,
    primary_data_source_impl: Optional[DataSourceImpl] = None,
) -> CheckCollectionSessionResult:
    """Run a list of check-collection items against a shared data-source connection.

    Per-item error isolation: each item is verified inside its own
    try/except. On failure, the item gets a ``CheckCollectionResult`` with
    ``status=ERROR`` and the exception attached on ``.error``. The result
    list stays positional with the input items.

    ``abort_on_first_error``: when True, the first exception is re-raised
    verbatim and aborts the loop. Used by the legacy single-contract path
    via ``ContractVerificationSession``. When False (default), exceptions
    are isolated so the launcher can map results back to inputs by index.

    Upload-must-split rule: every item's results are uploaded to Soda Cloud
    in its own ``sodaCoreInsertScanResults`` request (one upload per item),
    keyed by ``wire_source`` (e.g. ``soda-contract`` for contracts,
    ``soda-data-standard`` for data standards). The backend cannot ingest a
    single upload that mixes checks from different wire sources — its
    ingestion filter routes the whole batch by the top-level ``source`` and
    rejects the request if any check inside disagrees. Mixed-source items in
    one ``execute_check_collections`` call are therefore safe (each item
    uploads independently); a single item must never emit checks whose
    ``source`` disagrees with its own ``wire_source``.

    Callers wanting the universal entrypoint pass ``primary_data_source_impl``
    explicitly. The contract path uses ``ContractVerificationSessionImpl``,
    which resolves the primary data source from its named-data-source map
    *before* calling this function — no ``"primary_datasource"`` key
    convention leaks into the universal executor.
    """
    results: list[CheckCollectionResult] = []
    for item in items:
        try:
            yaml = item.impl_class.yaml_class.parse(
                yaml_source=item.yaml_source,
                provided_variable_values=variables,
                data_timestamp=data_timestamp,
                primary_data_source_impl=primary_data_source_impl,
            )
            # Forward the parsed yaml's resolved data_timestamp /
            # execution_timestamp to the impl when the yaml exposes them
            # (ContractYaml does; the base CheckCollectionYaml in POC scope
            # does not yet). Fall back to the raw inputs otherwise.
            yaml_data_timestamp = getattr(yaml, "data_timestamp", None)
            yaml_execution_timestamp = getattr(yaml, "execution_timestamp", None)
            impl = item.impl_class(
                yaml=yaml,
                data_source_impl=data_source_impl,
                soda_cloud_impl=soda_cloud_impl,
                publish_results=publish_results,
                collection_id=item.collection_id,
                only_validate_without_execute=only_validate_without_execute,
                check_selectors=check_selectors,
                all_data_source_impls=all_data_source_impls,
                dwh_data_source_file_path=dwh_data_source_file_path,
                logs=logs,
                data_timestamp=yaml_data_timestamp if yaml_data_timestamp is not None else data_timestamp,
                execution_timestamp=yaml_execution_timestamp,
            )
            results.append(impl.verify())
        except Exception as exc:
            if abort_on_first_error:
                raise
            results.append(item.impl_class.build_error_result(item.yaml_source, exc))
    return CheckCollectionSessionResult(results)
