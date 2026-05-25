"""Per-item verification loop with shared data-source connection.

Universal executor for check-collection YAML sources (contracts, data
standards, mixed). Each yaml is verified inside its own try/except block;
on failure the item gets an ERROR-status placeholder result and the loop
continues.

The single-input contract caller (``ContractVerificationSession.execute``
with a 1-element ``contract_yaml_sources`` list) sets
``abort_on_first_error=True`` to preserve the historical re-raise contract.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Union

from soda_core.check_collections.base import (
    CheckCollectionImpl,
    CheckCollectionResult,
    CheckCollectionSessionResult,
)
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.datetime_conversions import (
    convert_datetime_to_str,
    convert_str_to_datetime,
)
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import CheckCollectionYamlSource
from soda_core.contracts.impl.diagnostics_warehouse_files import (
    DiagnosticsWarehouseFiles,
)


def execute_check_collections(
    yaml_sources: list[CheckCollectionYamlSource],
    data_source_impl: Optional[DataSourceImpl],
    soda_cloud_impl: Optional[SodaCloud] = None,
    publish_results: bool = False,
    only_validate_without_execute: bool = False,
    variables: Optional[dict[str, str]] = None,
    data_timestamp: Optional[Union[str, datetime]] = None,
    all_data_source_impls: Optional[dict[str, DataSourceImpl]] = None,
    check_selectors: Optional[list] = None,
    dwh_files: Optional[DiagnosticsWarehouseFiles] = None,
    abort_on_first_error: bool = False,
    logs: Optional[Logs] = None,
    primary_data_source_impl: Optional[DataSourceImpl] = None,
    default_impl_class: Optional[type[CheckCollectionImpl]] = None,
) -> CheckCollectionSessionResult:
    """Run a list of check-collection YAML sources.

    Per-item dispatch: for each ``yaml_source`` the engine reads the
    YAML's top-level ``kind:`` field (defaulting to ``"contract"`` when
    absent for BC with existing contract YAMLs that don't declare one)
    and looks up the impl class via ``CheckCollectionImpl.for_kind(...)``.

    Per-item error isolation: each yaml is verified inside its own
    try/except. On failure, the item gets a ``CheckCollectionResult`` with
    ``status=ERROR`` and the exception attached on ``.error``. The result
    list stays positional with the input.

    ``abort_on_first_error`` (default False) — when True the first
    exception re-raises verbatim. Used by the legacy single-contract
    facade via ``ContractVerificationSession``.

    Upload model: one ``sodaCoreInsertScanResults`` request per
    ``(session, wire_source)`` pair. Subtypes whose impl class declares
    ``combine_uploads = True`` (e.g. data standards) issue one combined
    request per session covering all that subtype's files; subtypes that
    keep the default ``combine_uploads = False`` (e.g. contracts) upload
    once per file inside ``verify()`` itself. Either way the backend
    cannot ingest a single upload that mixes checks from different wire
    sources — its ingestion filter routes the whole batch by the top-level
    ``source`` and rejects the request if any check inside disagrees.
    Mixed-source items in one ``execute_check_collections`` call are
    therefore safe (each wire-source group uploads independently); a single
    item must never emit checks whose ``source`` disagrees with its own
    ``wire_source``.

    Callers wanting the universal entrypoint pass ``primary_data_source_impl``
    explicitly. The contract path uses ``ContractVerificationSessionImpl``,
    which resolves the primary data source from its named-data-source map
    *before* calling this function — no ``"primary_datasource"`` key
    convention leaks into the universal executor.

    ``data_timestamp`` is parsed once at this boundary: callers may pass
    either an ISO-8601 string (legacy public API form) or a ``datetime``.
    Internally and downstream into ``CheckCollectionImpl.__init__`` the
    value is always ``datetime`` — no string-typed value ever reaches
    ``self.data_timestamp``.

    ``default_impl_class`` is the impl class used to build the ERROR
    placeholder when kind dispatch fails before an ``impl_class`` could be
    resolved (unknown ``kind:`` value, malformed YAML, file-not-found, ...).
    Callers that publish a subtype-narrowed return type
    (e.g. ``ContractVerificationSessionImpl.execute`` returns
    ``list[ContractVerificationResult]``) pass ``ContractImpl`` so the
    fallback's ``build_error_result`` returns the right ``result_class``
    and the typed return stays honest. Defaults to ``CheckCollectionImpl``
    for callers that genuinely don't know a sensible subtype default.
    """
    parsed_data_timestamp: Optional[datetime] = _parse_data_timestamp(data_timestamp)
    # The yaml-level parser still takes the ISO string (it has its own
    # validation path); rebuild a string form when callers supplied a
    # datetime so both representations stay consistent.
    data_timestamp_str: Optional[str] = (
        data_timestamp
        if isinstance(data_timestamp, str)
        else (convert_datetime_to_str(parsed_data_timestamp) if parsed_data_timestamp is not None else None)
    )

    results: list[CheckCollectionResult] = []
    # Parallel to ``results``: the impl class used for each item. ``None``
    # entries correspond to results whose kind dispatch failed before an
    # impl class could be resolved — those don't participate in combined
    # upload grouping below.
    result_impl_classes: list[Optional[type[CheckCollectionImpl]]] = []
    for yaml_source in yaml_sources:
        impl_class: Optional[type[CheckCollectionImpl]] = None
        try:
            # Peek the kind from the YAML root. Cheap parse (YAML library
            # call); the subtype's full parse below repeats this work but
            # also walks columns/checks/variables which is the heavy part.
            yaml_object = yaml_source.parse()
            kind = (yaml_object.read_string_opt("kind") if yaml_object is not None else None) or "contract"
            impl_class = CheckCollectionImpl.for_kind(kind)

            yaml = impl_class.yaml_class.parse(
                yaml_source=yaml_source,
                provided_variable_values=variables,
                data_timestamp=data_timestamp_str,
                primary_data_source_impl=primary_data_source_impl,
            )
            # Forward the parsed yaml's resolved data_timestamp /
            # execution_timestamp to the impl when the yaml exposes them
            # (ContractYaml does; the base CheckCollectionYaml in POC scope
            # does not yet). Fall back to the boundary-parsed datetime
            # otherwise — ``CheckCollectionImpl.__init__`` types
            # ``data_timestamp`` as ``Optional[datetime]``, so the fallback
            # must never be a string.
            yaml_data_timestamp = getattr(yaml, "data_timestamp", None)
            yaml_execution_timestamp = getattr(yaml, "execution_timestamp", None)
            impl = impl_class(
                yaml=yaml,
                data_source_impl=data_source_impl,
                soda_cloud_impl=soda_cloud_impl,
                publish_results=publish_results,
                only_validate_without_execute=only_validate_without_execute,
                check_selectors=check_selectors,
                all_data_source_impls=all_data_source_impls,
                dwh_files=dwh_files,
                logs=logs,
                data_timestamp=yaml_data_timestamp if yaml_data_timestamp is not None else parsed_data_timestamp,
                execution_timestamp=yaml_execution_timestamp,
                defer_upload=impl_class.combine_uploads,
            )
            results.append(impl.verify())
            result_impl_classes.append(impl_class)
        except Exception as exc:
            if abort_on_first_error:
                raise
            # On unknown kind or pre-impl failures (where ``impl_class``
            # could not be resolved), fall back to the caller-supplied
            # ``default_impl_class``. Callers that publish a subtype-typed
            # return (e.g. ``list[ContractVerificationResult]``) pass their
            # subtype here so the ERROR placeholder's ``result_class``
            # matches the declared return; callers without a sensible
            # subtype default (or that genuinely want base
            # ``CheckCollectionResult``) leave ``default_impl_class=None``
            # and the universal base is used.
            builder = (
                impl_class
                if impl_class is not None
                else (default_impl_class if default_impl_class is not None else CheckCollectionImpl)
            )
            results.append(builder.build_error_result(yaml_source, exc))
            # impl_class may still be unresolved here (kind dispatch
            # failed before reaching the impl class lookup). Whatever the
            # state, we don't include ERROR placeholders in combined upload
            # groups — they have no soda_cloud_file_id and no real checks
            # to contribute. Track impl_class so callers (or tests) that
            # care can still see which subtype handled the failure.
            result_impl_classes.append(impl_class)

    # Combined-upload pass: group results whose impl class opted into
    # combine_uploads. ERROR-status placeholders and results with no
    # soda_cloud_file_id are skipped — they have nothing to send. The
    # session-level upload runs after every file has been verified so the
    # batch reflects the full session in one Soda Cloud request.
    if soda_cloud_impl is not None and publish_results:
        combined_by_wire_source: dict[str, list[CheckCollectionResult]] = {}
        combined_suffix_by_wire_source: dict[str, Optional[str]] = {}
        for result, result_impl_class in zip(results, result_impl_classes):
            if result_impl_class is None or not result_impl_class.combine_uploads:
                continue
            if result.sending_results_to_soda_cloud_failed:
                # Per-file alignment guard or data-source-missing path
                # already flagged the result; don't include it in the
                # combined upload.
                continue
            file_id = (
                result.check_collection.source.soda_cloud_file_id
                if result.check_collection and result.check_collection.source
                else None
            )
            if file_id is None:
                continue
            combined_by_wire_source.setdefault(result_impl_class.wire_source, []).append(result)
            combined_suffix_by_wire_source[result_impl_class.wire_source] = result_impl_class.scan_definition_suffix

        for wire_source, group in combined_by_wire_source.items():
            soda_cloud_impl.send_combined_results(
                results=group,
                wire_source=wire_source,
                scan_definition_suffix=combined_suffix_by_wire_source.get(wire_source),
            )

    return CheckCollectionSessionResult(results)


def _parse_data_timestamp(value: Optional[Union[str, datetime]]) -> Optional[datetime]:
    """Coerce a ``data_timestamp`` value to ``Optional[datetime]``.

    Public callers (``ContractVerificationSession.execute``) pass an
    ISO-8601 string; programmatic callers (universal session, future
    launchers) pass a ``datetime``. We parse once here and keep
    ``datetime`` end-to-end below this boundary.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return convert_str_to_datetime(value)
