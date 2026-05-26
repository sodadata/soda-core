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
from soda_core.common.exceptions import InvalidArgumentException
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
    ``(session, wire_source)`` pair. Subtypes with ``combine_uploads = True``
    issue one combined request per session covering all their files;
    subtypes with the default ``combine_uploads = False`` upload once per
    file inside ``verify()`` itself. The backend cannot ingest a single
    upload that mixes checks from different wire sources — its ingestion
    filter routes the whole batch by the top-level ``source`` and rejects
    the request if any check inside disagrees. Mixed-source items in one
    ``execute_check_collections`` call are therefore safe (each wire-source
    group uploads independently); a single item must never emit checks
    whose ``source`` disagrees with its own ``wire_source``.

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

    # ---- Phase 1: parse + construct every impl, per-file isolated. ----
    # Entries: ``(impl, impl_class, None, yaml_source)`` on success,
    # ``(None, impl_class_or_None, exc, yaml_source)`` on failure. The
    # ``yaml_source`` slot lets phase 1.5 filter ``constructed`` without
    # index-coupling to ``yaml_sources``. Construct failures flow to
    # phase 2 as ERROR placeholders; ``abort_on_first_error`` still
    # re-raises immediately.
    constructed: list[
        tuple[
            Optional[CheckCollectionImpl],
            Optional[type[CheckCollectionImpl]],
            Optional[BaseException],
            CheckCollectionYamlSource,
        ]
    ] = []
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
            # Forward the yaml's resolved data_timestamp / execution_timestamp
            # to the impl when the yaml exposes them (ContractYaml does; the
            # base CheckCollectionYaml in POC scope does not yet). Fall back
            # to the boundary-parsed datetime otherwise — CheckCollectionImpl
            # types data_timestamp as Optional[datetime], so the fallback must
            # never be a string.
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
            )
            constructed.append((impl, impl_class, None, yaml_source))
        except Exception as exc:
            if abort_on_first_error:
                raise
            constructed.append((None, impl_class, exc, yaml_source))

    # ---- Phase 1.5: session-wide invariant — unique collection_id. ----
    # For ``combine_uploads = True`` subtypes, the pair
    # ``(wire_source, collection_id)`` MUST be unique across the session:
    # duplicates would emit colliding ``checkPath`` prefixes and identity
    # hashes into the combined upload, and the backend would resolve them
    # to the same entry. Hard raise before phase 2 — independent of
    # ``abort_on_first_error`` since this is a session-level user-input
    # error, not a per-file engine failure. Phase 1 failures (impl is
    # None) are skipped here; they surface as phase 2 ERROR placeholders
    # if not pre-empted by a dup raise.
    collisions: dict[tuple[str, str], list[CheckCollectionYamlSource]] = {}
    for impl, impl_class, _construct_exc, yaml_source in constructed:
        if impl is None or impl_class is None or not impl_class.combine_uploads:
            continue
        if not impl.collection_id:
            continue
        collisions.setdefault((impl_class.wire_source, impl.collection_id), []).append(yaml_source)

    dup_lines: list[str] = []
    for (wire_source, collection_id), sources_in_group in collisions.items():
        if len(sources_in_group) < 2:
            continue
        paths = [getattr(src, "file_path", None) or repr(src) for src in sources_in_group]
        dup_lines.append(f"  - {wire_source} '{collection_id}': {', '.join(paths)}")
    if dup_lines:
        raise InvalidArgumentException(
            "Duplicate collection identifier(s) detected in session — each "
            "combine-upload subtype requires a unique identifier per file:\n"
            + "\n".join(dup_lines)
        )

    # ---- Phase 2: verify every constructed impl, per-file isolated. ----
    # Construct-failure placeholders from phase 1 become ERROR results.
    # Verify-time exceptions follow the existing isolation semantics.
    for impl, impl_class, construct_exc, yaml_source in constructed:
        if impl is None:
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
            results.append(builder.build_error_result(yaml_source, construct_exc))
            continue
        try:
            results.append(impl.verify())
        except Exception as exc:
            if abort_on_first_error:
                raise
            results.append(impl_class.build_error_result(yaml_source, exc))

    # ---- Phase 3: combined-upload pass for ``combine_uploads`` subtypes. ----
    # ERROR placeholders and alignment-guard-failed results are skipped from
    # the upload itself, but post-processing handlers still run for them with
    # ``response_json=None`` — matching the per-file path's "handlers run
    # regardless of upload success" semantics. ``constructed`` is positionally
    # aligned with ``results``, so we iterate both together and read the
    # impl_class straight from the construct tuple.
    if soda_cloud_impl is not None and publish_results:
        combined_by_wire_source: dict[str, list[CheckCollectionResult]] = {}
        combined_suffix_by_wire_source: dict[str, Optional[str]] = {}
        # Track which results made it into the upload group so handlers
        # below can pick the right response_json (or None for skipped files).
        uploaded_ids: set[int] = set()
        for (_, impl_class, _, _), result in zip(constructed, results):
            if impl_class is None or not impl_class.combine_uploads:
                continue
            # Per-file alignment guard or data-source-missing path already
            # flagged this result; don't include it in the combined upload.
            if result.sending_results_to_soda_cloud_failed:
                continue
            file_id = (
                result.check_collection.source.soda_cloud_file_id
                if result.check_collection and result.check_collection.source
                else None
            )
            if file_id is None:
                continue
            combined_by_wire_source.setdefault(impl_class.wire_source, []).append(result)
            combined_suffix_by_wire_source[impl_class.wire_source] = impl_class.scan_definition_suffix
            uploaded_ids.add(id(result))

        # Send combined uploads, capture per-wire-source response so each
        # file's handlers see the shared scan_id + response_json.
        response_json_by_wire_source: dict[str, Optional[dict]] = {}
        for wire_source, group in combined_by_wire_source.items():
            response_json_by_wire_source[wire_source] = soda_cloud_impl.send_combined_results(
                results=group,
                wire_source=wire_source,
                scan_definition_suffix=combined_suffix_by_wire_source.get(wire_source),
            )

        # Run post-processing handlers for every combine-subtype file (the
        # per-file ``verify()`` deferred this for combine_uploads=True).
        # Skipped files get response_json=None — same as the per-file path
        # when its upload returns None.
        for (impl, impl_class, _, _), result in zip(constructed, results):
            if impl is None or impl_class is None or not impl_class.combine_uploads:
                continue
            response_for_file = (
                response_json_by_wire_source.get(impl_class.wire_source)
                if id(result) in uploaded_ids
                else None
            )
            impl.run_post_processing_handlers(result, response_for_file)

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
