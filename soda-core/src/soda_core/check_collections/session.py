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
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.exceptions import InvalidArgumentException
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import CheckCollectionYamlSource
from soda_core.contracts.impl.diagnostics_warehouse_files import (
    DiagnosticsWarehouseFiles,
)

logger = soda_logger


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
    expected_kinds: Optional[set[str]] = None,
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

    ``expected_kinds`` is an opt-in set of permitted top-level ``kind:``
    values. When set, the executor reads each yaml's ``kind:`` in phase 1
    and — if a yaml declares a kind outside the set — collects it and
    raises ``InvalidArgumentException`` at the end of phase 1 (before any
    impl is verified). Subtype-narrowed public APIs
    (``verify_data_standards``, future ``verify_reconciliations``, ...)
    pass their own ``{kind}`` so dispatch matches the function name's
    promise and per-yaml ``kind:`` is read exactly once (the same parse
    that drives ``CheckCollectionImpl.for_kind(...)``). The universal
    entrypoint leaves it ``None`` and accepts every registered kind.
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
    # Offenders for ``expected_kinds`` rejection — collected during the
    # phase 1 kind-dispatch read and raised eagerly after the loop so we
    # never run any verify() for a wrong-kind session.
    kind_offenders: list[tuple[CheckCollectionYamlSource, Optional[str]]] = []
    for yaml_source in yaml_sources:
        impl_class: Optional[type[CheckCollectionImpl]] = None
        try:
            # Parse the YAML once for kind dispatch; reuse the parsed
            # object inside the subtype's ``yaml_class.parse(...)`` so the
            # subtype's __init__ doesn't re-parse the same source.
            # ``YamlSource.parse()`` either returns a ``YamlObject`` or
            # raises; never ``None``.
            yaml_object = yaml_source.parse()
            # ``raw_kind`` may be ``None`` if the yaml omits the ``kind:``
            # field — we keep it raw for the offender report (so the
            # message reads ``kind=None`` instead of misleadingly saying
            # ``kind='contract'`` for a file that had no kind at all).
            # The ``"contract"`` default is only applied for impl-class
            # dispatch — BC for legacy contract YAMLs without a kind line.
            raw_kind = yaml_object.read_string_opt("kind")
            if expected_kinds is not None and raw_kind not in expected_kinds:
                kind_offenders.append((yaml_source, raw_kind))
                continue
            impl_class = CheckCollectionImpl.for_kind(raw_kind or "contract")

            yaml = impl_class.yaml_class.parse(
                yaml_source=yaml_source,
                yaml_object=yaml_object,
                provided_variable_values=variables,
                data_timestamp=data_timestamp_str,
                primary_data_source_impl=primary_data_source_impl,
            )
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
                # ``data_timestamp`` and ``execution_timestamp`` are
                # first-class fields on ``CheckCollectionYaml``: every
                # subtype yaml has them after construction.
                data_timestamp=yaml.data_timestamp,
                execution_timestamp=yaml.execution_timestamp,
            )
            constructed.append((impl, impl_class, None, yaml_source))
        except Exception as exc:
            if abort_on_first_error:
                raise
            constructed.append((None, impl_class, exc, yaml_source))

    # ---- Session-wide invariants — raise before any verify() runs. ----
    # Both checks are user-input errors at the session boundary, so they
    # ignore ``abort_on_first_error`` (which gates per-file engine errors).
    _raise_if_kind_offenders(kind_offenders, expected_kinds)
    _raise_if_duplicate_collection_ids(constructed)

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

    # ---- Phase 3: combined upload (cloud-gated) + post-processing handlers (always). ----
    # ``constructed`` is positionally aligned with ``results``, so we iterate
    # both together and read the impl_class straight from the construct tuple.
    # Two distinct concerns split by their gating:
    #   - The combined upload runs only when there's a cloud client AND the
    #     caller opted into publish_results — otherwise there's nothing to send.
    #   - Post-processing handlers run for every combine-upload result regardless
    #     of cloud presence, mirroring the non-combine path's "handlers run
    #     unconditionally at the end of verify()" semantics. Handlers receive
    #     the shared response_json when the result was actually uploaded, or
    #     None otherwise (alignment-guard failure, missing file_id, cloud absent,
    #     publish_results=False).
    response_json_by_wire_source: dict[str, Optional[dict]] = {}
    uploaded_ids: set[int] = set()
    if soda_cloud_impl is not None and publish_results:
        combined_by_wire_source: dict[str, list[CheckCollectionResult]] = {}
        combined_suffix_by_wire_source: dict[str, Optional[str]] = {}
        # A runner-created scan id is the precondition for reporting FAILED (it's what
        # mark_scan_as_failed needs). Ad-hoc runs have no scan id, so for those we keep the
        # normal upload behavior and don't special-case errored results.
        soda_scan_id: Optional[str] = EnvConfigHelper().soda_scan_id
        # First collection that errored before producing any check results (e.g. the data
        # source connection failed). Such a result must not be sent as results — see the
        # per-file handling in CheckCollectionImpl.verify(). Held so that, if the whole scan
        # produced nothing to send, we report it as FAILED. Keep the first match so the logs
        # used are deterministic when several collections error.
        errored_without_results_result: Optional[CheckCollectionResult] = None
        for (_, impl_class, _, _), result in zip(constructed, results):
            if impl_class is None or not impl_class.combine_uploads:
                continue
            # Per-file alignment guard or data-source-missing path already
            # flagged this result; don't include it in the combined upload.
            if result.sending_results_to_soda_cloud_failed:
                continue
            if soda_scan_id and result.errored_without_results:
                if errored_without_results_result is None:
                    errored_without_results_result = result
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

        for wire_source, group in combined_by_wire_source.items():
            response_json_by_wire_source[wire_source] = soda_cloud_impl.send_check_collection_results(
                results=group,
                wire_source=wire_source,
                scan_definition_suffix=combined_suffix_by_wire_source.get(wire_source),
            )

        # Report the (still-PENDING) scan as FAILED only when the whole scan produced nothing
        # to send and at least one collection errored before producing results. Guarded so we
        # never override a sibling result that already uploaded (scan_id stamped) or mask a
        # genuine send failure (sending_results_to_soda_cloud_failed) — those must keep their
        # own outcome. Sending an errored, result-less batch instead would make Cloud attempt
        # PENDING -> COMPLETED_WITH_ERRORS, which the scan-state machine rejects; PENDING ->
        # FAILED is allowed.
        if (
            soda_scan_id
            and errored_without_results_result is not None
            and not combined_by_wire_source
            and not any(r.scan_id for r in results)
            and not any(r.sending_results_to_soda_cloud_failed for r in results)
        ):
            # Stamp the known scan id (so post-processing failure reporting can update Cloud)
            # and pass it explicitly. Forward any stored exception too: executor placeholders
            # (build_error_result) carry result.error but log_records=None, so without this the
            # scan would be marked FAILED in Cloud with an empty, undiagnosable log payload.
            errored_without_results_result.scan_id = soda_scan_id
            soda_cloud_impl.mark_scan_as_failed(
                scan_id=soda_scan_id,
                logs=errored_without_results_result.log_records,
                exc=errored_without_results_result.error,
            )

    # Post-processing handlers — combine-upload subtypes. The non-combine path
    # runs handlers inline per file inside ``verify()``; combine-upload results are
    # post-processed HERE, after the single combined upload, so handlers see the
    # shared scan_id / response_json. Handlers run ONCE per wire-source group via
    # ``handle_session`` (not once per file): this lets a session-scoped handler
    # (e.g. the diagnostics-warehouse extractor) fetch config once per dataset,
    # reuse one connection, and post a single aggregated stage update. ERROR
    # placeholders are skipped for parity with the non-combine path (an exception
    # in verify() bypasses handlers). Each item carries the shared response when it
    # was part of the upload, else None, so the default per-item ``handle_session``
    # preserves the old "run handlers regardless of upload success" semantics.
    from soda_core.contracts.impl.contract_verification_impl import (
        ContractVerificationHandlerRegistry,
        PostProcessingSessionItem,
    )

    session_items_by_wire_source: dict[str, list[PostProcessingSessionItem]] = {}
    for (impl, impl_class, _, _), result in zip(constructed, results):
        if impl is None or impl_class is None or not impl_class.combine_uploads:
            continue
        if result.error is not None:
            continue
        response_for_file = (
            response_json_by_wire_source.get(impl_class.wire_source) if id(result) in uploaded_ids else None
        )
        session_items_by_wire_source.setdefault(impl_class.wire_source, []).append(
            PostProcessingSessionItem(
                contract_impl=impl,
                verification_result=result,
                soda_cloud_send_results_response_json=response_for_file,
            )
        )

    for wire_source, session_items in session_items_by_wire_source.items():
        group_response_json = response_json_by_wire_source.get(wire_source)
        for handler in ContractVerificationHandlerRegistry.contract_verification_handlers:
            try:
                handler.handle_session(
                    items=session_items,
                    soda_cloud=soda_cloud_impl,
                    soda_cloud_send_results_response_json=group_response_json,
                    dwh_files=dwh_files,
                )
            except Exception as e:
                logger.error(
                    f"Error in session post-processing handler {type(handler).__name__}: {e}",
                    exc_info=True,
                )
                # Backstop: a well-behaved handle_session isolates per item and posts its own
                # terminal stage state. If the override escapes anyway, mark this handler's
                # stages FAILED per file — mirroring the per-file path's
                # run_post_processing_handlers, so a crashing session handler never leaves a
                # post-processing stage stuck/unreported. (No-ops when scan_id/cloud is absent.)
                for item in session_items:
                    item.contract_impl._handle_post_processing_failure(
                        scan_id=item.verification_result.scan_id,
                        exc=e,
                        contract_verification_handler=handler,
                    )
        # Re-stamp the per-file thread label on log records (incl. handler
        # emissions) — mirrors run_post_processing_handlers' final relabel pass,
        # now done once per file after all session handlers have run, so the
        # Cloud-side wire grouping by ``thread`` stays correct.
        for item in session_items:
            item.contract_impl._apply_thread_label_to_log_records(item.verification_result.log_records)

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


def _raise_if_kind_offenders(
    kind_offenders: list[tuple[CheckCollectionYamlSource, Optional[str]]],
    expected_kinds: Optional[set[str]],
) -> None:
    """Raise ``InvalidArgumentException`` listing every yaml whose ``kind:``
    fell outside ``expected_kinds``.

    Offenders are collected inline during phase 1's kind-dispatch parse
    (single parse per yaml) and handed here for the raise. Subtype-narrowed
    APIs (``verify_data_standards``, future ``verify_reconciliations``, ...)
    rely on this to enforce their function-name promise.
    """
    if not kind_offenders:
        return
    expected_str = ", ".join(repr(k) for k in sorted(expected_kinds or ()))
    offender_str = ", ".join(
        f"{getattr(src, 'file_path', None) or repr(src)} (kind={kind!r})" for src, kind in kind_offenders
    )
    raise InvalidArgumentException(
        f"Every yaml must declare 'kind:' in {{{expected_str}}} at the root. "
        f"Offending file(s): {offender_str}. "
        "Use execute_check_collections directly to verify mixed kinds."
    )


def _raise_if_duplicate_collection_ids(
    constructed: list[
        tuple[
            Optional[CheckCollectionImpl],
            Optional[type[CheckCollectionImpl]],
            Optional[BaseException],
            CheckCollectionYamlSource,
        ]
    ],
) -> None:
    """Raise ``InvalidArgumentException`` if two constructed impls in a
    ``combine_uploads = True`` subtype share ``(wire_source, collection_id)``.

    Duplicates would emit colliding ``checkPath`` prefixes and identity
    hashes into the combined upload, and the backend would resolve them to
    the same entry. Phase 1 failures (impl is None) and impls without a
    ``collection_id`` are skipped; they don't participate in the dedup.
    """
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
            "combine-upload subtype requires a unique identifier per file:\n" + "\n".join(dup_lines)
        )
