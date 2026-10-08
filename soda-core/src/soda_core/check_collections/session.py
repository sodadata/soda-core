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

import dataclasses
from datetime import datetime
from logging import LogRecord
from typing import Optional, Union

from soda_core.check_collections.base import (
    CheckCollectionImpl,
    CheckCollectionResult,
    CheckCollectionSessionResult,
    describe_construct_failure,
)
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.datetime_conversions import convert_datetime_to_str, convert_str_to_datetime
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.exceptions import InvalidArgumentException
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Logs, preserve_active_logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import CheckCollectionYamlSource
from soda_core.contracts.contract_verification import CheckCollectionStatus, CheckOutcome
from soda_core.contracts.impl.check_selector import value_matches
from soda_core.contracts.impl.diagnostics_warehouse_files import DiagnosticsWarehouseFiles
from soda_core.contracts.impl.scope import BASE_SCOPE_KEY

logger = soda_logger


# Each owned impl's Logs activates on construction and is never individually
# closed, so restore the active capture target to its pre-call value on exit
# (no Logs left dangling as active after the session).
@preserve_active_logs()
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

    On a run that publishes, a combined upload never goes up clean after an
    error. A file that errored before it had check results goes up with the
    others, so the upload has errors; a file that never became a collection
    rides along after them and never leads the upload. When a file of a managed
    run's group errored before its check results and the files that can go up
    evaluated no check, the scan is marked failed instead of uploading excluded
    checks next to the error. A file that cannot be sent,
    such as one whose file upload Soda Cloud rejected, stays out, and every
    upload of the session carries an error record naming it instead, so it
    has errors. Its result is flagged as not sent, so the CLI exits
    RESULTS_NOT_SENT_TO_CLOUD. When nothing went up, a managed scan is marked
    failed once, with every file's records, since the launcher commands that
    verify do not mark it on that exit code. The combined uploads never mark a
    scan after an insert that reached it, or may have: a 5xx or a timeout can
    follow an insert Soda Cloud stored, and a mark would turn that scan FAILED
    and replace its logs. Per-file collections decide in their own
    ``verify()`` and follow the same rule: in a session of several of them under
    one scan id, a file that cannot go up after an earlier file reached the scan
    is flagged as not sent and marks nothing. A session where every file
    succeeds uploads exactly as before.

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

    ``logs`` is the caller's capture target, e.g. the CLI failure boundary's.
    With more than one yaml, each file gets a child of it (``Logs.child()``)
    with its own records, thread label and error count, so one file's errors
    never set another file's status or reach its upload; the caller's ``logs``
    still sees every record once. Each child starts empty, so what ``logs`` held
    before the session sets no file's status. The session keeps those records
    once and adds them to every upload and failure mark it or a file sends,
    ahead of the upload's own records. A single yaml uses ``logs`` itself.
    Without ``logs``, each impl builds its own.

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
    # Each impl's ``Logs`` becomes active as it is constructed, capturing its
    # construction logs; phase 2/3 re-activate it around verify()/handlers.
    # Only one gatherer is ever active, so siblings can't cross-capture.
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
    # Siblings sharing the caller's Logs would share its error count, so each file
    # of a multi-file session gets a child of it instead. The child is built before
    # the parse, so the file's parse records land in it too. What the caller logged
    # before the session belongs to no one file: the session keeps it once and every
    # upload and failure mark carries it.
    child_logs_per_file: bool = logs is not None and len(yaml_sources) > 1
    pre_session_records: list[LogRecord] = list(logs.get_log_records()) if child_logs_per_file else []
    for yaml_source in yaml_sources:
        impl_class: Optional[type[CheckCollectionImpl]] = None
        impl_logs: Optional[Logs] = logs.child() if child_logs_per_file else logs
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
                logs=impl_logs,
                # ``data_timestamp`` and ``execution_timestamp`` are
                # first-class fields on ``CheckCollectionYaml``: every
                # subtype yaml has them after construction.
                data_timestamp=yaml.data_timestamp,
                execution_timestamp=yaml.execution_timestamp,
            )
            impl.session_log_records = tuple(pre_session_records)
            constructed.append((impl, impl_class, None, yaml_source))
        except Exception as exc:
            if abort_on_first_error:
                # Re-raise verbatim, without touching Cloud: the CLI failure boundary
                # (``scan.run_scan``) owns the single mark-scan-failed —
                # a session-level mark here would duplicate it.
                raise
            constructed.append((None, impl_class, exc, yaml_source))

    # ---- Session-wide invariants — raise before any verify() runs. ----
    # These checks are user-input errors at the session boundary, so they
    # ignore ``abort_on_first_error`` (which gates per-file engine errors).
    _raise_if_kind_offenders(kind_offenders, expected_kinds)
    _raise_if_duplicate_collection_ids(constructed)
    _raise_if_combined_session_spans_multiple_datasets(constructed)
    raise_if_unknown_scope_keys(constructed, check_selectors)

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
        # A per-file collection marks its own scan in verify(), so it needs to know whether an
        # earlier file already reached it.
        impl.scan_reached_by_an_earlier_file = any(
            result.scan_id or result.results_may_have_reached_soda_cloud for result in results
        )
        # Capture this verify()'s records into this collection's gatherer.
        with impl.logs.activate(impl.thread_label):
            try:
                results.append(impl.verify())
            except Exception as exc:
                if abort_on_first_error:
                    # Re-raise verbatim, without touching Cloud (see the phase-1 abort above).
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
    #     None otherwise: a file left out of the upload, a group with nothing to
    #     send, a managed scan marked failed, cloud absent or
    #     publish_results=False.
    response_json_by_wire_source: dict[str, Optional[dict]] = {}
    uploaded_ids: set[int] = set()
    if soda_cloud_impl is not None and publish_results:
        # A runner-created scan id is the precondition for reporting FAILED (it's what
        # mark_scan_as_failed needs). An ad-hoc run has no scan to mark, so its upload
        # creates the scan and carries the errors.
        soda_scan_id: Optional[str] = EnvConfigHelper().soda_scan_id
        groups, unplaced_results = _group_combine_upload_results(constructed, results, default_impl_class)
        # Files of unknown kind that no single combined upload can claim: none of the uploads
        # holds them, so they are flagged as not sent.
        for result in unplaced_results:
            result.sending_results_to_soda_cloud_failed = True
        uploads_by_wire_source: dict[str, list[CheckCollectionResult]] = {}
        suffix_by_wire_source: dict[str, Optional[str]] = {}
        # The results whose own data goes up in no upload. Each upload carries a stand-in for
        # each of them, with its records and an error naming it, so no upload reads clean.
        results_left_out: list[CheckCollectionResult] = list(unplaced_results)
        # Every result of a managed run's group that errored and evaluated no check. An upload
        # of it could only hold excluded checks next to the error, so the scan is marked
        # failed instead, below.
        results_to_mark_failed: list[CheckCollectionResult] = []
        for wire_source, group_members in groups.items():
            members, unsendable_results = _split_unsendable(group_members)
            results_left_out.extend(unsendable_results)
            member_results: list[CheckCollectionResult] = [result for _, _, result in members]
            if soda_scan_id and _errored_without_evaluating_a_check(member_results, left_out=unsendable_results):
                results_to_mark_failed.extend(member_results)
                continue
            # Every collection goes up, so the upload has errors when one of them errored, and
            # carries its records. A file that never became a collection has no dataset, data
            # source or file of its own: it rides along after the collections and never leads
            # the upload, whose first result names the scan.
            upload: list[CheckCollectionResult] = [r for r in member_results if r.error is None] + [
                r for r in member_results if r.error is not None
            ]
            if not upload or upload[0].error is not None:
                # Nothing to build a scan from. An ad-hoc run has no scan to mark either: the
                # errors are on the console and the run exits LOG_ERRORS, as when it fails before
                # it has results, or RESULTS_NOT_SENT_TO_CLOUD when a file could not be sent.
                if unsendable_results:
                    for result in member_results:
                        result.sending_results_to_soda_cloud_failed = True
                continue
            uploads_by_wire_source[wire_source] = upload
            suffix_by_wire_source[wire_source] = next(
                member_class.scan_definition_suffix
                for member_class, _, result in reversed(members)
                if result.error is None
            )

        if uploads_by_wire_source and results_to_mark_failed:
            # Another group goes up, so a mark for this one would land on a scan that insert
            # completed. Its records ride along in that upload instead, and its results are
            # flagged as not sent.
            for result in results_to_mark_failed:
                result.sending_results_to_soda_cloud_failed = True
            results_left_out.extend(results_to_mark_failed)
            results_to_mark_failed = []
        stand_ins: list[CheckCollectionResult] = [_left_out_stand_in(result) for result in results_left_out]

        for wire_source, upload in uploads_by_wire_source.items():
            response_json_by_wire_source[wire_source] = soda_cloud_impl.send_check_collection_results(
                results=upload + stand_ins,
                wire_source=wire_source,
                scan_definition_suffix=suffix_by_wire_source[wire_source],
                session_log_records=pre_session_records,
            )
            uploaded_ids.update(id(result) for result in upload)

        if soda_scan_id:
            _mark_scan_failed(
                combined_results=[result for members in groups.values() for _, _, result in members] + unplaced_results,
                results_to_mark_failed=results_to_mark_failed,
                all_results=results,
                soda_cloud_impl=soda_cloud_impl,
                soda_scan_id=soda_scan_id,
                session_log_records=pre_session_records,
            )

    # Post-processing handlers — combine-upload subtypes. The non-combine path
    # runs handlers inline per file inside ``verify()``; combine-upload results are
    # post-processed HERE, after the single combined upload, so handlers see the
    # shared scan_id / response_json. Handlers run ONCE per wire-source group via
    # ``handle_session`` (not once per file): this lets a session-scoped handler
    # do session-level work once (e.g. resolve shared config, reuse one connection,
    # post a single aggregated stage update) instead of repeating it per file. ERROR
    # placeholders are skipped for parity with the non-combine path (an exception
    # in verify() bypasses handlers). Each item carries the shared response when it
    # was part of the upload, else None, so the default per-item ``handle_session``
    # preserves the old "run handlers regardless of upload success" semantics.
    # Log attribution: the default per-item ``handle_session`` activates each
    # item's ``Logs``, so handler emissions are captured for — and ``thread``-
    # labelled as — the emitting file at emit time. A session-scoped override's
    # emissions span files and are not attributed to any single one.
    from soda_core.contracts.impl.contract_verification_impl import (
        PostProcessingSessionItem,
        post_processing_handlers_for_current_scan,
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
        for handler in post_processing_handlers_for_current_scan():
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
                # terminal stage state (the documented overrider contract). If the override
                # escapes anyway, mark this handler's stages FAILED per file — mirroring the
                # per-file path's run_post_processing_handlers, so a crashing session handler
                # never leaves a post-processing stage stuck/unreported. (No-ops when
                # scan_id/cloud is absent.)
                #
                # Intentionally conservative: this fires only on a contract violation (an
                # escaping override). If such an override had already posted COMPLETED for some
                # items before raising, those get re-marked FAILED here. We accept over-reporting
                # FAILED over leaving a stage hung — the alternative (tracking which items already
                # reached a terminal stage) lives inside the handler, not the executor. An override
                # that honors the documented contract (isolate per item; always post a terminal
                # stage in ``finally``) never escapes, so this path is unreached in practice.
                for item in session_items:
                    item.contract_impl._handle_post_processing_failure(
                        scan_id=item.verification_result.scan_id,
                        exc=e,
                        contract_verification_handler=handler,
                    )

    return CheckCollectionSessionResult(results, session_log_records=pre_session_records)


_CombineUploadMember = tuple[type[CheckCollectionImpl], Optional[CheckCollectionImpl], CheckCollectionResult]


def _group_combine_upload_results(
    constructed: list[
        tuple[
            Optional[CheckCollectionImpl],
            Optional[type[CheckCollectionImpl]],
            Optional[BaseException],
            CheckCollectionYamlSource,
        ]
    ],
    results: list[CheckCollectionResult],
    default_impl_class: Optional[type[CheckCollectionImpl]],
) -> tuple[dict[str, list[_CombineUploadMember]], list[CheckCollectionResult]]:
    """The results of combine-upload subtypes by wire source, in session order, and the
    results of files that belong to no group but should have.

    A file that failed before its kind was known belongs to the caller's
    ``default_impl_class``, the subtype the session verifies, so its error still
    reaches Soda Cloud with the group. Without a default subtype it joins the
    session's combined upload when there is only one. Next to several it could belong
    to any of them, so it is returned apart: no upload can claim every file.
    """
    member_classes: list[Optional[type[CheckCollectionImpl]]] = [
        impl_class if impl_class is not None else default_impl_class for _, impl_class, _, _ in constructed
    ]
    combine_upload_classes: dict[str, type[CheckCollectionImpl]] = {
        member_class.wire_source: member_class
        for member_class in member_classes
        if member_class is not None and member_class.combine_uploads
    }
    unknown_kind_class: Optional[type[CheckCollectionImpl]] = (
        next(iter(combine_upload_classes.values())) if len(combine_upload_classes) == 1 else None
    )
    groups: dict[str, list[_CombineUploadMember]] = {}
    unplaced_results: list[CheckCollectionResult] = []
    for (impl, _, _, _), member_class, result in zip(constructed, member_classes, results):
        if member_class is None:
            if unknown_kind_class is None:
                if combine_upload_classes:
                    unplaced_results.append(result)
                continue
            member_class = unknown_kind_class
        if not member_class.combine_uploads:
            continue
        groups.setdefault(member_class.wire_source, []).append((member_class, impl, result))
    return groups, unplaced_results


def _split_unsendable(
    members: list[_CombineUploadMember],
) -> tuple[list[_CombineUploadMember], list[CheckCollectionResult]]:
    """The members of the group that can go up in its combined upload, and the results
    of the ones that cannot, flagged as not sent.

    A result cannot be sent when the alignment guard, a missing data source or a
    rejected file upload already flagged it, or when it became a collection but has
    no file on Soda Cloud. It stays out of the upload. The others still go up, with a
    stand-in for it that gives the upload errors and names it, and the run exits
    RESULTS_NOT_SENT_TO_CLOUD for it.
    """
    sendable: list[_CombineUploadMember] = []
    unsendable_results: list[CheckCollectionResult] = []
    for member in members:
        _, impl, result = member
        if result.sending_results_to_soda_cloud_failed:
            unsendable_results.append(result)
        elif result.error is None and not _soda_cloud_file_id(result):
            if impl is not None:
                with impl.logs.activate(impl.thread_label):
                    logger.error(
                        f"Not sending results to Soda Cloud {Emoticons.CROSS_MARK} "
                        f"The {impl.display_name} file did not upload to Soda Cloud."
                    )
            result.sending_results_to_soda_cloud_failed = True
            unsendable_results.append(result)
        else:
            sendable.append(member)
    return sendable, unsendable_results


def _left_out_stand_in(result: CheckCollectionResult) -> CheckCollectionResult:
    """What an upload carries for a result whose own data it leaves out: an ERROR copy
    of it with no checks, measurements or stages, whose records end with an error that
    names its file. The upload has errors, says what is missing, and never reads as a
    clean run. The result itself is left as it was."""
    source = result.check_collection.source if result.check_collection else None
    file_description: str = (
        (source.local_file_path if source else None)
        or (result.check_collection.soda_qualified_dataset_name if result.check_collection else None)
        or "a file of this run"
    )
    # Captured into its own Logs, the way build_error_result does, so the record reaches no
    # other file's records. The console still shows it.
    with preserve_active_logs():
        stand_in_logs = Logs()
        logger.error(
            f"Not sending results to Soda Cloud {Emoticons.CROSS_MARK} {file_description} is not part of "
            f"this upload: its results could not be sent to Soda Cloud."
        )
    return dataclasses.replace(
        result,
        status=CheckCollectionStatus.ERROR,
        measurements=[],
        check_results=[],
        measurement_dicts=[],
        token_usage=None,
        post_processing_stages=[],
        dataset_columns=None,
        scan_id=None,
        log_records=[*(result.log_records or []), *stand_in_logs.get_log_records()],
    )


def _soda_cloud_file_id(result: CheckCollectionResult) -> Optional[str]:
    source = result.check_collection.source if result.check_collection else None
    return source.soda_cloud_file_id if source else None


def _errored_without_evaluating_a_check(
    results: list[CheckCollectionResult], left_out: list[CheckCollectionResult] = ()
) -> bool:
    """True when a result of the group errored before it had check results and no result
    that can go up evaluated a check: every check there is was left out by a check filter.
    A result that is ``left_out`` only because it cannot be sent is no such error: the
    others then go up with a stand-in that names it."""
    errored: bool = any(result.errored_without_results for result in [*results, *left_out])
    return errored and not any(
        check_result.outcome != CheckOutcome.EXCLUDED for result in results for check_result in result.check_results
    )


def _mark_scan_failed(
    combined_results: list[CheckCollectionResult],
    results_to_mark_failed: list[CheckCollectionResult],
    all_results: list[CheckCollectionResult],
    soda_cloud_impl: SodaCloud,
    soda_scan_id: str,
    session_log_records: list[LogRecord],
) -> None:
    """Report a managed scan as FAILED, at most once, for the session's combined uploads.

    Never after an insert that reached the scan or may have, a 5xx or a timeout: a mark
    turns the scan FAILED whatever state it is in, replaces its logs and ends it a second
    time. What went up already says it has errors, and the flags make the run exit
    RESULTS_NOT_SENT_TO_CLOUD.

    Otherwise, when a result of ``combined_results`` could not be sent, the scan is marked
    failed with every file's records, so the errors reach Soda Cloud, and the results stay
    flagged: the run exits RESULTS_NOT_SENT_TO_CLOUD. The launcher commands that verify do
    not mark a scan on that exit code, so the session does. A per-file collection marks
    its own scan in ``verify()``.

    Otherwise, a group that errored and evaluated no check is reported by marking the
    scan failed with that group's records.
    """
    if any(r.scan_id for r in all_results) or any(r.results_may_have_reached_soda_cloud for r in all_results):
        return
    if any(result.sending_results_to_soda_cloud_failed for result in combined_results):
        for result in results_to_mark_failed:
            result.sending_results_to_soda_cloud_failed = True
        # A rejected mark changes nothing here: the flags already make the run exit
        # RESULTS_NOT_SENT_TO_CLOUD, and the scan is not marked a second time.
        soda_cloud_impl.mark_scan_as_failed(
            scan_id=soda_scan_id,
            logs=_session_and_result_log_records(session_log_records, all_results),
            exc=next((result.error for result in all_results if result.error is not None), None),
        )
        return
    if not results_to_mark_failed:
        return
    # Every group here holds a result that errored without results. Should that ever change,
    # the default marks with the first result instead of raising StopIteration.
    first_errored: CheckCollectionResult = next(
        (r for r in results_to_mark_failed if r.errored_without_results), results_to_mark_failed[0]
    )
    # Stamp the known scan id, so post-processing failure reporting can update Cloud, and
    # pass it explicitly. Forward the first stored exception too, which a file that never
    # became a collection carries on result.error.
    # NOTE: log_records is [] on a run whose logs stream to Soda Cloud, and this mark
    # REPLACES the scan's stored logs. Move to Logs.records_for_failure_report() before
    # any combine-uploads flow opts into batched ingestion.
    first_errored.scan_id = soda_scan_id
    marked_as_failed: bool = soda_cloud_impl.mark_scan_as_failed(
        scan_id=soda_scan_id,
        logs=_session_and_result_log_records(session_log_records, results_to_mark_failed),
        exc=first_errored.error,
    )
    if not marked_as_failed:
        # A rejected mark leaves the failure invisible on Cloud; surface it as a
        # send failure, so the run exits RESULTS_NOT_SENT_TO_CLOUD.
        first_errored.sending_results_to_soda_cloud_failed = True


def _session_and_result_log_records(
    session_log_records: list[LogRecord], results: list[CheckCollectionResult]
) -> list[LogRecord]:
    """The session's own records, then every result's, in order. No record sits in two
    of them: a file's Logs holds only that file's records."""
    log_records: list[LogRecord] = list(session_log_records)
    for result in results:
        log_records.extend(result.log_records or [])
    return log_records


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


def _raise_if_combined_session_spans_multiple_datasets(
    constructed: list[
        tuple[
            Optional[CheckCollectionImpl],
            Optional[type[CheckCollectionImpl]],
            Optional[BaseException],
            CheckCollectionYamlSource,
        ]
    ],
) -> None:
    """Raise ``InvalidArgumentException`` if a ``combine_uploads = True`` wire source's
    files target more than one dataset.

    A combine-upload session uploads all its files under ONE ``scanId`` (one per wire
    source). Downstream consumers key a scan by ``scan_id`` and link per-check results to
    it by ``scan_id`` alone, assuming one scan maps to exactly one dataset. Letting a single
    ``scanId`` span multiple datasets would break that 1:1 link (e.g. multiple scan rows
    sharing one ``scan_id``, fanned-out joins). A combine-upload subtype is therefore expected
    to cover a single dataset per session; a multi-dataset combined session is rejected here
    rather than producing an incoherent scan. Non-combine subtypes are exempt — each file is
    its own scan. Impls missing a qualified dataset name are skipped.
    """
    # wire_source -> dataset -> [file path], so the error can point at the offending files
    # (mirrors _raise_if_duplicate_collection_ids).
    files_by_dataset: dict[str, dict[str, list[str]]] = {}
    for impl, impl_class, _construct_exc, yaml_source in constructed:
        if impl is None or impl_class is None or not impl_class.combine_uploads:
            continue
        dataset = getattr(impl, "soda_qualified_dataset_name", None)
        if not dataset:
            continue
        path = getattr(yaml_source, "file_path", None) or repr(yaml_source)
        files_by_dataset.setdefault(impl_class.wire_source, {}).setdefault(dataset, []).append(path)

    offenders = {ws: by_ds for ws, by_ds in files_by_dataset.items() if len(by_ds) > 1}
    if offenders:
        lines: list[str] = []
        for wire_source, by_ds in offenders.items():
            lines.append(f"  - {wire_source}:")
            for dataset, paths in sorted(by_ds.items()):
                lines.append(f"      {dataset}: {', '.join(paths)}")
        raise InvalidArgumentException(
            "A combined (combine_uploads) session must target a single dataset — its files share "
            "one scanId, and a scan is assumed to map to exactly one dataset. "
            "Multiple datasets found in a single wire source:\n" + "\n".join(lines) + "\n"
            "Run one dataset per combined session."
        )


def _matches_a_known_scope_key(value: str, known_keys: set[str]) -> bool:
    """Whether a ``scope`` filter value names a known key, or as a pattern matches one, the way a check filter
    matches it."""
    return any(value_matches(key, value) for key in known_keys)


def raise_if_unknown_scope_keys(
    constructed: list[
        tuple[
            Optional[CheckCollectionImpl],
            Optional[type[CheckCollectionImpl]],
            Optional[BaseException],
            CheckCollectionYamlSource,
        ]
    ],
    check_selectors: Optional[list],
) -> None:
    """Raise ``InvalidArgumentException`` if a ``scope`` check filter names a key that
    no collection in the session declares.

    ``base`` is always known. The other known keys are the declared scopes of every
    constructed impl; a file that failed construction declares none. Positive and negated values are checked
    alike, and a value with a ``*`` or ``?`` wildcard must match at least one known key. A collection
    that does not declare a known key needs nothing here: its checks fail the filter
    and go up as EXCLUDED, so one session-level error replaces an error per file.

    Runs after phase 1, which may open connections and fetch the dataset
    configuration, and before any query against a dataset or any upload. When it
    raises, phase 2 never builds the ERROR placeholders that log construct failures,
    so those are logged here first. A caller checking one file before handing its
    filters to a runner passes a one-entry ``constructed`` list.
    """
    scope_values: list[str] = [selector.value for selector in check_selectors or [] if selector.field == "scope"]
    if not scope_values:
        return

    known_keys: set[str] = {BASE_SCOPE_KEY}
    for impl, _impl_class, _construct_exc, _yaml_source in constructed:
        if impl is not None:
            known_keys.update(impl.scopes)

    unknown_keys: list[str] = list(
        dict.fromkeys(value for value in scope_values if not _matches_a_known_scope_key(value, known_keys))
    )
    if not unknown_keys:
        return

    for impl, _impl_class, construct_exc, yaml_source in constructed:
        if impl is None and construct_exc is not None:
            logger.error(describe_construct_failure(construct_exc, yaml_source))

    # A scope is no list, so '[eu,us]' is read as one key. Repeating the filter selects several scopes.
    list_hint: str = (
        " The [a,b] form matches list attributes only. To select several scopes, give one scope filter per key, "
        "as in scope=eu and scope=us."
        if any(key.startswith("[") and key.endswith("]") for key in unknown_keys)
        else ""
    )
    raise InvalidArgumentException(
        f"Unknown scope key(s) in the check filter: {', '.join(repr(key) for key in unknown_keys)}. "
        "No file in this session declares them. "
        f"Known scope keys: {', '.join(repr(key) for key in sorted(known_keys))}.{list_hint}"
    )
