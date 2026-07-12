"""Base classes for check-collection verification.

A check collection is one verifiable YAML file. ``CheckCollectionImpl``
is the engine. Subclasses declare four plain class attributes:

    wire_source:  str  — Cloud upload ``"source"`` literal (e.g. ``"soda-contract"``).
    display_name: str  — User-facing word in logs and errors.
    yaml_class:   type[CheckCollectionYaml]  — YAML parser used by the executor.
    result_class: type[CheckCollectionResult] — Concrete result returned by ``verify()``.

Everything else (parse columns, parse checks, resolve metrics, build queries,
execute, upload to Soda Cloud, run post-processing handlers) is inherited.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from logging import ERROR, WARNING, LogRecord
from numbers import Number
from typing import Any, Optional

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.datetime_conversions import convert_str_to_datetime
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.exceptions import SodaCoreException, get_exception_stacktrace
from soda_core.common.logging_constants import Emoticons, ExtraKeys, soda_logger
from soda_core.common.logs import Location, Logs
from soda_core.common.metadata_types import SamplerType
from soda_core.common.soda_cloud_converter import map_sampler_type_from_dto
from soda_core.common.soda_cloud_dto import DatasetConfigurationDTO
from soda_core.common.sql_dialect import (
    CTE,
    FROM,
    SELECT,
    STAR,
    WHERE,
    DatasetIdentifier,
    SqlExpressionStr,
)
from soda_core.common.yaml import CheckCollectionYamlSource, YamlObject
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    CheckOutcome,
    CheckResult,
    Contract,
    DataSource,
    Measurement,
    PostProcessingStage,
    ScanTokenUsage,
    YamlFileContentInfo,
)
from soda_core.contracts.impl.diagnostics_warehouse_files import (
    DiagnosticsWarehouseFiles,
)

logger: logging.Logger = soda_logger


@dataclass
class CheckCollectionResult:
    """Result of verifying one check-collection file.

    Holds the immutable record of a single file's verification: status,
    measurements, check results, log records, and post-processing stages.
    ``ContractVerificationResult`` is a subclass preserving the historical name.

    ``check_collection`` carries the wire-source-neutral metadata about the
    parsed file (data source name, dataset prefix, qualified name, file
    source info). The field type is still ``Contract`` until the future
    ``Contract`` → ``CheckCollection`` class rename lands; only the field
    name is neutral in this PR.
    """

    check_collection: Contract
    data_source: Optional[DataSource]
    data_timestamp: Optional[datetime]
    started_timestamp: datetime
    ended_timestamp: datetime
    status: CheckCollectionStatus
    measurements: list[Measurement]
    check_results: list[CheckResult]
    sending_results_to_soda_cloud_failed: bool
    log_records: Optional[list[LogRecord]] = None
    post_processing_stages: Optional[list[PostProcessingStage]] = None
    token_usage: Optional[list[ScanTokenUsage]] = None
    scan_id: Optional[str] = None
    # Set on ERROR-status placeholder results when the file failed before
    # producing real output (used by ``execute_check_collections`` per-item
    # error isolation). Real verifications leave this as None.
    error: Optional[BaseException] = None
    # Raw measurement dicts emitted as ``metrics: [...]`` in the scan-results
    # payload. Populated by metric-monitoring callers; contracts leave this empty.
    # list[dict] rather than list[Measurement] so callers can attach pre-serialised
    # cloud-shape dicts without depending on the engine's internal Measurement class.
    measurement_dicts: list[dict] = field(default_factory=list)

    def get_logs(self) -> list[str]:
        return [r.getMessage() for r in self.log_records] if self.log_records else []

    def get_logs_str(self) -> str:
        return "\n".join(self.get_logs())

    def get_errors(self) -> list[str]:
        return [r.getMessage() for r in self.log_records if r.levelno >= ERROR] if self.log_records else []

    def get_errors_str(self) -> str:
        return "\n".join(self.get_errors())

    def get_warnings(self) -> list[str]:
        return [r.getMessage() for r in self.log_records if r.levelno == WARNING] if self.log_records else []

    def get_warnings_str(self) -> str:
        return "\n".join(self.get_warnings())

    @property
    def has_errors(self) -> bool:
        return self.status is CheckCollectionStatus.ERROR

    @property
    def errored_without_results(self) -> bool:
        """True when the run errored before producing any check results (e.g. the data
        source connection failed). Such a scan must be reported to Soda Cloud as FAILED,
        not sent as results: Cloud maps an errored result batch to COMPLETED_WITH_ERRORS,
        a transition the backend forbids while the scan is still PENDING."""
        return self.has_errors and not self.check_results

    @property
    def is_failed(self) -> bool:
        """
        Returns true if there are checks that have failed.
        False is returned if there are no check results.
        Only looks at check results.
        Ignores execution errors in the logs.
        """
        return self.status is CheckCollectionStatus.FAILED

    @property
    def is_passed(self) -> bool:
        """
        Returns true if there are no checks that have failed.
        Ignores execution errors in the logs.
        """
        return self.status is CheckCollectionStatus.PASSED

    @property
    def is_warned(self) -> bool:
        """
        Returns true if there are checks that have warnings.
        Ignores execution errors in the logs.
        """
        return self.status is CheckCollectionStatus.WARNED

    @property
    def is_ok(self) -> bool:
        return not self.is_failed and not self.has_errors

    @property
    def number_of_checks(self) -> int:
        return len(self.check_results)

    @property
    def number_of_checks_passed(self) -> int:
        return len([check_result for check_result in self.check_results if check_result.outcome == CheckOutcome.PASSED])

    @property
    def number_of_checks_failed(self) -> int:
        return len([check_result for check_result in self.check_results if check_result.outcome == CheckOutcome.FAILED])

    @property
    def number_of_checks_excluded(self) -> int:
        return len(
            [check_result for check_result in self.check_results if check_result.outcome == CheckOutcome.EXCLUDED]
        )


@dataclass
class CheckCollectionSessionResult:
    """Result of verifying multiple check-collection files in one session.

    Per-file results are positional with the input items.
    """

    results: list[CheckCollectionResult] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        """True if any per-file result has an engine/parse ERROR status."""
        return any(r.has_errors for r in self.results)

    @property
    def is_failed(self) -> bool:
        """True if any per-file result has at least one FAILED check."""
        return any(r.is_failed for r in self.results)

    @property
    def is_warned(self) -> bool:
        """True if any per-file result has at least one WARN check."""
        return any(r.is_warned for r in self.results)

    @property
    def sending_results_to_soda_cloud_failed(self) -> bool:
        """True if any per-file result failed to send to Soda Cloud."""
        return any(r.sending_results_to_soda_cloud_failed for r in self.results)


class CheckCollectionYaml:
    """Parsed YAML for one check-collection file.

    First-class properties populated by the base ``__init__``:

      - ``yaml_source``: the originating ``CheckCollectionYamlSource``.
      - ``yaml_object``: the parsed ``YamlObject`` root — parsed once
        here, reused by subtypes. The executor's kind-peek pass passes
        the pre-parsed object back in via the ``yaml_object`` kwarg so
        the YAML is parsed exactly once per session.
      - ``kind``: the YAML root's ``kind:`` field (used by the executor
        for subtype dispatch). May be ``None`` for files that omit it
        (the executor defaults missing kinds to ``"contract"`` for BC).
      - ``execution_timestamp``: when this session started — captured at
        construct time so every subtype has the same one.
      - ``data_timestamp``: the user-supplied (or implicit) data
        timestamp, normalised to ``datetime``. Falls back to
        ``execution_timestamp`` when the caller didn't supply one.

    Concrete subtypes call ``super().__init__(...)`` and add their own
    fields. ``ContractYaml`` is the canonical example.
    """

    def __init__(
        self,
        yaml_source: CheckCollectionYamlSource,
        yaml_object: Optional[YamlObject] = None,
        provided_variable_values: Optional[dict[str, str]] = None,
        data_timestamp: Optional[str] = None,
        primary_data_source_impl: Optional[DataSourceImpl] = None,
    ):
        self.yaml_source: CheckCollectionYamlSource = yaml_source
        # The executor's kind-peek pass parses each YAML once and passes
        # the result back in. When called directly (no executor), parse
        # here. Either way the YAML is parsed exactly once.
        # ``YamlSource.parse()`` either returns a ``YamlObject`` or raises;
        # never ``None``.
        self.yaml_object: YamlObject = yaml_object if yaml_object is not None else yaml_source.parse()
        self.kind: Optional[str] = self.yaml_object.read_string_opt("kind")
        self.execution_timestamp: datetime = datetime.now(timezone.utc)
        self.data_timestamp: datetime = _resolve_data_timestamp_str(data_timestamp, self.execution_timestamp)

    @classmethod
    def parse(
        cls,
        yaml_source: CheckCollectionYamlSource,
        yaml_object: Optional[YamlObject] = None,
        provided_variable_values: Optional[dict[str, str]] = None,
        data_timestamp: Optional[str] = None,
        primary_data_source_impl: Optional[DataSourceImpl] = None,
    ) -> "CheckCollectionYaml":
        return cls(
            yaml_source=yaml_source,
            yaml_object=yaml_object,
            provided_variable_values=provided_variable_values,
            data_timestamp=data_timestamp,
            primary_data_source_impl=primary_data_source_impl,
        )


def _resolve_data_timestamp_str(
    data_timestamp: Optional[str],
    default_when_unset_or_invalid: datetime,
) -> datetime:
    """Normalise a user-supplied ISO-8601 ``data_timestamp`` string to a
    ``datetime``; fall back to ``default_when_unset_or_invalid`` when the
    caller didn't supply one or the string fails to parse."""
    if isinstance(data_timestamp, str):
        parsed = convert_str_to_datetime(data_timestamp)
        if isinstance(parsed, datetime):
            return parsed
        logger.error(
            f"Provided 'data_timestamp' value is not a correct ISO 8601 " f"timestamp format: '{data_timestamp}'"
        )
    return default_when_unset_or_invalid


class CheckCollectionImpl:
    """Engine that verifies one check-collection file against a data source.

    Subclasses provide four plain class attributes; the engine inherits.

    Example subtype declaration::

        class FooImpl(CheckCollectionImpl):
            kind = "foo"          # YAML 'kind:' dispatch key
            wire_source = "foo"   # Cloud upload 'source' literal
            display_name = "foo"  # human-readable in logs (defaults to kind)
            yaml_class = FooYaml
            result_class = FooResult

    The base auto-disambiguates per-check identity via ``identity_prefix()``;
    subtypes that need byte-identical-history per-check hashes override it
    to return an empty tuple.
    """

    # Subtype identity — declared per-subtype as a plain class attribute.
    # The base default is empty string → not registered.
    kind: str = ""

    # Subclasses MUST override ``wire_source``. It is guarded at the top of
    # ``verify()`` so a missing override raises immediately rather than silently
    # routing to no Cloud feature.
    wire_source: str = ""
    # Optional suffix appended to the dataset qualified name to derive the
    # Soda Cloud scan-definition name. ``None`` (default) uses the bare
    # qualified name. Subtypes opt in by declaring a non-empty suffix; the
    # engine just threads the value through the upload.
    scan_definition_suffix: Optional[str] = None
    # Whether the engine requires ``collection_id`` to be set on this
    # subtype before upload. Default ``True``: emitted ``checkPath`` gets
    # prefixed with ``collection_id`` so the backend's
    # ``firstSegmentOf(checkPath)`` filter can route to the subtype's
    # identifier. Subtypes whose backend ingestion doesn't need that
    # prefix override to ``False``.
    requires_collection_id: bool = True
    # When True, N files of this subtype in one session combine into one
    # ``sodaCoreInsertScanResults`` upload. Default False keeps the
    # per-file upload path (byte-identical wire output) intact.
    #
    # Contract for combine_uploads=True subtypes: all files of one session must target a
    # single dataset (they share one scanId, and a scan is assumed to map to one dataset).
    # The executor enforces this via ``_raise_if_combined_session_spans_multiple_datasets``,
    # which reads ``self.soda_qualified_dataset_name`` — so such a subtype MUST populate that
    # attribute in ``__init__`` (the base does, from ``yaml.dataset``).
    combine_uploads: bool = False
    # Parametrize the type hints so subclass declarations (e.g.
    # ``yaml_class: type[ContractYaml]`` on ``ContractImpl``) are statically
    # checked: a subclass that points these at unrelated types will be
    # flagged by a type-checker rather than silently surviving until runtime.
    yaml_class: type[CheckCollectionYaml] = CheckCollectionYaml
    result_class: type[CheckCollectionResult] = CheckCollectionResult

    # Registry mapping ``kind`` → concrete impl class. Populated by
    # ``__init_subclass__`` when a subtype declares a non-empty ``kind``.
    # Test stubs that don't want to register simply leave ``kind`` empty.
    _REGISTRY: dict[str, type["CheckCollectionImpl"]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Only register subclasses that declare a non-empty kind in their
        # own class body. Inherited kinds (e.g. test stubs subclassing
        # ContractImpl) don't re-register.
        own_kind = cls.__dict__.get("kind", "")
        if own_kind:
            CheckCollectionImpl._REGISTRY[own_kind] = cls

    @classmethod
    def for_kind(cls, kind: str) -> type["CheckCollectionImpl"]:
        """Look up the impl class for a YAML ``kind:`` value.

        Raises ``ValueError`` with the list of registered kinds if the
        requested kind isn't registered. Missing/unimported subtype
        packages will fail loudly here.
        """
        if kind not in CheckCollectionImpl._REGISTRY:
            available = sorted(CheckCollectionImpl._REGISTRY.keys())
            raise ValueError(
                f"Unknown check-collection kind '{kind}'. "
                f"Registered: {available}. "
                f"Ensure the subtype's package is imported before dispatching."
            )
        return CheckCollectionImpl._REGISTRY[kind]

    # Plugin hooks. Extensions register against the concrete subclass that
    # cares about them; the base initialises its own empty dict so the
    # engine code can iterate without conditional checks. The classmethod
    # auto-isolates the dict per concrete subtype so registering on one
    # subtype doesn't mutate the shared base / sibling-subtype dict via
    # the MRO.
    impl_extensions: dict[str, type] = {}

    @classmethod
    def register_extension(cls, name: str, extension_cls: type) -> None:
        # Auto-isolate per concrete subtype: without this, ``cls`` may resolve
        # ``impl_extensions`` to a parent class's dict (the MRO), and mutating
        # that dict leaks the extension registration to every sibling subtype.
        if "impl_extensions" not in cls.__dict__:
            cls.impl_extensions = {}
        cls.impl_extensions[name] = extension_cls

    @property
    def display_name(self) -> str:
        """Human-readable label for this subtype in logs and error messages.

        Default: ``self.kind`` with hyphens swapped for spaces (so a
        ``kind = "my-subtype"`` declaration shows up as ``"my subtype"``
        in user-facing output). Subclasses override by declaring a plain
        ``display_name`` class attribute — that shadows this property via
        normal MRO lookup (Python finds the subclass's string before the
        base's data descriptor).
        """
        return self.kind.replace("-", " ") if self.kind else "check collection"

    def identity_prefix(self) -> tuple:
        """Identity prefix mixed into every emitted check's identity hash.

        Default: ``(wire_source, collection_id)`` so two collections with
        identical check shapes on the same dataset produce distinct
        identities. Subtypes that need a different prefix shape override
        this method (e.g. return ``()`` to keep per-check hashes
        byte-identical to historical emissions).
        """
        return (self.wire_source, self.collection_id)

    @property
    def collection_id(self) -> Optional[str]:
        """Identifier of this check-collection instance.

        Default: ``None`` — subtypes without a per-instance identifier
        inherit this and derive their identity from dataset + check
        shape alone. Subtypes that carry a per-instance identifier
        override to compute it from their YAML (e.g.,
        ``return self.yaml.name``). The result becomes the first segment
        of the wire ``checkPath`` and is mixed into the identity prefix.
        """
        return None

    @property
    def thread_label(self) -> str:
        """Per-collection ``thread`` value stamped on each captured record:
        ``"{wire_source}.{collection_id}"`` (or the bare ``wire_source`` without a
        ``collection_id``). Soda Cloud surfaces it as a per-scan log filter."""
        return f"{self.wire_source}.{self.collection_id}" if self.collection_id else self.wire_source

    @property
    def source_description(self) -> str:
        """Human identifier for this collection's yaml source in console lines:
        the file path when the yaml came from a local file, else the collection
        id (e.g. a Cloud-fetched data standard's DQN, where there is no path),
        else the yaml source's generic description — never ``None``."""
        return self.yaml.yaml_source.file_path or self.collection_id or self.yaml.yaml_source.description

    def __init__(
        self,
        yaml: CheckCollectionYaml,
        data_source_impl: Optional[DataSourceImpl],
        soda_cloud_impl: Optional[SodaCloud] = None,
        publish_results: bool = False,
        only_validate_without_execute: bool = False,
        check_selectors: Optional[list] = None,
        execution_timestamp: Optional[datetime] = None,
        data_timestamp: Optional[datetime] = None,
        all_data_source_impls: Optional[dict[str, DataSourceImpl]] = None,
        dwh_files: Optional[DiagnosticsWarehouseFiles] = None,
        logs: Optional[Logs] = None,
    ):
        # Defer import: CheckImpl/ColumnImpl/MetricsResolver/RowCountMetricImpl live in
        # contract_verification_impl.py which imports this module.
        from soda_core.contracts.impl.check_types.row_count_check import (
            RowCountMetricImpl,
        )
        from soda_core.contracts.impl.contract_verification_impl import MetricsResolver

        self.logs: Logs = logs if logs is not None else Logs()
        self.yaml: CheckCollectionYaml = yaml
        self.only_validate_without_execute: bool = only_validate_without_execute
        # Stamp this collection's ``thread`` label on every record it emits.
        self.logs.label = self.thread_label
        self.data_source_impl: Optional[DataSourceImpl] = data_source_impl
        self.all_data_source_impls: dict[str, DataSourceImpl] = all_data_source_impls or {}
        self.soda_cloud: Optional[SodaCloud] = soda_cloud_impl
        self.publish_results: bool = publish_results
        self.soda_config = EnvConfigHelper()

        self.filter: Optional[str] = yaml.filter
        self.check_selectors: list = check_selectors if check_selectors is not None else []

        self.started_timestamp: datetime = datetime.now(tz=timezone.utc)

        self.execution_timestamp: datetime = execution_timestamp
        self.data_timestamp: datetime = data_timestamp

        self.dataset_name: Optional[str] = None

        self.check_attributes: dict[str, Any] = yaml.check_attributes

        self.dataset_identifier = DatasetIdentifier.parse(yaml.dataset)
        self.dataset_prefix: list[str] = self.dataset_identifier.prefixes
        self.dataset_name = self.dataset_identifier.dataset_name

        self.metrics_resolver: MetricsResolver = MetricsResolver()

        self.column_impls: list = []
        self.check_impls: list = []

        self.soda_qualified_dataset_name = yaml.dataset
        self.sql_qualified_dataset_name: Optional[str] = None

        self.datasource_warehouse: Optional[str] = None
        self.compute_warehouse: Optional[str] = None

        if data_source_impl:
            self.sql_qualified_dataset_name = data_source_impl.sql_dialect.qualify_dataset_name(
                dataset_prefix=self.dataset_prefix, dataset_name=self.dataset_name
            )

            if data_source_impl.data_source_connection:
                if hasattr(data_source_impl.data_source_connection.connection_properties, "warehouse"):
                    self.datasource_warehouse = data_source_impl.data_source_connection.connection_properties.warehouse

                if self.datasource_warehouse is None:
                    self.datasource_warehouse = data_source_impl.get_current_warehouse()

        self.row_count_metric_impl = self.metrics_resolver.resolve_metric(RowCountMetricImpl(contract_impl=self))
        self.dataset_rows_tested: Optional[int] = None

        # Dataset defining CTE - used as basis for all queries in this collection
        self.cte = CTE("_soda_filtered_dataset").AS(
            [
                SELECT(STAR()),
                FROM(self.dataset_identifier.dataset_name, self.dataset_identifier.prefixes),
                WHERE.optional(SqlExpressionStr.optional(self.filter)),
            ]
        )
        # Optional sampler configuration.
        self.sampler_type: Optional[SamplerType] = None
        self.sampler_limit: Optional[Number] = None

        self.dataset_configuration: Optional[DatasetConfigurationDTO] = None
        if self.soda_cloud:
            self.dataset_configuration = self.soda_cloud.fetch_dataset_configuration(self.dataset_identifier)

        if self.dataset_configuration:
            if (
                self.dataset_configuration.test_row_sampler_configuration
                and self.dataset_configuration.test_row_sampler_configuration.enabled
                and self.dataset_configuration.test_row_sampler_configuration.test_row_sampler is not None
            ):
                self.sampler_type = map_sampler_type_from_dto(
                    self.dataset_configuration.test_row_sampler_configuration.test_row_sampler.type
                )
                self.sampler_limit = self.dataset_configuration.test_row_sampler_configuration.test_row_sampler.limit

            if self.dataset_configuration.compute_warehouse_override:
                self.compute_warehouse = self.dataset_configuration.compute_warehouse_override.name

        if self.should_apply_sampling:
            logger.info(
                f"Row sampling is enabled for dataset {self.dataset_identifier.to_string()} "
                f"with sampler config: type:'{self.dataset_configuration.test_row_sampler_configuration.test_row_sampler.type}', "
                f"limit:'{self.dataset_configuration.test_row_sampler_configuration.test_row_sampler.limit}'"
            )

            self.cte.cte_query[1] = self.cte.cte_query[1].SAMPLE(
                self.sampler_type,
                self.sampler_limit,
            )

        self.extensions: list = []
        for extension_cls in type(self).impl_extensions.values():
            try:
                extension = extension_cls(self)
                self.extensions.append(extension)
            except Exception as e:
                logger.error(
                    f"Error extending {self.display_name} implementation with extension {extension_cls.__name__}: {e}",
                )

        self.column_impls = self._parse_columns(yaml)
        self.check_impls = self._parse_checks(yaml)

        dataset_check_impls: list = list(self.check_impls)
        column_check_impls: list = []
        for column_impl in self.column_impls:
            column_check_impls.extend(column_impl.check_impls)
        # For consistency and predictability, we want the checks eval and results in the same order as in the YAML
        self.all_check_impls: list = (
            dataset_check_impls + column_check_impls
            if self._dataset_checks_came_before_columns_in_yaml()
            else column_check_impls + dataset_check_impls
        )

        self._verify_duplicate_identities(self.all_check_impls)
        self.metrics: list = self.metrics_resolver.get_resolved_metrics()

        self.queries: list = []
        if data_source_impl:
            self.queries = self._build_queries()

        self.dwh_files: Optional[DiagnosticsWarehouseFiles] = dwh_files

    @property
    def is_test_verification_on_runner(self) -> bool:
        """Whether this is a test scan running on the Soda Cloud Runner (formerly Soda Agent).

        Default: False. Subclasses (``ContractImpl``) override.
        """
        return False

    @property
    def is_test_verification_on_agent(self) -> bool:
        """Deprecated alias for :pyattr:`is_test_verification_on_runner`."""
        from soda_core.common._deprecation import warn_deprecated

        warn_deprecated(
            f"{type(self).__name__}.is_test_verification_on_agent",
            f"{type(self).__name__}.is_test_verification_on_runner",
        )
        return self.is_test_verification_on_runner

    @property
    def is_sampling_enabled(self) -> bool:
        return self.sampler_type is not None and self.sampler_limit is not None

    @property
    def should_apply_sampling(self) -> bool:
        return self.is_test_verification_on_runner and self.is_sampling_enabled

    def _dataset_checks_came_before_columns_in_yaml(self) -> Optional[bool]:
        keys: list[str] = self.yaml.yaml_object.keys()
        if "checks" in keys and "columns" in keys:
            return keys.index("checks") < keys.index("columns")
        return None

    def _parse_checks(self, yaml: CheckCollectionYaml) -> list:
        from soda_core.contracts.impl.contract_verification_impl import CheckImpl

        check_impls: list = []
        if yaml.checks:
            for check_yaml in yaml.checks:
                if check_yaml:
                    check = CheckImpl.parse_check(contract_impl=self, check_yaml=check_yaml)
                    check_impls.append(check)

        for extension in self.extensions:
            try:
                check_impls.extend(extension.parse_checks(contract_impl=self))
            except Exception as e:
                logger.error(f"Error parsing checks with extension {extension.__class__.__name__}: {e}")

        return check_impls

    def _build_queries(self) -> list:
        from soda_core.contracts.impl.check_types.schema_check import SchemaQuery
        from soda_core.contracts.impl.contract_verification_impl import (
            AggregationMetricImpl,
            AggregationQuery,
        )

        queries: list = []

        for check in self.all_check_impls:
            queries.extend(check.queries)

        aggregation_metrics: list = []
        for metric in self.metrics:
            # Only build aggregation queries for metrics of known origin. Extensions might build their own queries.
            if isinstance(metric, AggregationMetricImpl):
                if (metric.data_source_impl is None and metric.dataset_identifier is None) or (
                    metric.data_source_impl == self.data_source_impl
                    and metric.dataset_identifier == self.dataset_identifier
                ):
                    aggregation_metrics.append(metric)

        schema_queries: list = []
        other_queries: list = []
        for query in queries:
            if isinstance(query, SchemaQuery):
                schema_queries.append(query)
            else:
                other_queries.append(query)

        # Deduplicate byte-identical aggregation metrics before bundling them into
        # queries. Two metrics that render to the same SQL (e.g. MissingCheckImpl
        # and InvalidCheckImpl on the same column both resolving a MissingCount
        # metric) compute the redundant aggregate twice on the warehouse otherwise.
        # Key on (type, rendered_sql) so we never collapse metrics that would
        # convert their DB value differently (``int`` vs ``float`` vs identity).
        canonical_metrics: list = []
        canonical_by_sql: dict = {}
        # canonical_metric.id -> [aliased_metric, ...]. We keep the full metric impls
        # (not just ids) so AggregationQuery.execute can emit each Measurement under
        # the aliased metric's own get_short_description() — two deduped metrics may
        # belong to different check types (e.g. missing vs invalid) and have different
        # metric_name semantics for downstream consumers.
        metric_aliases: dict = {}
        if self.data_source_impl is not None:
            sql_dialect = self.data_source_impl.sql_dialect
            for metric in aggregation_metrics:
                key = (type(metric), sql_dialect.build_expression_sql(metric.sql_expression()))
                canonical = canonical_by_sql.get(key)
                if canonical is None:
                    canonical_by_sql[key] = metric
                    canonical_metrics.append(metric)
                else:
                    metric_aliases.setdefault(canonical.id, []).append(metric)
        else:
            canonical_metrics = aggregation_metrics

        aggregation_queries: list = []
        for aggregation_metric in canonical_metrics:
            if len(aggregation_queries) == 0 or not aggregation_queries[-1].can_accept(aggregation_metric):
                aggregation_queries.append(
                    AggregationQuery(
                        cte=self.cte,
                        dataset_prefix=self.dataset_prefix,
                        dataset_name=self.dataset_name,
                        data_source_impl=self.data_source_impl,
                        logs=self.logs,
                        metric_aliases=metric_aliases,
                    )
                )
            last_aggregation_query = aggregation_queries[-1]
            last_aggregation_query.append_aggregation_metric(aggregation_metric)

        all_queries: list = schema_queries + aggregation_queries + other_queries

        for extension in self.extensions:
            try:
                extension_queries: list = extension.build_queries(contract_impl=self)
                all_queries.extend(extension_queries)
            except Exception as e:
                logger.error(f"Error building queries with extension {extension.__class__.__name__}: {e}")

        return all_queries

    def _parse_columns(self, yaml: CheckCollectionYaml) -> list:
        from soda_core.contracts.impl.contract_verification_impl import ColumnImpl

        columns: list = []
        if yaml.columns:
            for column_yaml in yaml.columns:
                if column_yaml:
                    column = ColumnImpl(contract_impl=self, column_yaml=column_yaml)
                    columns.append(column)
        return columns

    def verify(self) -> CheckCollectionResult:
        from soda_core.contracts.impl.contract_verification_impl import (
            ContractVerificationHandlerRegistry,
            DerivedMetricImpl,
            MeasurementValues,
            _get_contract_verification_status,
        )

        if not self.wire_source:
            raise ValueError(f"{type(self).__name__} did not declare wire_source class attribute")

        # Subtypes that need ``collection_id`` (the default) must have a
        # non-empty value: the wire ``checkPath`` is prefixed with it so
        # the backend's ``firstSegmentOf(checkPath)`` filter can route to
        # the subtype's identifier. Without one the emitted checks would
        # silently degrade to unprefixed paths the backend would drop.
        # Subtypes that opt out via ``requires_collection_id = False``
        # bypass this guard.
        if self.requires_collection_id and not self.collection_id:
            raise ValueError(
                f"{type(self).__name__} with wire_source={self.wire_source!r} requires a non-empty "
                f"collection_id (used to prefix checkPath for backend routing)."
            )

        if self.data_source_impl and self.soda_config.is_running_on_runner:
            self.data_source_impl.switch_warehouse(self.compute_warehouse, contract_impl=self)
        data_source: Optional[DataSource] = None
        check_results: list[CheckResult] = []
        measurements: list[Measurement] = []
        verification_status: CheckCollectionStatus = CheckCollectionStatus.UNKNOWN

        verb: str = "Validating" if self.only_validate_without_execute else "Verifying"
        logger.info(
            f"{verb} {self.display_name} {Emoticons.SCROLL} " f"{self.source_description} {Emoticons.FINGERS_CROSSED}"
        )

        if self.data_source_impl:
            data_source = self.data_source_impl.build_data_source()

        if self.logs.has_errors:
            verification_status = CheckCollectionStatus.ERROR

        elif not self.only_validate_without_execute:
            # Executing the queries will set the value of the metrics linked to queries.
            # A SodaCoreException from one query (e.g. an aggregation referencing a column
            # that has been dropped) must not abort the scan — other queries, including the
            # schema query, still need to run so the user sees the real cause.
            for query in self.queries:
                try:
                    query_measurements: list[Measurement] = query.execute()
                    measurements.extend(query_measurements)
                except SodaCoreException as e:
                    logger.error(f"Query execution failed, continuing with remaining checks: {e}")

            measurement_values: MeasurementValues = MeasurementValues(measurements)

            self.dataset_rows_tested = measurement_values.get_value(self.row_count_metric_impl)

            # Triggering the derived metrics to initialize their value based on their dependencies
            derived_metric_impls: list = [
                derived_metric for derived_metric in self.metrics if isinstance(derived_metric, DerivedMetricImpl)
            ]
            for derived_metric_impl in derived_metric_impls:
                measurement_values.derive_value(derived_metric_impl)

            if self.data_source_impl:
                # Evaluate the checks
                for check_impl in self.all_check_impls:
                    if check_impl.skip:
                        logger.info(f"Skipping evaluation of check at path '{check_impl.path}'")
                        check_result: CheckResult = CheckResult(
                            check=check_impl._build_check_info(), outcome=CheckOutcome.EXCLUDED
                        )
                    else:
                        # Framework-level NOT_EVALUATED gating: if a check declares
                        # required metrics via get_required_metric_impls() and any
                        # of them are unmeasured (upstream aggregation query failed),
                        # short-circuit to NOT_EVALUATED. The outcome itself signals
                        # "not measured", so diagnostics carry only dataset_rows_tested
                        # (plus any sentinels the check opts into via get_diagnostic_defaults).
                        required = check_impl.get_required_metric_impls()
                        if required and not measurement_values.all_measured(*required):
                            # Merge order: defaults first, dataset_rows_tested last,
                            # so a check that accidentally puts `dataset_rows_tested`
                            # in its defaults can't override the real dataset value.
                            check_result: CheckResult = CheckResult(
                                check=check_impl._build_check_info(),
                                outcome=CheckOutcome.NOT_EVALUATED,
                                threshold_value=None,
                                diagnostic_metric_values={
                                    **check_impl.get_diagnostic_defaults(),
                                    "dataset_rows_tested": self.dataset_rows_tested,
                                },
                            )
                        else:
                            check_result: CheckResult = check_impl.evaluate(measurement_values=measurement_values)
                    check_results.append(check_result)

            verification_status = _get_contract_verification_status(self.logs.has_errors, check_results)

            log_lines = self.build_log_summary(
                soda_qualified_dataset_name=self.soda_qualified_dataset_name, check_results=check_results
            )
            for line in log_lines:
                logger.info(line)

        # The gatherer's live list: records emitted after this point (upload,
        # post-processing handlers) still surface on the held result.
        log_records: Optional[list[LogRecord]] = self.logs.get_log_records()

        soda_cloud_file_id: Optional[str] = None
        sending_results_to_soda_cloud_failed: bool = False
        yaml_source_str_original = self.yaml.yaml_source.yaml_str_original
        soda_cloud_response_json: Optional[dict] = None

        if self.soda_cloud and self.publish_results:
            soda_cloud_file_id = self.soda_cloud._upload_contract_yaml_file(
                yaml_source_str_original, file_label=self.display_name
            )

        post_processing_stages: list[PostProcessingStage] = []
        for handler in ContractVerificationHandlerRegistry.post_processing_stages.values():
            post_processing_stages += handler.provides_post_processing_stages()

        verification_result: CheckCollectionResult = self.result_class(
            check_collection=Contract(
                data_source_name=self.data_source_impl.name if self.data_source_impl else None,
                dataset_prefix=self.dataset_prefix,
                dataset_name=self.dataset_name,
                soda_qualified_dataset_name=self.soda_qualified_dataset_name,
                source=YamlFileContentInfo(
                    source_content_str=yaml_source_str_original,
                    local_file_path=self.yaml.yaml_source.file_path,
                    soda_cloud_file_id=soda_cloud_file_id,
                ),
                dataset_id=None,
            ),
            data_source=data_source,
            data_timestamp=self.data_timestamp,
            started_timestamp=self.started_timestamp,
            ended_timestamp=datetime.now(tz=timezone.utc),
            measurements=measurements,
            check_results=check_results,
            sending_results_to_soda_cloud_failed=sending_results_to_soda_cloud_failed,
            status=verification_status,
            log_records=log_records,
            post_processing_stages=post_processing_stages,
        )

        scan_id: Optional[str] = None
        if soda_cloud_file_id:
            if data_source is None:
                logger.error(
                    f"Not sending results to Soda Cloud {Emoticons.CROSS_MARK} "
                    f"Data source not found. Check that the data source name in the YAML's "
                    f"'dataset' field matches the name in your data source configuration."
                )
                sending_results_to_soda_cloud_failed = True
                verification_result.sending_results_to_soda_cloud_failed = True
            elif not self._verify_check_sources_aligned(verification_result):
                # Alignment guard tripped — logging + flag already set inside the helper.
                # We intentionally skip the upload (don't even attempt) so the backend
                # never sees a misaligned batch (a single misaligned check would 500
                # the entire batch on the server-side ingestion filter).
                sending_results_to_soda_cloud_failed = True
            elif self.combine_uploads:
                # Session-level combined upload — executor sends after the loop (and applies
                # the same errored-without-results -> mark-scan-failed handling there).
                logger.debug(f"Deferring upload to session-level combined request " f"{Emoticons.FINGERS_CROSSED}")
            elif verification_result.errored_without_results and self.soda_config.soda_scan_id:
                # A runner-created (still PENDING) Cloud scan errored before producing any
                # check results. Report it as FAILED instead of sending results: an errored,
                # result-less batch makes Cloud attempt PENDING -> COMPLETED_WITH_ERRORS, which
                # the scan-state machine rejects (invalid_scan_state), whereas PENDING ->
                # FAILED is allowed — so no false RESULTS_NOT_SENT_TO_CLOUD (exit 4) is raised.
                # Gate on the scan id (what mark_scan_as_failed needs), not is_running_on_runner:
                # without a scan id we fall through to the normal upload below, which creates
                # the scan and preserves the errored result's Cloud visibility.
                # Stamp the known scan id on the result so post-processing failure reporting
                # (run_post_processing_handlers) can update Cloud, and pass it explicitly
                # rather than have mark_scan_as_failed re-read it from the environment.
                verification_result.scan_id = self.soda_config.soda_scan_id
                self.soda_cloud.mark_scan_as_failed(scan_id=verification_result.scan_id, logs=log_records)
            else:
                # send_check_collection_results stamps scan_id + dataset_id
                # on the result internally; we just hold the response_json
                # for the post-processing-handler invocation below.
                soda_cloud_response_json = self.soda_cloud.send_check_collection_results(
                    [verification_result],
                    wire_source=self.wire_source,
                    scan_definition_suffix=type(self).scan_definition_suffix,
                )
        else:
            logger.debug(f"Not sending results to Soda Cloud {Emoticons.CROSS_MARK}")

        # Post-processing handlers. For combine-upload subtypes, defer to
        # the session executor — handlers need the scan_id and
        # response_json from the *combined* upload, which hasn't happened
        # yet at this point in verify(). The executor dispatches
        # ``ContractVerificationHandler.handle_session`` once per wire-source
        # group after the session-level ``send_check_collection_results``
        # returns.
        if not self.combine_uploads:
            self.run_post_processing_handlers(verification_result, soda_cloud_response_json)

        return verification_result

    def run_post_processing_handlers(
        self,
        verification_result: CheckCollectionResult,
        soda_cloud_send_results_response_json: Optional[dict],
    ) -> None:
        """Invoke every registered ``ContractVerificationHandler`` for this file.

        Called inline from ``verify()`` for non-combine subtypes only.
        Combine-upload subtypes are post-processed by the session executor
        instead, which dispatches ``handle_session`` once per wire-source
        group after the combined ``send_check_collection_results`` returns
        so handlers see the shared ``scan_id`` and ``response_json``. Files
        that were skipped from the combined upload (alignment guard, missing
        file_id, send failure) are still post-processed there with
        ``response_json=None`` to match this path's "run handlers regardless
        of upload success" semantics.
        """
        from soda_core.contracts.impl.contract_verification_impl import (
            ContractVerificationHandlerRegistry,
        )

        scan_id = verification_result.scan_id
        for handler in ContractVerificationHandlerRegistry.contract_verification_handlers:
            try:
                handler.handle(
                    contract_impl=self,
                    data_source_impl=self.data_source_impl,
                    contract_verification_result=verification_result,
                    soda_cloud=self.soda_cloud,
                    soda_cloud_send_results_response_json=soda_cloud_send_results_response_json,
                    dwh_files=self.dwh_files,
                )
            except Exception as e:
                logger.error(f"Error in {self.display_name} verification handler: {e}", exc_info=True)
                self._handle_post_processing_failure(scan_id=scan_id, exc=e, contract_verification_handler=handler)

    def verify_on_runner(
        self,
        soda_cloud_impl: SodaCloud,
        variables: dict,
        blocking_timeout_in_minutes: int,
        publish_results: bool,
        verbose: bool,
    ) -> CheckCollectionResult:
        """Runner-path verification. Default: raise NotImplementedError.

        Subtypes that support remote execution (``ContractImpl``) override.
        """
        raise NotImplementedError(f"{self.display_name} does not support runner execution")

    @classmethod
    def build_error_result(
        cls,
        yaml_source: CheckCollectionYamlSource,
        exception: BaseException,
    ) -> CheckCollectionResult:
        """Build a minimal ERROR-status result for a file that failed before
        producing real output.

        Used by per-item isolation in ``execute_check_collections``.
        """
        now = datetime.now(tz=timezone.utc)
        # Invariant: this placeholder Contract is never uploaded to Soda Cloud.
        # ``build_error_result`` is only invoked when the YAML failed to parse
        # before a real ``Contract`` could be constructed; the result it
        # produces has ERROR status and no ``soda_cloud_file_id`` is ever
        # attached to ``source``, so the engine's "upload if file id present"
        # gate in ``verify()`` skips it. The empty-string / empty-list /
        # ``None`` values below are inert: they exist solely to satisfy the
        # ``Contract`` dataclass signature on the in-memory result returned
        # to the launcher.
        result = cls.result_class(
            check_collection=Contract(
                data_source_name=None,
                dataset_prefix=[],
                dataset_name="",
                soda_qualified_dataset_name="",
                source=YamlFileContentInfo(
                    source_content_str=getattr(yaml_source, "yaml_str_original", None),
                    local_file_path=getattr(yaml_source, "file_path", None),
                ),
            ),
            data_source=None,
            data_timestamp=None,
            started_timestamp=now,
            ended_timestamp=now,
            status=CheckCollectionStatus.ERROR,
            measurements=[],
            check_results=[],
            sending_results_to_soda_cloud_failed=False,
            log_records=None,
            post_processing_stages=[],
        )
        result.error = exception
        return result

    def build_log_summary(self, soda_qualified_dataset_name: str, check_results: list[CheckResult]) -> list[str]:
        from tabulate import tabulate

        summary_lines: list[str] = []

        failed_count: int = 0
        warned_count: int = 0
        not_evaluated_count: int = 0
        passed_count: int = 0
        excluded_count: int = 0

        for check_result in check_results:
            if check_result.is_failed:
                failed_count += 1
            elif check_result.is_not_evaluated:
                not_evaluated_count += 1
            elif check_result.is_passed:
                passed_count += 1
            elif check_result.is_warned:
                warned_count += 1
            elif check_result.is_excluded:
                excluded_count += 1
        total_count: int = failed_count + not_evaluated_count + passed_count + warned_count + excluded_count

        error_count: int = len(self.logs.get_errors())

        table_lines = [
            ["Checks", total_count],
            ["Passed", passed_count, Emoticons.WHITE_CHECK_MARK],
        ]

        if failed_count > 0:
            table_lines.append(["Failed", failed_count, Emoticons.CROSS_MARK])
        else:
            table_lines.append(["Failed", failed_count, Emoticons.WHITE_CHECK_MARK])

        if warned_count > 0:
            table_lines.append(["Warned", warned_count, Emoticons.WARNING])
        else:
            table_lines.append(["Warned", warned_count, Emoticons.WHITE_CHECK_MARK])

        if not_evaluated_count > 0:
            table_lines.append(["Not Evaluated", not_evaluated_count, Emoticons.CROSS_MARK])
        else:
            table_lines.append(["Not Evaluated", not_evaluated_count, Emoticons.WHITE_CHECK_MARK])

        if excluded_count > 0:
            table_lines.append(["Excluded", excluded_count, Emoticons.QUESTION_MARK])
        else:
            table_lines.append(["Excluded", excluded_count, Emoticons.WHITE_CHECK_MARK])

        if error_count > 0:
            table_lines.append(["Runtime Errors", error_count, Emoticons.CROSS_MARK])
        else:
            table_lines.append(["Runtime Errors", error_count, Emoticons.WHITE_CHECK_MARK])

        summary_lines.append(f"\n### {self.display_name.capitalize()} results for {soda_qualified_dataset_name}")
        summary_lines.append(self.build_summary_table(check_results))

        overview_table = tabulate(table_lines, tablefmt="github", stralign="left")
        summary_lines.append(f"# Summary:\n{overview_table}\n")

        return [line for joined_line in summary_lines for line in joined_line.split("\n")]

    def log_table_extra_columns(self, check_result: CheckResult) -> dict:
        """Optional per-row extra columns for the results table logged by
        ``build_log_summary``.

        Default ``{}``: the table is unchanged for every collection type
        that doesn't override this (contract verification, regular metric
        monitoring, ...). Subtypes whose rows are otherwise visually
        identical (e.g. metric-monitoring BACKFILL — one row per backfilled
        window) override it to return e.g. ``{"Window": "2026-07-09"}``:
        NEW keys become columns inserted right after "Check" (participating
        in the row sort at that position); keys that already exist in the
        row (e.g. "Diagnostics") replace that cell's value in place.
        """
        return {}

    def log_table_header_overrides(self) -> dict[str, str]:
        """Optional header renames for the results table logged by
        ``build_log_summary``.

        Default ``{}``: headers are unchanged for every collection type that
        doesn't override this (contract verification, ...). Subtypes for
        which the standard header is a misnomer override it, e.g. metric
        monitoring returns ``{"Check": "Monitor"}`` so its table renders the
        "Check" column as "Monitor". This is a rename applied only when the
        table is rendered: row-dict keys (and the row sort) keep the internal
        names, so ``log_table_extra_columns`` overrides keep targeting e.g.
        ``"Diagnostics"`` regardless of how the header is displayed.
        """
        return {}

    def build_summary_table(self, check_results: list[CheckResult]) -> str:
        from tabulate import tabulate

        overview_table_data: list[dict] = []
        extra_column_names: list[str] = []  # caller-supplied NEW columns, in first-seen order
        for check_result in check_results:
            row = check_result.log_table_row()
            extra_columns = self.log_table_extra_columns(check_result)
            if extra_columns:
                row = _merge_log_table_extra_columns(row, extra_columns, extra_column_names)
            overview_table_data.append(row)

        # Sort by column name, check name, caller-supplied extra columns (none
        # by default) and check outcome
        sort_columns = ["Column", "Check", *extra_column_names, "Outcome"]
        overview_table_data.sort(key=lambda row: tuple(str(row.get(column, "")) for column in sort_columns))

        # Re-iterate rows data and remove column name if it is the same as the previous row
        previous_column_name: Optional[str] = None
        for row in overview_table_data:
            if previous_column_name == row["Column"]:
                row["Column"] = ""  # Clear column name if it is the same as the previous row
            else:
                previous_column_name = row["Column"]

        # Apply header renames (default {}) only at render time so the rows
        # above keep their internal keys for sorting and cell overrides.
        header_overrides: dict[str, str] = self.log_table_header_overrides()
        if header_overrides:
            overview_table_data = [
                {header_overrides.get(key, key): value for key, value in row.items()} for row in overview_table_data
            ]

        return tabulate(overview_table_data, headers="keys", tablefmt="grid")

    @classmethod
    def _verify_duplicate_identities(cls, all_check_impls: list):
        checks_by_identity: dict = {}
        for check_impl in all_check_impls:
            existing_check_impl = checks_by_identity.get(check_impl.identity)
            if existing_check_impl:
                original_location: Optional[Location] = existing_check_impl.check_yaml.check_yaml_object.location
                original_location_str: str = f" Original({original_location})" if original_location else ""
                duplicate_location: Optional[Location] = check_impl.check_yaml.check_yaml_object.location
                duplicate_location_str: str = f" Duplicate({duplicate_location})" if duplicate_location else ""
                logger.error(
                    msg=(
                        f"Duplicate identity {check_impl.build_identity_path()}."
                        f"{original_location_str}{duplicate_location_str}"
                    ),
                    extra={
                        ExtraKeys.LOCATION: duplicate_location,
                    },
                )
            checks_by_identity[check_impl.identity] = check_impl

    def _verify_check_sources_aligned(self, verification_result: "CheckCollectionResult") -> bool:
        """Iterate every emitted check result and verify its per-check
        ``source`` override (if any) matches ``self.wire_source``.

        Today every check inherits the collection's ``wire_source`` by
        default (``Check.source is None`` → the wire emitter falls back
        to ``wire_source``), so this guard is a no-op for current code
        paths. It exists to defend against future extension code that
        emits a Check with an explicit ``source`` override that disagrees
        with the parent collection — server-side ingestion filters reject
        a misaligned batch and fail the entire upload on a single
        offending check.

        On mismatch:
        - logs an error per offending check (with ``full_path``, the
          offending ``source``, and the expected ``wire_source``);
        - sets ``sending_results_to_soda_cloud_failed = True`` on the result;
        - returns ``False`` so ``verify()`` skips the Cloud upload entirely.

        Returns ``True`` when every check passes (the common path).
        """
        offending: list = []
        for check_result in verification_result.check_results:
            check_source: Optional[str] = check_result.check.source
            if check_source is not None and check_source != self.wire_source:
                offending.append(check_result)

        if not offending:
            return True

        # These records land on ``verification_result.log_records`` directly:
        # that list is the live gatherer list, so launchers see them via
        # ``result.get_errors()`` with no re-collection.
        for check_result in offending:
            logger.error(
                f"Source mismatch — check '{check_result.check.full_path}' has "
                f"source={check_result.check.source!r} but parent collection "
                f"declares wire_source={self.wire_source!r}. Skipping Cloud upload to avoid "
                f"a backend-side source-mismatch failure on the whole batch."
            )

        verification_result.sending_results_to_soda_cloud_failed = True
        return False

    def _handle_post_processing_failure(
        self,
        scan_id: Optional[str],
        exc: Exception,
        contract_verification_handler,
    ):
        from soda_core.contracts.contract_verification import PostProcessingStageState

        if scan_id is None:
            logger.warning("Not sending post-processing stage updates to Soda Cloud - no scan ID")
            return
        if self.soda_cloud is None:
            logger.warning("Not sending post-processing stage updates to Soda Cloud - no Soda Cloud client")
            return
        for post_processing_stage in contract_verification_handler.provides_post_processing_stages():
            self.soda_cloud.post_processing_update(
                stage=post_processing_stage.name,
                scan_id=scan_id,
                state=PostProcessingStageState.FAILED,
                error=get_exception_stacktrace(exc),
            )


def _merge_log_table_extra_columns(row: dict, extra_columns: dict, extra_column_names: list[str]) -> dict:
    """Merge one row's ``log_table_extra_columns`` result into the row dict.

    Keys already present in the row (e.g. "Diagnostics") replace that cell's
    value in place; NEW keys become columns inserted right after "Check" and
    are recorded in ``extra_column_names`` (first-seen order) so
    ``build_summary_table`` can let them participate in the row sort.
    """
    new_columns = {key: value for key, value in extra_columns.items() if key not in row}
    for column_name in new_columns:
        if column_name not in extra_column_names:
            extra_column_names.append(column_name)
    merged: dict = {}
    for key, value in row.items():
        merged[key] = extra_columns.get(key, value)
        if key == "Check":
            merged.update(new_columns)
    # A row without a "Check" cell still gets the new columns (appended).
    for key, value in new_columns.items():
        merged.setdefault(key, value)
    return merged
