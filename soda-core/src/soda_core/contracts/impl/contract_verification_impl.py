from __future__ import annotations

from abc import ABC
from enum import Enum
from io import StringIO
from typing import Protocol

from ruamel.yaml import YAML
from soda_core.check_collections.base import CheckCollectionImpl
from soda_core.common.consistent_hash_builder import ConsistentHashBuilder
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.exceptions import InvalidRegexException, SodaCoreException
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.sql_dialect import *
from soda_core.common.yaml import (
    ContractYamlSource,
    DataSourceYamlSource,
    SodaCloudYamlSource,
)
from soda_core.contracts.contract_verification import (
    Check,
    CheckCollectionStatus,
    CheckOutcome,
    CheckResult,
    ContractVerificationResult,
    ContractVerificationSessionResult,
    Measurement,
    PostProcessingStage,
    SodaException,
    Threshold,
)
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_core.contracts.impl.contract_yaml import (
    CheckYaml,
    ColumnYaml,
    ContractYaml,
    MissingAncValidityCheckYaml,
    MissingAndValidityYaml,
    RegexFormat,
    ThresholdYaml,
    ValidReferenceDataYaml,
)

logger: logging.Logger = soda_logger


class ContractVerificationHandler(ABC):
    @abstractmethod
    def handle(
        self,
        contract_impl: ContractImpl,
        data_source_impl: Optional[DataSourceImpl],
        contract_verification_result: ContractVerificationResult,
        soda_cloud: SodaCloud,
        soda_cloud_send_results_response_json: dict,
        dwh_data_source_file_path: Optional[str] = None,
    ):
        pass

    @abstractmethod
    def provides_post_processing_stages(self) -> list[PostProcessingStage]:
        pass


class ContractVerificationHandlerRegistry(ABC):
    contract_verification_handlers: list[ContractVerificationHandler] = []
    post_processing_stages: dict[str, ContractVerificationHandler] = {}

    @classmethod
    def register(cls, verification_handler: ContractVerificationHandler) -> None:
        cls.contract_verification_handlers.append(verification_handler)
        for stage in verification_handler.provides_post_processing_stages():
            stage_name = stage.name
            if stage_name in cls.post_processing_stages:
                logger.warning(f"Overriding existing contract verification handler for check type {stage_name}")
            cls.post_processing_stages[stage_name] = verification_handler


class ContractVerificationSessionImpl:
    """Implements the contract verification session.

    @param contract_yaml_sources: The list of contract YAML sources to verify.
    @param only_validate_without_execute: If True, only validate the contracts without executing them.
    @param variables: The variables to use in the contract queries.
    @param data_timestamp: The timestamp of the data to use for the verification.
    @param data_source_impls: The data source implementations to use for the verification.
    @param data_source_yaml_sources: The data source YAML sources to use for the verification.
    @param soda_cloud_impl: The Soda Cloud implementation to use for the verification.
    @param soda_cloud_publish_results: If True, publish the results to Soda Cloud.
    @param soda_cloud_use_agent: If True, use the Soda Cloud agent for the verification.
    @param soda_cloud_verbose: If True, enable verbose logging for the Soda Cloud agent.
    @param soda_cloud_use_agent_blocking_timeout_in_minutes: The timeout for the Soda Cloud agent.
    @param dwh_data_source_file_path: The file path to the Diagnostics Warehouse data source.
    """

    @classmethod
    def execute(
        cls,
        contract_yaml_sources: list[ContractYamlSource],
        only_validate_without_execute: bool = False,
        variables: Optional[dict[str, str]] = None,
        data_timestamp: Optional[str] = None,
        data_source_impls: Optional[list[DataSourceImpl]] = None,
        data_source_yaml_sources: Optional[list[DataSourceYamlSource]] = None,
        soda_cloud_impl: Optional[SodaCloud] = None,
        soda_cloud_publish_results: bool = False,
        soda_cloud_use_agent: bool = False,
        soda_cloud_verbose: bool = False,
        soda_cloud_use_agent_blocking_timeout_in_minutes: int = 60,
        check_selectors: Optional[list[CheckSelector]] = None,
        dwh_data_source_file_path: Optional[str] = None,
    ):
        logs: Logs = Logs()

        # Validate input contract_yaml_sources
        assert isinstance(contract_yaml_sources, list)
        assert all(
            isinstance(contract_yaml_source, ContractYamlSource) for contract_yaml_source in contract_yaml_sources
        )

        # Validate input variables
        if variables is None:
            variables = {}
        else:
            assert isinstance(variables, dict)
            assert all(isinstance(k, str) and isinstance(v, (str, Number)) for k, v in variables.items())

        # Validate input data_source_impls
        if data_source_impls is None:
            data_source_impls = []
        else:
            assert isinstance(data_source_impls, list)
            assert all(isinstance(data_source_impl, DataSourceImpl) for data_source_impl in data_source_impls)

        # Validate input data_source_yaml_sources
        if data_source_yaml_sources is None:
            data_source_yaml_sources = []
        else:
            assert isinstance(data_source_yaml_sources, list)
            assert all(
                isinstance(data_source_yaml_source, DataSourceYamlSource) or soda_cloud_use_agent
                for data_source_yaml_source in data_source_yaml_sources
            )

        # Validate input soda_cloud_impl
        if soda_cloud_impl is not None:
            assert isinstance(soda_cloud_impl, SodaCloud)

        # Validate input soda_cloud_skip_publish
        assert isinstance(soda_cloud_publish_results, bool)

        # Validate input soda_cloud_use_agent
        assert isinstance(soda_cloud_use_agent, bool)

        # Validate input soda_cloud_use_agent_blocking_timeout_in_minutes
        assert isinstance(soda_cloud_use_agent_blocking_timeout_in_minutes, int)

        if check_selectors is None:
            check_selectors = []

        if soda_cloud_use_agent:
            contract_verification_results: list[ContractVerificationResult] = cls._execute_on_agent(
                contract_yaml_sources=contract_yaml_sources,
                variables=variables,
                soda_cloud_impl=soda_cloud_impl,
                soda_cloud_use_agent_blocking_timeout_in_minutes=soda_cloud_use_agent_blocking_timeout_in_minutes,
                soda_cloud_publish_results=soda_cloud_publish_results,
                soda_cloud_verbose=soda_cloud_verbose,
            )

        else:
            contract_verification_results: list[ContractVerificationResult] = cls._execute_locally(
                logs=logs,
                contract_yaml_sources=contract_yaml_sources,
                only_validate_without_execute=only_validate_without_execute,
                provided_variable_values=variables,
                data_timestamp=data_timestamp,
                data_source_impls=data_source_impls,
                data_source_yaml_sources=data_source_yaml_sources,
                soda_cloud_impl=soda_cloud_impl,
                soda_cloud_publish_results=soda_cloud_publish_results,
                check_selectors=check_selectors,
                dwh_data_source_file_path=dwh_data_source_file_path,
            )
        return ContractVerificationSessionResult(contract_verification_results=contract_verification_results)

    @classmethod
    def _execute_locally(
        cls,
        logs: Logs,
        contract_yaml_sources: list[ContractYamlSource],
        only_validate_without_execute: bool,
        provided_variable_values: dict[str, str],
        data_timestamp: Optional[str],
        data_source_impls: list[DataSourceImpl],
        data_source_yaml_sources: list[DataSourceYamlSource],
        soda_cloud_impl: Optional[SodaCloud],
        soda_cloud_publish_results: bool,
        check_selectors: list[CheckSelector],
        dwh_data_source_file_path: Optional[str] = None,
    ) -> list[ContractVerificationResult]:
        "Verifies Contracts locally by funnelling through ``execute_check_collections``."
        from soda_core.check_collections.session import execute_check_collections

        data_source_impls_by_name: dict[str, DataSourceImpl] = cls._build_data_source_impls_by_name(
            data_source_impls=data_source_impls,
            data_source_yaml_sources=data_source_yaml_sources,
            provided_variable_values=provided_variable_values,
        )

        # Track which data sources had no open connection at session start so
        # we only close those that this session itself opened (caller-supplied
        # pre-opened connections are left alone, matching historical behavior).
        names_to_close: list[str] = [
            name for name, ds in data_source_impls_by_name.items() if not ds.has_open_connection()
        ]

        # Contract convention: the parse-time "primary" data source is the
        # entry registered under the literal name ``"primary_datasource"``
        # in ``data_source_impls_by_name``. Resolve here so the universal
        # ``execute_check_collections`` executor stays free of contract-
        # specific naming conventions.
        primary_data_source_impl: Optional[DataSourceImpl] = data_source_impls_by_name.get("primary_datasource")
        try:
            # Single-input contract callers historically re-raise on parse /
            # construction failure (tests rely on ``pytest.raises(...)``);
            # multi-input callers isolate per-item errors. ``ContractImpl``
            # resolves its own per-contract data source from
            # ``all_data_source_impls`` inside ``__init__``.
            #
            # Dispatch is by-kind: each contract yaml routes via its
            # top-level ``kind:`` field (defaulting to ``"contract"`` for
            # BC with existing YAMLs that don't declare one).
            session_result = execute_check_collections(
                yaml_sources=list(contract_yaml_sources),
                data_source_impl=None,
                soda_cloud_impl=soda_cloud_impl,
                publish_results=soda_cloud_publish_results,
                only_validate_without_execute=only_validate_without_execute,
                variables=provided_variable_values,
                data_timestamp=data_timestamp,
                all_data_source_impls=data_source_impls_by_name,
                check_selectors=check_selectors,
                dwh_data_source_file_path=dwh_data_source_file_path,
                abort_on_first_error=(len(contract_yaml_sources) == 1),
                logs=logs,
                primary_data_source_impl=primary_data_source_impl,
            )
        finally:
            for name in names_to_close:
                ds = data_source_impls_by_name.get(name)
                if ds is not None and ds.has_open_connection():
                    ds.close_connection()
        return list(session_result.results)

    @classmethod
    def _build_data_source_impls_by_name(
        cls,
        data_source_impls: list[DataSourceImpl],
        data_source_yaml_sources: list[DataSourceYamlSource],
        provided_variable_values: dict[str, str],
    ) -> dict[str, DataSourceImpl]:
        data_source_impl_by_name: dict[str, DataSourceImpl] = (
            {data_source_impl.name: data_source_impl for data_source_impl in data_source_impls}
            if data_source_impls
            else {}
        )
        for data_source_yaml_source in data_source_yaml_sources:
            data_source_impl: DataSourceImpl = DataSourceImpl.from_yaml_source(
                data_source_yaml_source=data_source_yaml_source, provided_variable_values=provided_variable_values
            )
            data_source_impl_by_name[data_source_impl.name] = data_source_impl
        return data_source_impl_by_name

    @classmethod
    def _build_soda_cloud_impl(
        cls,
        soda_cloud_impl: Optional[SodaCloud],
        soda_cloud_yaml_source: Optional[SodaCloudYamlSource],
        provided_variable_values: dict[str, str],
    ) -> Optional[SodaCloud]:
        if soda_cloud_impl:
            return soda_cloud_impl
        if soda_cloud_yaml_source:
            return SodaCloud.from_yaml_source(
                soda_cloud_yaml_source=soda_cloud_yaml_source, provided_variable_values=provided_variable_values
            )
        return None

    @classmethod
    def _execute_on_agent(
        cls,
        contract_yaml_sources: list[ContractYamlSource],
        variables: dict[str, str],
        soda_cloud_impl: Optional[SodaCloud],
        soda_cloud_use_agent_blocking_timeout_in_minutes: int,
        soda_cloud_publish_results: bool,
        soda_cloud_verbose: bool,
    ) -> list[ContractVerificationResult]:
        "Verifies Contracts on the Soda Cloud agent."
        contract_verification_results: list[ContractVerificationResult] = []

        for contract_yaml_source in contract_yaml_sources:
            try:
                contract_yaml: ContractYaml = ContractYaml.parse(
                    yaml_source=contract_yaml_source, provided_variable_values=variables
                )
                # Build a ContractImpl whose only job is to dispatch through
                # ``verify_on_agent``. ``data_source_impl`` and
                # ``soda_cloud_impl`` are left None on the impl, and
                # ``only_validate_without_execute=True`` keeps the engine
                # from connecting to a data source or executing queries.
                # Note that ``__init__`` is NOT a no-op here: it still parses
                # the YAML's columns and checks, builds the metrics resolver,
                # resolves the row-count metric, constructs the dataset CTE,
                # and instantiates registered extensions. None of that hits
                # the network — but it does run real parsing work, so a bad
                # YAML still raises here rather than at agent dispatch time.
                # The actual Soda Cloud client used to invoke the agent is
                # passed below to ``verify_on_agent``.
                contract_impl: ContractImpl = ContractImpl(
                    yaml=contract_yaml,
                    only_validate_without_execute=True,
                )
                contract_verification_result: ContractVerificationResult = contract_impl.verify_on_agent(
                    soda_cloud_impl=soda_cloud_impl,
                    variables=variables,
                    blocking_timeout_in_minutes=soda_cloud_use_agent_blocking_timeout_in_minutes,
                    publish_results=soda_cloud_publish_results,
                    verbose=soda_cloud_verbose,
                )
                contract_verification_results.append(contract_verification_result)
            except:
                logger.error(msg=f"Could not verify contract {contract_yaml_source}", exc_info=True)
        return contract_verification_results


class ContractImplExtension(Protocol):
    def __init__(self, contract_impl: ContractImpl):
        self.contract_impl: ContractImpl = contract_impl

    def parse_checks(self, contract_impl: ContractImpl) -> list[CheckImpl]:
        return []

    def build_queries(self, contract_impl: ContractImpl) -> list[Query]:
        return []


class ContractImpl(CheckCollectionImpl):
    """Contract subtype — same engine as ``CheckCollectionImpl``, contract identity.

    Engine logic (parse columns/checks, resolve metrics, build queries,
    execute, upload to Cloud) is inherited from ``CheckCollectionImpl``.
    """

    kind: str = "contract"
    wire_source: str = "soda-contract"
    display_name: str = "contract"
    yaml_class: type[ContractYaml] = ContractYaml
    result_class: type[ContractVerificationResult] = ContractVerificationResult

    # Per-subtype isolated extension registry. ``CheckCollectionImpl``'s
    # ``register_extension`` auto-isolates this dict at registration time, so
    # registering on ``ContractImpl`` never touches the base dict or sibling
    # subtypes (``DataStandardImpl``). The explicit declaration here makes
    # the isolation visible to static analysis and to anyone inspecting the
    # class for its extension surface.
    impl_extensions: dict[str, type[ContractImplExtension]] = {}

    def __init__(
        self,
        logs: Optional[Logs] = None,
        only_validate_without_execute: bool = False,
        data_source_impl: Optional[DataSourceImpl] = None,
        all_data_source_impls: Optional[dict[str, DataSourceImpl]] = None,
        data_timestamp: Optional[datetime] = None,
        execution_timestamp: Optional[datetime] = None,
        publish_results: bool = False,
        check_selectors: Optional[list[CheckSelector]] = None,
        dwh_data_source_file_path: Optional[str] = None,
        # Universal kwargs from ``CheckCollectionImpl`` — the session
        # executor passes these and they are the canonical names. The
        # legacy contract-specific aliases (``contract_yaml=`` /
        # ``soda_cloud=``) were dropped: callers in soda-core now pass
        # ``yaml=`` / ``soda_cloud_impl=``. soda-extensions consumers
        # cascade in a follow-up dispatch.
        yaml: Optional[ContractYaml] = None,
        soda_cloud_impl: Optional[SodaCloud] = None,
    ):
        resolved_all_data_source_impls: dict[str, DataSourceImpl] = (
            all_data_source_impls if all_data_source_impls is not None else {}
        )

        # Per-contract data source resolution. The session executor passes
        # ``data_source_impl=None`` plus ``all_data_source_impls`` and lets
        # the impl resolve its own data source from its yaml — that's an
        # impl-level concern, not a session-level concern. Legacy callers
        # that pass ``data_source_impl=`` directly bypass this branch.
        # ``all_data_source_impls`` may be an empty dict; the resolver still
        # runs so the missing-data-source error is logged (and surfaces on
        # the result's log_records the same way the legacy session loop did).
        resolved_data_source_impl: Optional[DataSourceImpl] = data_source_impl
        if resolved_data_source_impl is None and not only_validate_without_execute and yaml is not None:
            resolved_data_source_impl = self._resolve_data_source_impl(
                contract_yaml=yaml,
                all_data_source_impls=resolved_all_data_source_impls,
            )

        super().__init__(
            yaml=yaml,
            data_source_impl=resolved_data_source_impl,
            soda_cloud_impl=soda_cloud_impl,
            publish_results=publish_results,
            only_validate_without_execute=only_validate_without_execute,
            check_selectors=check_selectors if check_selectors is not None else [],
            execution_timestamp=execution_timestamp,
            data_timestamp=data_timestamp,
            all_data_source_impls=resolved_all_data_source_impls,
            dwh_data_source_file_path=dwh_data_source_file_path,
            logs=logs,
        )

    @staticmethod
    def _resolve_data_source_impl(
        contract_yaml: ContractYaml,
        all_data_source_impls: dict[str, DataSourceImpl],
    ) -> Optional[DataSourceImpl]:
        """Resolve the per-contract data source from ``all_data_source_impls``.

        Looks up the data source name from ``contract_yaml.dataset`` (the
        leading segment before the first ``/``), then opens the connection
        lazily if not already open. Logs an error and returns None when the
        named data source is missing.
        """
        if not contract_yaml.dataset:
            return None
        data_source_name: str = contract_yaml.dataset[: contract_yaml.dataset.find("/")]
        if not data_source_name:
            return None
        data_source_impl: Optional[DataSourceImpl] = all_data_source_impls.get(data_source_name)
        if not isinstance(data_source_impl, DataSourceImpl):
            logger.error(f"Data source '{data_source_name}' not found")
            return None
        if not data_source_impl.has_open_connection():
            data_source_impl.open_connection()
        return data_source_impl

    @property
    def contract_yaml(self) -> ContractYaml:
        """BC accessor — callers that read ``.contract_yaml`` still work."""
        return self.yaml

    @property
    def is_test_verification_on_agent(self) -> bool:
        """Contract-specific test-mode predicate.

        True when running on the Soda Cloud agent in test-scan mode (as
        determined by ``EnvConfigHelper`` env vars).
        """
        return self.soda_config.is_running_on_agent and self.soda_config.is_contract_test_scan_definition_type

    def verify_on_agent(
        self,
        soda_cloud_impl: SodaCloud,
        variables: dict,
        blocking_timeout_in_minutes: int,
        publish_results: bool,
        verbose: bool,
    ) -> ContractVerificationResult:
        return soda_cloud_impl.verify_contract_on_agent(
            contract_yaml=self.yaml,
            variables=variables,
            blocking_timeout_in_minutes=blocking_timeout_in_minutes,
            publish_results=publish_results,
            verbose=verbose,
        )


def _get_contract_verification_status(has_errors: bool, check_results: list[CheckResult]) -> CheckCollectionStatus:
    if has_errors:
        return CheckCollectionStatus.ERROR

    if any(check_result.outcome == CheckOutcome.FAILED for check_result in check_results):
        return CheckCollectionStatus.FAILED

    if any(check_result.outcome == CheckOutcome.WARN for check_result in check_results):
        return CheckCollectionStatus.WARNED

    if all(check_result.outcome == CheckOutcome.PASSED for check_result in check_results):
        return CheckCollectionStatus.PASSED

    return CheckCollectionStatus.UNKNOWN


class MeasurementValues:
    def __init__(self, measurements: list[Measurement]):
        self.metric_values_by_id: dict[str, any] = {
            measurement.metric_id: measurement.value for measurement in measurements
        }
        self.metric_ids_being_derived: set[str] = set()

    def get_value(self, metric_impl: MetricImpl) -> any:
        return self.metric_values_by_id.get(metric_impl.id)

    def all_measured(self, *metric_impls: "MetricImpl") -> bool:
        """True iff every metric has a non-None measurement.

        Intent: gate threshold-value construction in `evaluate()`. If any dependency
        is unmeasured (upstream aggregation query failed), callers must keep
        `threshold_value=None` so `evaluate_threshold(None)` returns NOT_EVALUATED
        — defaulting it to a Number (e.g. 0) would falsely PASS against thresholds
        like `must_be: 0`.

        Note: this is `is not None`, not `isinstance(Number)`. Diagnostic-dict
        population in `evaluate()` already gates on `isinstance(Number)` for type
        narrowing — that's a different concern and stays as-is.
        """
        return all(self.get_value(m) is not None for m in metric_impls)

    def derive_value(self, derived_metric_impl: DerivedMetricImpl) -> None:
        if derived_metric_impl.id not in self.metric_values_by_id:
            if derived_metric_impl.id in self.metric_ids_being_derived:
                logger.error("Bug: please report circular reference in derived metrics")
            else:
                self.metric_ids_being_derived.add(derived_metric_impl.id)
                for metric_dependency in derived_metric_impl.get_metric_dependencies():
                    if isinstance(metric_dependency, DerivedMetricImpl):
                        self.derive_value(metric_dependency)
                self.metric_ids_being_derived.remove(derived_metric_impl.id)
                value = derived_metric_impl.compute_derived_value(self)
                self.metric_values_by_id[derived_metric_impl.id] = value


class ColumnImpl:
    def __init__(self, contract_impl: ContractImpl, column_yaml: ColumnYaml):
        self.column_yaml: ColumnYaml = column_yaml
        self.missing_and_validity: MissingAndValidity = MissingAndValidity(missing_and_validity_yaml=column_yaml)
        self.check_impls: list[CheckImpl] = []
        if column_yaml.check_yamls:
            for check_yaml in column_yaml.check_yamls:
                if check_yaml:
                    check = CheckImpl.parse_check(
                        contract_impl=contract_impl,
                        column_impl=self,
                        check_yaml=check_yaml,
                    )
                    self.check_impls.append(check)

    @property
    def column_expression(self) -> SqlExpressionStr | COLUMN:
        if self.column_yaml.column_expression:
            return SqlExpressionStr(self.column_yaml.column_expression)

        return COLUMN(self.column_yaml.name)


class ValidReferenceData:
    @classmethod
    def parse(cls, valid_reference_data_yaml: Optional[ValidReferenceDataYaml]) -> Optional[ValidReferenceData]:
        return (
            ValidReferenceData(valid_reference_data_yaml)
            if valid_reference_data_yaml and valid_reference_data_yaml.dataset and valid_reference_data_yaml.column
            else None
        )

    def __init__(self, valid_reference_data_yaml: ValidReferenceDataYaml):
        dataset_identifier: DatasetIdentifier = DatasetIdentifier.parse(valid_reference_data_yaml.dataset)
        self.data_source_name: str = dataset_identifier.data_source_name
        self.dataset_prefix: list[str] = dataset_identifier.prefixes
        self.dataset_name: str = dataset_identifier.dataset_name
        self.column: str = valid_reference_data_yaml.column


class MissingAndValidity:
    def __init__(self, missing_and_validity_yaml: MissingAndValidityYaml):
        self.missing_values: Optional[list] = missing_and_validity_yaml.missing_values
        self.missing_format: Optional[RegexFormat] = missing_and_validity_yaml.missing_format

        self.invalid_values: Optional[list] = missing_and_validity_yaml.invalid_values
        self.invalid_format: Optional[RegexFormat] = missing_and_validity_yaml.invalid_format
        self.valid_values: Optional[list] = missing_and_validity_yaml.valid_values
        self.valid_format: Optional[RegexFormat] = missing_and_validity_yaml.valid_format
        self.valid_min: Optional[Number] = missing_and_validity_yaml.valid_min
        self.valid_max: Optional[Number] = missing_and_validity_yaml.valid_max
        self.valid_length: Optional[int] = missing_and_validity_yaml.valid_length
        self.valid_min_length: Optional[int] = missing_and_validity_yaml.valid_min_length
        self.valid_max_length: Optional[int] = missing_and_validity_yaml.valid_max_length
        self.valid_reference_data: Optional[ValidReferenceData] = ValidReferenceData.parse(
            missing_and_validity_yaml.valid_reference_data
        )

    def is_missing_expr(self, column_expression: COLUMN | SqlExpressionStr) -> SqlExpression:
        is_missing_clauses: list[SqlExpression] = [IS_NULL(column_expression)]
        if isinstance(self.missing_values, list):
            literal_values = [LITERAL(value) for value in self.missing_values]
            is_missing_clauses.append(IN(column_expression, literal_values))
        if isinstance(self.missing_format, RegexFormat) and isinstance(self.missing_format.regex, str):
            is_missing_clauses.append(REGEX_LIKE(column_expression, self.missing_format.regex))
        return OR.optional(is_missing_clauses)

    def is_invalid_expr(self, column_expression: str | COLUMN | SqlExpressionStr) -> Optional[SqlExpression]:
        invalid_clauses: list[SqlExpression] = []
        if isinstance(self.valid_values, list):
            literal_values = [LITERAL(value) for value in self.valid_values if value is not None]
            if None in self.valid_values:
                invalid_clauses.append(
                    AND([NOT(IN(column_expression, literal_values)), IS_NOT_NULL(column_expression)])
                )
            elif not self.valid_values:
                invalid_clauses.append(AND([LITERAL(True)]))
            else:
                invalid_clauses.append(NOT(IN(column_expression, literal_values)))
        if isinstance(self.invalid_values, list):
            literal_values = [LITERAL(value) for value in self.invalid_values if value is not None]
            if None in self.invalid_values:
                invalid_clauses.append(AND([IN(column_expression, literal_values), IS_NULL(column_expression)]))
            elif not self.invalid_values:
                invalid_clauses.append(AND([LITERAL(False)]))
            else:
                invalid_clauses.append(IN(column_expression, literal_values))
        if isinstance(self.valid_format, RegexFormat) and isinstance(self.valid_format.regex, str):
            invalid_clauses.append(NOT(REGEX_LIKE(column_expression, self.valid_format.regex)))
        if isinstance(self.valid_min, Number) or isinstance(self.valid_min, str):
            invalid_clauses.append(LT(column_expression, LITERAL(self.valid_min)))
        if isinstance(self.valid_max, Number) or isinstance(self.valid_max, str):
            invalid_clauses.append(GT(column_expression, LITERAL(self.valid_max)))
        if isinstance(self.invalid_format, RegexFormat) and isinstance(self.invalid_format.regex, str):
            invalid_clauses.append(REGEX_LIKE(column_expression, self.invalid_format.regex))
        if isinstance(self.valid_length, int):
            invalid_clauses.append(NEQ(LENGTH(column_expression), LITERAL(self.valid_length)))
        if isinstance(self.valid_min_length, int):
            invalid_clauses.append(LT(LENGTH(column_expression), LITERAL(self.valid_min_length)))
        if isinstance(self.valid_max_length, int):
            invalid_clauses.append(GT(LENGTH(column_expression), LITERAL(self.valid_max_length)))
        return OR.optional(invalid_clauses)

    def is_valid_expr(self, column_expression: str | COLUMN | SqlExpression) -> SqlExpression:
        return NOT.optional(
            OR.optional([self.is_missing_expr(column_expression), self.is_invalid_expr(column_expression)])
        )

    @classmethod
    def __apply_default(cls, self_value, default_value) -> any:
        if self_value is not None:
            return self_value
        return default_value

    def apply_column_defaults(self, column_impl: ColumnImpl) -> None:
        if not column_impl:
            return
        column_defaults: MissingAndValidity = column_impl.missing_and_validity
        if not column_defaults:
            return

        check_has_missing: bool = self.has_missing_configurations()
        self.missing_values = self.missing_values if check_has_missing else column_defaults.missing_values
        self.missing_format = self.missing_format if check_has_missing else column_defaults.missing_format

        check_has_validity: bool = self.has_validity_configurations()
        self.invalid_values = self.invalid_values if check_has_validity else column_defaults.invalid_values
        self.invalid_format = self.invalid_format if check_has_validity else column_defaults.invalid_format
        self.valid_values = self.valid_values if check_has_validity else column_defaults.valid_values
        self.valid_format = self.valid_format if check_has_validity else column_defaults.valid_format
        self.valid_min = self.valid_min if check_has_validity else column_defaults.valid_min
        self.valid_max = self.valid_max if check_has_validity else column_defaults.valid_max
        self.valid_length = self.valid_length if check_has_validity else column_defaults.valid_length
        self.valid_min_length = self.valid_min_length if check_has_validity else column_defaults.valid_min_length
        self.valid_max_length = self.valid_max_length if check_has_validity else column_defaults.valid_max_length
        self.valid_reference_data = (
            self.valid_reference_data if check_has_validity else column_defaults.valid_reference_data
        )

    def has_missing_configurations(self) -> bool:
        return self.missing_values is not None or self.missing_format is not None

    def has_validity_configurations(self) -> bool:
        return any(
            cfg is not None
            for cfg in [
                self.invalid_values,
                self.invalid_format,
                self.valid_values,
                self.valid_format,
                self.valid_min,
                self.valid_max,
                self.valid_length,
                self.valid_min_length,
                self.valid_max_length,
                self.valid_reference_data,
            ]
        )

    def has_reference_data(self) -> bool:
        return isinstance(self.valid_reference_data, ValidReferenceData)


class MetricsResolver:
    def __init__(self):
        self.metrics: list[MetricImpl] = []

    def resolve_metric(self, metric_impl: MetricImpl) -> MetricImpl:
        existing_metric_impl: Optional[MetricImpl] = next((m for m in self.metrics if m == metric_impl), None)
        if existing_metric_impl:
            return existing_metric_impl
        else:
            self.metrics.append(metric_impl)
            return metric_impl

    def get_resolved_metrics(self) -> list[MetricImpl]:
        return self.metrics


class ThresholdType(Enum):
    SINGLE_COMPARATOR = "single_comparator"
    INNER_RANGE = "inner_range"
    OUTER_RANGE = "outer_range"


class ThresholdLevel(Enum):
    FAIL = "fail"
    WARN = "warn"

    @classmethod
    def from_str(cls, level_str: str) -> ThresholdLevel:
        level_str_lower = level_str.lower()
        if level_str_lower == "fail":
            return ThresholdLevel.FAIL
        elif level_str_lower == "warn":
            return ThresholdLevel.WARN
        else:
            logger.warning(f"Unknown threshold level '{level_str}', defaulting to 'fail'")
            return ThresholdLevel.FAIL


class ThresholdImpl:
    @classmethod
    def create(
        cls, threshold_yaml: ThresholdYaml, default_threshold: Optional[ThresholdImpl] = None
    ) -> Optional[ThresholdImpl]:
        if threshold_yaml is None:
            if default_threshold:
                return default_threshold
            else:
                logger.error(f"Threshold required, but not specified")
                return None

        threshold_level: ThresholdLevel = ThresholdLevel.from_str(threshold_yaml.level)

        if default_threshold:
            default_threshold.level = threshold_level

        if not threshold_yaml.has_any_configurations():
            if default_threshold:
                return default_threshold
            logger.error(f"Threshold required, but not specified")
            return None

        if threshold_yaml.has_exactly_one_comparison():
            return ThresholdImpl(
                type=ThresholdType.SINGLE_COMPARATOR,
                level=threshold_level,
                must_be_greater_than=threshold_yaml.must_be_greater_than,
                must_be_greater_than_or_equal=threshold_yaml.must_be_greater_than_or_equal,
                must_be_less_than=threshold_yaml.must_be_less_than,
                must_be_less_than_or_equal=threshold_yaml.must_be_less_than_or_equal,
                must_be=threshold_yaml.must_be,
                must_not_be=threshold_yaml.must_not_be,
            )

        elif threshold_yaml.must_be_between:
            range_error: Optional[str] = threshold_yaml.must_be_between.get_between_range_error()
            if range_error:
                logger.error(f"Invalid between threshold range: {range_error}")
                return None
            else:
                return ThresholdImpl(
                    type=ThresholdType.INNER_RANGE,
                    level=threshold_level,
                    must_be_greater_than=threshold_yaml.must_be_between.greater_than,
                    must_be_greater_than_or_equal=threshold_yaml.must_be_between.greater_than_or_equal,
                    must_be_less_than=threshold_yaml.must_be_between.less_than,
                    must_be_less_than_or_equal=threshold_yaml.must_be_between.less_than_or_equal,
                )

        elif threshold_yaml.must_be_not_between:
            range_error: Optional[str] = threshold_yaml.must_be_not_between.get_not_between_range_error()
            if range_error:
                logger.error(f"Invalid not between threshold range: {range_error}")
                return None
            else:
                return ThresholdImpl(
                    type=ThresholdType.OUTER_RANGE,
                    level=threshold_level,
                    must_be_greater_than=threshold_yaml.must_be_not_between.greater_than,
                    must_be_greater_than_or_equal=threshold_yaml.must_be_not_between.greater_than_or_equal,
                    must_be_less_than=threshold_yaml.must_be_not_between.less_than,
                    must_be_less_than_or_equal=threshold_yaml.must_be_not_between.less_than_or_equal,
                )

    def __init__(
        self,
        type: ThresholdType,
        level: ThresholdLevel = ThresholdLevel.FAIL,
        must_be_greater_than: Optional[Number] = None,
        must_be_greater_than_or_equal: Optional[Number] = None,
        must_be_less_than: Optional[Number] = None,
        must_be_less_than_or_equal: Optional[Number] = None,
        must_be: Optional[Number] = None,
        must_not_be: Optional[Number] = None,
    ):
        self.type: ThresholdType = type
        self.level: ThresholdLevel = level
        self.must_be_greater_than: Optional[Number] = must_be_greater_than
        self.must_be_greater_than_or_equal: Optional[Number] = must_be_greater_than_or_equal
        self.must_be_less_than: Optional[Number] = must_be_less_than
        self.must_be_less_than_or_equal: Optional[Number] = must_be_less_than_or_equal
        self.must_be: Optional[Number] = must_be
        self.must_not_be: Optional[Number] = must_not_be

    def to_threshold_info(self) -> Threshold:
        if self.must_be is None and self.must_not_be is None:
            return Threshold(
                level=self.level.value,
                must_be_greater_than=self.must_be_greater_than,
                must_be_greater_than_or_equal=self.must_be_greater_than_or_equal,
                must_be_less_than=self.must_be_less_than,
                must_be_less_than_or_equal=self.must_be_less_than_or_equal,
            )
        elif self.must_be is not None:
            return Threshold(
                level=self.level.value,
                must_be=self.must_be,
            )
        elif self.must_not_be is not None:
            return Threshold(
                level=self.level.value,
                must_not_be=self.must_not_be,
            )

    @classmethod
    def get_metric_name(cls, metric_name: str, column_impl: Optional[ColumnImpl]) -> str:
        if column_impl:
            return f"{metric_name}({column_impl.column_yaml.name})"
        else:
            return metric_name

    def get_assertion_summary(self, metric_name: str) -> str:
        """
        For ease of reading, thresholds always list small values lef and big values right (where applicable).
        Eg '0 < metric_name', 'metric_name < 25', '0 <= metric_name < 25'
        """
        if self.type == ThresholdType.SINGLE_COMPARATOR:
            if isinstance(self.must_be_greater_than, Number):
                return f"{self.must_be_greater_than} < {metric_name}"
            if isinstance(self.must_be_greater_than_or_equal, Number):
                return f"{self.must_be_greater_than_or_equal} <= {metric_name}"
            if isinstance(self.must_be_less_than, Number):
                return f"{metric_name} < {self.must_be_less_than}"
            if isinstance(self.must_be_less_than_or_equal, Number):
                return f"{metric_name} <= {self.must_be_less_than_or_equal}"
            if isinstance(self.must_be, Number):
                return f"{metric_name} = {self.must_be}"
            if isinstance(self.must_not_be, Number):
                return f"{metric_name} != {self.must_not_be}"
        elif self.type == ThresholdType.INNER_RANGE or self.type == ThresholdType.OUTER_RANGE:
            gt_comparator: str = " < " if isinstance(self.must_be_greater_than, Number) else " <= "
            gt_bound: str = (
                str(self.must_be_greater_than)
                if isinstance(self.must_be_greater_than, Number)
                else str(self.must_be_greater_than_or_equal)
            )
            lt_comparator: str = " < " if isinstance(self.must_be_less_than, Number) else " <= "
            lt_bound: str = (
                str(self.must_be_less_than)
                if isinstance(self.must_be_less_than, Number)
                else str(self.must_be_less_than_or_equal)
            )
            if self.type == ThresholdType.INNER_RANGE:
                return f"{gt_bound}{gt_comparator}{metric_name}{lt_comparator}{lt_bound}"
            else:
                return f"{metric_name}{lt_comparator}{lt_bound} or {gt_bound}{gt_comparator}{metric_name}"

    def passes(self, value: Number) -> bool:
        is_greater_than_ok: bool = (self.must_be_greater_than is None or value > self.must_be_greater_than) and (
            self.must_be_greater_than_or_equal is None or value >= self.must_be_greater_than_or_equal
        )
        is_less_than_ok: bool = (self.must_be_less_than is None or value < self.must_be_less_than) and (
            self.must_be_less_than_or_equal is None or value <= self.must_be_less_than_or_equal
        )
        if self.type == ThresholdType.OUTER_RANGE:
            return is_greater_than_ok or is_less_than_ok
        else:
            return (
                is_greater_than_ok
                and is_less_than_ok
                and (not (isinstance(self.must_be, Number) or isinstance(self.must_be, str)) or value == self.must_be)
                and (
                    not (isinstance(self.must_not_be, Number) or isinstance(self.must_not_be, str))
                    or value != self.must_not_be
                )
            )


class CheckParser(ABC):
    @abstractmethod
    def get_check_type_names(self) -> list[str]:
        pass

    @abstractmethod
    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: CheckYaml,
    ) -> Optional[CheckImpl]:
        pass


class CheckImpl:
    check_parsers: dict[str, CheckParser] = {}

    @classmethod
    def register(cls, check_parser: CheckParser) -> None:
        for check_type_name in check_parser.get_check_type_names():
            cls.check_parsers[check_type_name] = check_parser

    @classmethod
    def get_check_type_names(cls) -> list[str]:
        return list(cls.check_parsers.keys())

    @classmethod
    def parse_check(
        cls,
        contract_impl: ContractImpl,
        check_yaml: CheckYaml,
        column_impl: Optional[ColumnImpl] = None,
    ) -> Optional[CheckImpl]:
        if isinstance(check_yaml.type_name, str):
            check_parser: Optional[CheckParser] = cls.check_parsers.get(check_yaml.type_name)
            if check_parser:
                check_impl = check_parser.parse_check(
                    contract_impl=contract_impl,
                    column_impl=column_impl,
                    check_yaml=check_yaml,
                )

                if not check_impl.skip:
                    check_impl.setup_metrics(
                        contract_impl=contract_impl,
                        column_impl=column_impl,
                        check_yaml=check_yaml,
                    )

                return check_impl
            else:
                logger.error(f"Unknown check type '{check_yaml.type_name}'")

    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: CheckYaml,
        extra_identity_properties: Optional[dict[str, object]] = None,
    ):
        self.logs: Logs = contract_impl.logs

        self.contract_impl: ContractImpl = contract_impl
        self.check_yaml: CheckYaml = check_yaml
        self.name: str = self._get_name_with_default(check_yaml)
        self.column_impl: Optional[ColumnImpl] = column_impl
        self.type: str = check_yaml.type_name
        self.identity: str = self._build_identity(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_type=check_yaml.type_name,
            qualifier=check_yaml.qualifier,
            extra_identity_properties=extra_identity_properties,
        )

        self.threshold: Optional[ThresholdImpl] = None
        self.metrics: list[MetricImpl] = []
        self.queries: list[Query] = []

        # Merge attributes before filtering (selectors may query them)
        self.attributes: dict[str, any] = {**contract_impl.check_attributes, **check_yaml.attributes}

        # Apply check selectors (subsumes old check_paths logic)
        self.skip: bool = not CheckSelector.all_match(contract_impl.check_selectors, self)

    @property
    def column_expression(self) -> Optional[SqlExpressionStr | COLUMN]:
        # Use check level column expression if exists, fall-back to column level check expression if possible.
        if self.check_yaml.column_expression:
            return SqlExpressionStr(self.check_yaml.column_expression)
        elif self.column_impl:
            return self.column_impl.column_expression

    __DEFAULT_CHECK_NAMES_BY_TYPE: dict[str, str] = {
        "schema": "Schema matches expected structure",
        "row_count": "Row count meets expected threshold",
        "freshness": "Data is fresh",
        "missing": "No missing values",
        "invalid": "No invalid values",
        "duplicate": "No duplicate values",
        "aggregate": "Metric function meets threshold",
        "metric": "Metric meets threshold",
        "failed_rows": "No rows violating the condition",
    }

    @property
    def path(self) -> str:
        parts: list[str] = []

        column_name = self.column_impl.column_yaml.name if self.column_impl else None
        if column_name:
            parts.append("columns")
            parts.append(column_name)

        parts.append("checks")
        parts.append(self.type)

        qualifier = self.check_yaml.qualifier
        if qualifier:
            parts.append(qualifier)

        return ".".join(parts)

    @property
    def full_path(self) -> str:
        """Wire path emitted to Soda Cloud as ``checkPath``.

        For contracts (``wire_source == "soda-contract"``) this is identical
        to ``self.path`` — byte-identical to today's emission. For
        non-contract subtypes (data standards) it is prefixed with
        ``"{collection_id}.{path}"`` so the backend's
        ``firstSegmentOf(checkPath)`` filter matches ``DataStandard.name``.

        Selector matching uses ``self.path`` (not ``full_path``) so the
        prefix never leaks into ``--check-selector`` matching.
        """
        # ``contract_impl`` is the back-ref to the enclosing
        # ``CheckCollectionImpl`` (name preserved during the rename slice;
        # rename deferred to a follow-up). The wire_source check matches
        # ``ContractImpl.wire_source`` literally so any future subtype
        # ("data-standard", etc.) automatically opts into prefixing.
        if self.contract_impl.wire_source == "soda-contract":
            return self.path
        collection_id: Optional[str] = self.contract_impl.collection_id
        # Non-contract subtypes MUST declare collection_id: the
        # ``CheckCollectionImpl.verify()`` guard raises before this property
        # is reached when collection_id is missing, but we defensively fall
        # back to the bare path so dataclass-build callers can still
        # instantiate a Check during error paths.
        if not collection_id:
            return self.path
        return f"{collection_id}.{self.path}"

    def _get_name_with_default(self, check_yaml: CheckYaml) -> str:
        if isinstance(check_yaml.name, str):
            return check_yaml.name
        default_check_name: Optional[str] = self.__DEFAULT_CHECK_NAMES_BY_TYPE.get(check_yaml.type_name)
        if isinstance(default_check_name, str):
            return default_check_name
        return check_yaml.type_name

    def _resolve_metric(self, metric_impl: MetricImpl) -> MetricImpl:
        resolved_metric_impl: MetricImpl = self.contract_impl.metrics_resolver.resolve_metric(metric_impl)
        self.metrics.append(resolved_metric_impl)
        return resolved_metric_impl

    @abstractmethod
    def setup_metrics(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: CheckYaml,
    ) -> None:
        pass

    @abstractmethod
    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        pass

    def _build_check_info(self) -> Check:
        return Check(
            type=self.type,
            qualifier=self.check_yaml.qualifier,
            name=self.name,
            path=self.path,
            full_path=self.full_path,
            identity=self.identity,
            definition=self._build_definition(),
            column_name=self.column_impl.column_yaml.name if self.column_impl else None,
            contract_file_line=self.check_yaml.check_yaml_object.location.line,
            contract_file_column=self.check_yaml.check_yaml_object.location.column,
            threshold=self._build_threshold(),
            attributes=self.attributes,
            location=self.check_yaml.check_yaml_object.location,
        )

    @classmethod
    def _build_identity(
        cls,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_type: str,
        qualifier: Optional[str],
        extra_identity_properties: Optional[dict[str, object]] = None,
    ) -> str:
        identity_hash_builder: ConsistentHashBuilder = ConsistentHashBuilder(8)

        # Identity-prefix mix-in: contracts inherit ``identity_prefix() == ()``
        # so the loop is a no-op and the hash stays byte-identical to every
        # prior contract verification (preserving Cloud history). Non-contract
        # subtypes return ``(wire_source, collection_id)`` so two collections
        # with identical check shapes on the same dataset produce distinct
        # identities. The prefix entries use stable, namespaced keys
        # (``__cc_<index>``) so the contract empty-prefix path emits zero
        # additional bytes into the blake2b stream.
        for index, prefix_value in enumerate(contract_impl.identity_prefix()):
            identity_hash_builder.add_property(f"__cc_{index}", prefix_value)

        if contract_impl.data_source_impl:
            identity_hash_builder.add_property("dso", contract_impl.data_source_impl.name)
        identity_hash_builder.add_property("pr", contract_impl.dataset_prefix)
        identity_hash_builder.add_property("ds", contract_impl.dataset_name)
        identity_hash_builder.add_property("c", column_impl.column_yaml.name if column_impl else None)
        identity_hash_builder.add_property("t", check_type)
        identity_hash_builder.add_property("q", qualifier)
        if extra_identity_properties:
            for key in sorted(extra_identity_properties):
                identity_hash_builder.add_property(key, extra_identity_properties[key])

        return identity_hash_builder.get_hash()

    def build_identity_path(self) -> str:
        parts: list[Optional[str]] = [
            self.contract_impl.contract_yaml.yaml_source.file_path,
            self.column_impl.column_yaml.name if self.column_impl else None,
            self.type,
            self.check_yaml.qualifier if self.check_yaml else None,
        ]
        parts = [p for p in parts if p is not None]
        return "/".join(parts)

    def _build_definition(self) -> str:
        contract_dict: dict = {}
        if self.contract_impl.contract_yaml.filter:
            contract_dict["filter"] = self.contract_impl.contract_yaml.filter

        check_dict: dict = self.check_yaml.check_yaml_object.yaml_dict

        if self.column_impl:
            contract_dict["columns"] = [{"name": self.column_impl.column_yaml.name, "checks": [check_dict]}]
        else:
            contract_dict["checks"] = [check_dict]

        text_stream = StringIO()
        yaml = YAML()
        yaml.dump(contract_dict, text_stream)
        text_stream.seek(0)
        return text_stream.read()

    def _build_threshold(self) -> Optional[Threshold]:
        return self.threshold.to_threshold_info() if self.threshold else None

    def get_threshold_metric_impl(self) -> Optional[MetricImpl]:
        """
        Used in extensions
        """
        raise SodaException(f"Check type '{self.type}' does not support get_threshold_metric_impl'")

    def evaluate_threshold(self, value) -> CheckOutcome:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        if self.threshold and isinstance(value, Number):
            if self.threshold.passes(value):
                outcome = CheckOutcome.PASSED
            else:
                if self.threshold.level == ThresholdLevel.WARN:
                    outcome = CheckOutcome.WARN
                else:
                    outcome = CheckOutcome.FAILED

        return outcome


class MissingAndValidityCheckImpl(CheckImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: MissingAncValidityCheckYaml,
        extra_identity_properties: Optional[dict[str, object]] = None,
    ):
        super().__init__(contract_impl, column_impl, check_yaml, extra_identity_properties=extra_identity_properties)
        self.missing_and_validity: MissingAndValidity = MissingAndValidity(missing_and_validity_yaml=check_yaml)
        self.missing_and_validity.apply_column_defaults(column_impl)


class MetricImpl:
    def __init__(
        self,
        contract_impl: ContractImpl,
        metric_type: str,
        column_impl: Optional[ColumnImpl] = None,
        check_filter: Optional[str] = None,
        missing_and_validity: Optional[MissingAndValidity] = None,
        # Associate metric with a non-contract data source if needed. Build queries accordingly.
        data_source_impl: Optional[DataSourceImpl] = None,
        # Associate metric with a non-contract dataset if needed. Build queries accordingly.
        dataset_identifier: Optional[DatasetIdentifier] = None,
        # Support user-provided column expression for type casting and structured data support.
        column_expression: Optional[SqlExpressionStr | COLUMN] = None,
    ):
        self.contract_impl: ContractImpl = contract_impl
        self.column_impl: Optional[ColumnImpl] = column_impl
        self.type: str = metric_type
        self.check_filter: Optional[str] = check_filter
        self.missing_and_validity: Optional[MissingAndValidity] = missing_and_validity
        self.dataset_identifier = dataset_identifier or contract_impl.dataset_identifier

        self.data_source_impl: Optional[DataSourceImpl] = None
        if self.contract_impl.data_source_impl:
            self.data_source_impl = self.contract_impl.data_source_impl
        if data_source_impl:
            self.data_source_impl = data_source_impl

        self.column_expression: Optional[SqlExpressionStr | COLUMN] = column_expression

        self.id: str = self._build_id()

    def _build_id(self) -> str:
        hash_builder: ConsistentHashBuilder = ConsistentHashBuilder(hash_string_length=8)
        id_properties: dict[str, any] = self._get_id_properties()
        for k, v in id_properties.items():
            hash_builder.add_property(k, v)
        return hash_builder.get_hash()

    def _get_id_properties(self) -> dict[str, any]:
        id_properties: dict[str, any] = {"type": self.type}

        if self.data_source_impl:
            id_properties["data_source"] = self.data_source_impl.name
        id_properties["dataset"] = self.dataset_identifier.dataset_name
        if self.column_impl:
            id_properties["column"] = self.column_impl.column_yaml.name
        if self.check_filter:
            id_properties["check_filter"] = self.check_filter
        if self.missing_and_validity:
            id_properties["missing_values"] = self.missing_and_validity.missing_values
            if self.missing_and_validity.missing_format:
                id_properties["missing_format_regex"] = self.missing_and_validity.missing_format.regex
            id_properties["invalid_values"] = self.missing_and_validity.invalid_values
            if self.missing_and_validity.invalid_format:
                id_properties["invalid_format_regex"] = self.missing_and_validity.invalid_format.regex
            id_properties["valid_values"] = self.missing_and_validity.valid_values
            if self.missing_and_validity.valid_format:
                id_properties["valid_format"] = self.missing_and_validity.valid_format.regex
            id_properties["valid_min"] = self.missing_and_validity.valid_min
            id_properties["valid_max"] = self.missing_and_validity.valid_max
            id_properties["valid_length"] = self.missing_and_validity.valid_length
            id_properties["valid_min_length"] = self.missing_and_validity.valid_min_length
            id_properties["valid_max_length"] = self.missing_and_validity.valid_max_length
            if self.missing_and_validity.valid_reference_data:
                id_properties["ref_prefix"] = self.missing_and_validity.valid_reference_data.dataset_prefix
                id_properties["ref_name"] = self.missing_and_validity.valid_reference_data.dataset_name
                id_properties["ref_column"] = self.missing_and_validity.valid_reference_data.column
        if self.column_expression:
            id_properties["column_expression"] = str(self.column_expression)

        return id_properties

    def __eq__(self, other):
        if type(other) != type(self):
            return False
        return self.id == other.id

    @abstractmethod
    def sql_condition_expression(self) -> Optional[SqlExpression]:
        pass


class AggregationMetricImpl(MetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        metric_type: str,
        column_impl: Optional[ColumnImpl] = None,
        check_filter: Optional[str] = None,
        missing_and_validity: Optional[MissingAndValidity] = None,
        data_source_impl: Optional[DataSourceImpl] = None,
        dataset_identifier: Optional[DatasetIdentifier] = None,
        column_expression: Optional[SqlExpressionStr | COLUMN] = None,
    ):
        super().__init__(
            contract_impl=contract_impl,
            metric_type=metric_type,
            column_impl=column_impl,
            check_filter=check_filter,
            missing_and_validity=missing_and_validity,
            data_source_impl=data_source_impl,
            dataset_identifier=dataset_identifier,
            column_expression=column_expression,
        )

    @abstractmethod
    def sql_expression(self) -> SqlExpression:
        pass

    @abstractmethod
    def sql_condition_expression(self) -> SqlExpression:
        """
        Used in extensions
        """

    def convert_db_value(self, value: any) -> any:
        return value

    def get_short_description(self) -> str:
        return self.type


class DerivedMetricImpl(MetricImpl, ABC):
    @abstractmethod
    def get_metric_dependencies(self) -> list[MetricImpl]:
        pass

    @abstractmethod
    def compute_derived_value(self, measurement_values: MeasurementValues) -> Optional[Number]:
        pass

    def gather_dependency_values(self, measurement_values: MeasurementValues) -> Optional[list]:
        """Return the dependency values in declaration order, or None if any is unmeasured.

        Use at the top of `compute_derived_value` to short-circuit when an upstream
        aggregation query failed: returning None from the derived metric causes the
        consuming check to evaluate to NOT_EVALUATED, not crash on None arithmetic
        and not falsely PASS against a 0 default.
        """
        values = [measurement_values.get_value(d) for d in self.get_metric_dependencies()]
        return None if any(v is None for v in values) else values

    def _get_id_properties(self) -> dict[str, any]:
        id_properties: dict[str, any] = super()._get_id_properties()
        for index, metric_dependency in enumerate(self.get_metric_dependencies()):
            id_properties[str(index)] = metric_dependency.id
        return id_properties


class DerivedPercentageMetricImpl(DerivedMetricImpl):
    def __init__(self, metric_type: str, fraction_metric_impl: MetricImpl, total_metric_impl: MetricImpl):
        self.fraction_metric_impl: MetricImpl = fraction_metric_impl
        self.total_metric_impl: MetricImpl = total_metric_impl
        # Mind the ordering as the self._build_id() must come last
        super().__init__(
            contract_impl=fraction_metric_impl.contract_impl,
            column_impl=fraction_metric_impl.column_impl,
            metric_type=metric_type,
            check_filter=None,
        )

    def get_metric_dependencies(self) -> list[MetricImpl]:
        return [self.fraction_metric_impl, self.total_metric_impl]

    def compute_derived_value(self, measurement_values: MeasurementValues) -> Optional[Number]:
        values = self.gather_dependency_values(measurement_values)
        if values is None:
            return None
        fraction, total = values
        return (fraction * 100 / total) if total != 0 else 0


class ValidCountMetric(AggregationMetricImpl):
    """TODO -- 3/10/2025: this metric is not used anywhere in the codebase, it's not clear if it's needed."""

    def __init__(self, contract_impl: ContractImpl, column_impl: ColumnImpl, check_impl: MissingAndValidityCheckImpl):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            metric_type="valid_count",
            check_filter=check_impl.check_yaml.filter,
            missing_and_validity=check_impl.missing_and_validity,
        )

    def sql_expression(self) -> SqlExpression:
        column_name = self.column_impl.column_yaml.name
        filters: list = [SqlExpressionStr.optional(self.check_filter)]
        if self.missing_and_validity:
            filters.append(NOT(self.missing_and_validity.is_missing_expr(column_name)))
            filters.append(NOT.optional(self.missing_and_validity.is_invalid_expr(column_name)))
        if filters:
            return SUM(CASE_WHEN(AND.optional(filters), LITERAL(1)))
        else:
            return COUNT(column_name)

    def convert_db_value(self, value) -> int:
        # Note: expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if
        # there are no rows
        return int(value) if value is not None else 0


class Query(ABC):
    def __init__(
        self, data_source_impl: Optional[DataSourceImpl], metrics: list[MetricImpl], sql: Optional[str] = None
    ):
        self.data_source_impl: DataSourceImpl = data_source_impl
        self.metrics: list[MetricImpl] = metrics
        self.sql: Optional[str] = sql

    def build_sql(self) -> str:
        """
        Signals the query building process is done.  The framework assumes that after this call the self.sql is initialized with the query string.
        """
        return self.sql

    @abstractmethod
    def execute(self) -> list[Measurement]:
        pass


class AggregationQuery(Query):
    def __init__(
        self,
        cte: CTE,
        dataset_prefix: list[str],
        dataset_name: str,
        data_source_impl: Optional[DataSourceImpl],
        logs: Logs,
    ):
        super().__init__(data_source_impl=data_source_impl, metrics=[])
        self.cte: CTE = cte
        self.dataset_prefix: list[str] = dataset_prefix
        self.dataset_name: str = dataset_name
        self.aggregation_metrics: list[AggregationMetricImpl] = []
        self.data_source_impl: DataSourceImpl = data_source_impl
        self.query_size: int = len(self.build_sql())
        self.logs: Logs = logs

    def can_accept(self, aggregation_metric_impl: AggregationMetricImpl) -> bool:
        max_query_length: int = self.data_source_impl.sql_dialect.get_max_sql_statement_length()
        return self.query_size + self._estimate_metric_sql_length(aggregation_metric_impl) < max_query_length

    def append_aggregation_metric(self, aggregation_metric_impl: AggregationMetricImpl) -> None:
        # Track cumulative SQL size as metrics are added — `self.query_size` was
        # otherwise frozen at the constructor's `COUNT(*)`-only estimate, so the
        # splitter under-estimated for every metric past the first and could exceed
        # get_max_sql_statement_length() at execute time on large contracts.
        self.query_size += self._estimate_metric_sql_length(aggregation_metric_impl)
        self.aggregation_metrics.append(aggregation_metric_impl)

    def _estimate_metric_sql_length(self, aggregation_metric_impl: AggregationMetricImpl) -> int:
        # Measure the SQL as it will actually be emitted, including the per-position
        # ALIAS wrapper that build_field_expressions adds (`<expr> AS "m_<n>"`).
        aliased = ALIAS(aggregation_metric_impl.sql_expression(), f"m_{len(self.aggregation_metrics)}")
        return len(self.data_source_impl.sql_dialect.build_expression_sql(aliased))

    def build_sql(self) -> str:
        field_expressions: list[SqlExpression] = self.build_field_expressions()
        select = [
            WITH([self.cte]),
            SELECT(field_expressions),
            FROM(self.cte.alias),
        ]
        self.sql = self.data_source_impl.sql_dialect.build_select_sql(select)
        return self.sql

    def build_field_expressions(self) -> list[SqlExpression]:
        if len(self.aggregation_metrics) == 0:
            # This is to get the initial query length in the constructor
            return [COUNT(STAR())]
        # Wrap each aggregation expression with a per-position alias so the result
        # schema always has unique column names. Two metrics may render to byte-identical
        # SQL (e.g. MissingCheckImpl and InvalidCheckImpl both resolve a MissingCount
        # metric that produces the same SUM(CASE WHEN col IS NULL ...) expression);
        # without aliases Spark 4.x rejects the query with "Can't unify schema with
        # duplicate field names". Positional row access in execute() is unaffected.
        return [
            ALIAS(aggregation_metric.sql_expression(), f"m_{i}")
            for i, aggregation_metric in enumerate(self.aggregation_metrics)
        ]

    def execute(self) -> list[Measurement]:
        sql = self.build_sql()
        try:
            query_result: QueryResult = self.data_source_impl.execute_query(sql)
        except Exception as e:
            if invalid_regex_exc := InvalidRegexException.should_raise(e, sql):
                raise invalid_regex_exc
            raise SodaCoreException(f"Could not execute aggregation query: {e}") from e

        measurements: list[Measurement] = []
        row: tuple = query_result.rows[0]
        for i in range(0, len(self.aggregation_metrics)):
            aggregation_metric_impl: AggregationMetricImpl = self.aggregation_metrics[i]
            measurement_value = aggregation_metric_impl.convert_db_value(row[i])
            measurements.append(
                Measurement(
                    metric_id=aggregation_metric_impl.id,
                    value=measurement_value,
                    metric_name=aggregation_metric_impl.get_short_description(),
                )
            )
        return measurements
