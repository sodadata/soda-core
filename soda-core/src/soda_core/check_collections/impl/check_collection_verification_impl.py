from __future__ import annotations

from abc import ABC
from datetime import timezone
from enum import Enum
from io import StringIO
from logging import LogRecord
from typing import Any, ClassVar, Generic, Protocol, TypeVar, get_args, get_origin

from ruamel.yaml import YAML
from soda_core.check_collections.check_collection_spec import CheckCollectionSpec
from soda_core.check_collections.check_collection_verification import (
    Check,
    CheckCollectionResult,
    CheckCollectionSessionResult,
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationStatus,
    DataSource,
    Measurement,
    PostProcessingStage,
    PostProcessingStageState,
    SodaException,
    Threshold,
    YamlFileContentInfo,
)
from soda_core.check_collections.impl.check_collection_yaml import (
    CheckCollectionYaml,
    CheckYaml,
    ColumnYaml,
    MissingAncValidityCheckYaml,
    MissingAndValidityYaml,
    RegexFormat,
    ThresholdYaml,
    ValidReferenceDataYaml,
)
from soda_core.common.consistent_hash_builder import ConsistentHashBuilder
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.exceptions import (
    InvalidRegexException,
    SodaCoreException,
    get_exception_stacktrace,
)
from soda_core.common.logging_constants import Emoticons, ExtraKeys
from soda_core.common.logs import Location, Logs
from soda_core.common.metadata_types import SamplerType
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.soda_cloud_converter import map_sampler_type_from_dto
from soda_core.common.soda_cloud_dto import DatasetConfigurationDTO
from soda_core.common.sql_dialect import *
from soda_core.common.yaml import (
    CheckCollectionYamlSource,
    DataSourceYamlSource,
    SodaCloudYamlSource,
)
from soda_core.contracts.impl.check_selector import CheckSelector
from tabulate import tabulate

logger: logging.Logger = soda_logger


class CheckCollectionVerificationHandler(ABC):
    @abstractmethod
    def handle(
        self,
        check_collection_impl: CheckCollectionImpl,
        data_source_impl: Optional[DataSourceImpl],
        check_collection_verification_result: CheckCollectionResult,
        soda_cloud: SodaCloud,
        soda_cloud_send_results_response_json: dict,
        dwh_data_source_file_path: Optional[str] = None,
    ):
        pass

    @abstractmethod
    def provides_post_processing_stages(self) -> list[PostProcessingStage]:
        pass


class CheckCollectionVerificationHandlerRegistry(ABC):
    check_collection_verification_handlers: list[CheckCollectionVerificationHandler] = []
    post_processing_stages: dict[str, CheckCollectionVerificationHandler] = {}

    @classmethod
    def register(cls, verification_handler: CheckCollectionVerificationHandler) -> None:
        cls.check_collection_verification_handlers.append(verification_handler)
        for stage in verification_handler.provides_post_processing_stages():
            stage_name = stage.name
            if stage_name in cls.post_processing_stages:
                logger.warning(f"Overriding existing verification handler for check type {stage_name}")
            cls.post_processing_stages[stage_name] = verification_handler


YamlT = TypeVar("YamlT", bound=CheckCollectionYaml)
ResultT = TypeVar("ResultT", bound="CheckCollectionResult")


class CheckCollectionVerificationSessionImpl:
    """Universal verification session impl.

    A session run takes a heterogeneous ``list[CheckCollectionSpec]`` — each
    spec carries its ``kind`` and the session impl dispatches via the
    ``CheckCollection`` registry. The legacy ``check_collection_yaml_sources``
    kwarg is kept as a BC bridge that wraps each source in a
    ``kind="contract"`` spec.
    """

    @classmethod
    def execute(
        cls,
        check_collection_yaml_sources: Optional[list[CheckCollectionYamlSource]] = None,
        specs: Optional[list[CheckCollectionSpec]] = None,
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
    ) -> CheckCollectionSessionResult:
        logs: Logs = Logs()

        # BC bridge: callers that pass check_collection_yaml_sources get each
        # source wrapped in a kind="contract" spec — preserves the previous
        # contract-only execution path while heterogeneous callers move to
        # ``specs=`` directly.
        #
        # Reject passing both — symmetric with ``ContractVerificationSession.execute``
        # which raises ``TypeError`` on both being passed. Silently dropping one
        # input was a footgun for callers migrating from the BC kwarg to the
        # canonical ``specs=``.
        if specs is not None and check_collection_yaml_sources is not None:
            raise TypeError("Pass either specs= (canonical) or check_collection_yaml_sources= (BC), not both")
        if specs is None:
            if check_collection_yaml_sources is None:
                raise ValueError("Either specs or check_collection_yaml_sources must be provided.")
            if not isinstance(check_collection_yaml_sources, list):
                raise TypeError("check_collection_yaml_sources must be a list")
            if not all(
                isinstance(check_collection_yaml_source, CheckCollectionYamlSource)
                for check_collection_yaml_source in check_collection_yaml_sources
            ):
                raise TypeError("check_collection_yaml_sources must contain CheckCollectionYamlSource instances")
            specs = [CheckCollectionSpec(kind="contract", yaml_source=src) for src in check_collection_yaml_sources]
        else:
            if not isinstance(specs, list):
                raise TypeError("specs must be a list")
            if not all(isinstance(spec, CheckCollectionSpec) for spec in specs):
                raise TypeError("specs must contain CheckCollectionSpec instances")

        # Validate input variables
        if variables is None:
            variables = {}
        else:
            if not isinstance(variables, dict):
                raise TypeError("variables must be a dict")
            if not all(isinstance(k, str) and isinstance(v, (str, Number)) for k, v in variables.items()):
                raise TypeError("variables keys must be str and values must be str or numeric")

        # Validate input data_source_impls
        if data_source_impls is None:
            data_source_impls = []
        else:
            if not isinstance(data_source_impls, list):
                raise TypeError("data_source_impls must be a list")
            if not all(isinstance(data_source_impl, DataSourceImpl) for data_source_impl in data_source_impls):
                raise TypeError("data_source_impls must contain DataSourceImpl instances")

        # Validate input data_source_yaml_sources
        if data_source_yaml_sources is None:
            data_source_yaml_sources = []
        else:
            if not isinstance(data_source_yaml_sources, list):
                raise TypeError("data_source_yaml_sources must be a list")
            if not all(
                isinstance(data_source_yaml_source, DataSourceYamlSource) or soda_cloud_use_agent
                for data_source_yaml_source in data_source_yaml_sources
            ):
                raise TypeError(
                    "data_source_yaml_sources must contain DataSourceYamlSource instances "
                    "(or soda_cloud_use_agent=True)"
                )

        # Validate input soda_cloud_impl
        if soda_cloud_impl is not None and not isinstance(soda_cloud_impl, SodaCloud):
            raise TypeError("soda_cloud_impl must be a SodaCloud instance")

        # Validate flag inputs
        if not isinstance(soda_cloud_publish_results, bool):
            raise TypeError("soda_cloud_publish_results must be a bool")
        if not isinstance(soda_cloud_use_agent, bool):
            raise TypeError("soda_cloud_use_agent must be a bool")
        if not isinstance(soda_cloud_use_agent_blocking_timeout_in_minutes, int):
            raise TypeError("soda_cloud_use_agent_blocking_timeout_in_minutes must be an int")

        if check_selectors is None:
            check_selectors = []

        if soda_cloud_use_agent:
            check_collection_results: list = cls._execute_on_agent(
                specs=specs,
                variables=variables,
                soda_cloud_impl=soda_cloud_impl,
                soda_cloud_use_agent_blocking_timeout_in_minutes=soda_cloud_use_agent_blocking_timeout_in_minutes,
                soda_cloud_publish_results=soda_cloud_publish_results,
                soda_cloud_verbose=soda_cloud_verbose,
            )

        else:
            check_collection_results: list = cls._execute_locally(
                logs=logs,
                specs=specs,
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
        return CheckCollectionSessionResult(check_collection_results=check_collection_results)

    @classmethod
    def _execute_locally(
        cls,
        logs: Logs,
        specs: list[CheckCollectionSpec],
        only_validate_without_execute: bool,
        provided_variable_values: dict[str, str],
        data_timestamp: Optional[str],
        data_source_impls: list[DataSourceImpl],
        data_source_yaml_sources: list[DataSourceYamlSource],
        soda_cloud_impl: Optional[SodaCloud],
        soda_cloud_publish_results: bool,
        check_selectors: list[CheckSelector],
        dwh_data_source_file_path: Optional[str] = None,
    ) -> list[CheckCollectionResult]:
        "Verifies a heterogeneous list of check collections locally, dispatching per-spec via the registry."
        from soda_core.check_collections.check_collection import CheckCollection

        check_collection_results: list = []

        data_source_impls_by_name: dict[str, DataSourceImpl] = cls._build_data_source_impls_by_name(
            data_source_impls=data_source_impls,
            data_source_yaml_sources=data_source_yaml_sources,
            provided_variable_values=provided_variable_values,
        )

        opened_data_sources: list[DataSourceImpl] = []
        try:
            for idx, spec in enumerate(specs):
                # Per-spec isolation: a parse or runtime failure on one spec must
                # not abort the session and lose results for later specs. The result
                # list stays positional with the input ``specs`` — consumers (e.g.
                # backend ingestion) may match results to inputs by index, so we
                # always append exactly one entry per spec, an ERROR-status
                # ``CheckCollectionResult`` placeholder when the spec failed before
                # producing a real result.
                #
                # All ``Exception`` subtypes (including ``SodaCoreException``) are
                # isolated here — both parse-time and runtime failures stay
                # per-spec, matching the design doc's "parse or runtime failure
                # isolated" contract and the multi-collection robustness backend
                # ingestion expects. Legacy single-contract callers that
                # ``pytest.raises(YamlParserException)`` on caller-input errors are
                # preserved by ``ContractVerificationSession.execute`` re-raising
                # the captured exception when there is exactly one ERROR result and
                # it carries a ``SodaCoreException`` (see the facade in
                # ``contracts/contract_verification.py``). ``BaseException``
                # subclasses (``KeyboardInterrupt`` / ``SystemExit``) propagate.
                #
                # ``CheckCollection.get(spec.kind)`` is inside the try so that an
                # unregistered kind also isolates per-spec rather than aborting the
                # whole session — the result list stays positional even when the
                # descriptor lookup itself fails.
                descriptor = None
                try:
                    descriptor = CheckCollection.get(spec.kind)
                    check_collection_yaml: CheckCollectionYaml = descriptor.yaml_class.parse(
                        check_collection_yaml_source=spec.yaml_source,
                        provided_variable_values=provided_variable_values,
                        data_timestamp=data_timestamp,
                        primary_data_source_impl=data_source_impls_by_name.get("primary_datasource"),
                    )
                    data_source_name: str = (
                        check_collection_yaml.dataset[: check_collection_yaml.dataset.find("/")]
                        if check_collection_yaml.dataset
                        else None
                    )
                    data_source_impl: Optional[DataSourceImpl] = (
                        cls._get_data_source_impl(data_source_name, data_source_impls_by_name, opened_data_sources)
                        if (check_collection_yaml and data_source_name and not only_validate_without_execute)
                        else None
                    )
                    check_collection_impl: CheckCollectionImpl = descriptor.impl_class(
                        check_collection_yaml=check_collection_yaml,
                        only_validate_without_execute=only_validate_without_execute,
                        data_timestamp=check_collection_yaml.data_timestamp,
                        execution_timestamp=check_collection_yaml.execution_timestamp,
                        data_source_impl=data_source_impl,
                        all_data_source_impls=data_source_impls_by_name,
                        soda_cloud=soda_cloud_impl,
                        publish_results=soda_cloud_publish_results,
                        logs=logs,
                        check_selectors=check_selectors,
                        dwh_data_source_file_path=dwh_data_source_file_path,
                        collection_name=spec.collection_name,
                    )
                    check_collection_result = check_collection_impl.verify()
                    check_collection_results.append(check_collection_result)
                except Exception as exc:
                    logger.error(
                        msg=(
                            f"Could not verify spec[{idx}] "
                            f"({spec.kind}, {spec.yaml_source}): "
                            f"{type(exc).__name__}: {exc}"
                        ),
                        exc_info=True,
                    )
                    check_collection_results.append(
                        _build_error_result(spec=spec, descriptor=descriptor, exception=exc)
                    )
        finally:
            for data_source_impl in opened_data_sources:
                data_source_impl.close_connection()
        return check_collection_results

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
    def _get_data_source_impl(
        cls,
        data_source_name: Optional[str],
        data_source_impl_by_name: dict[str, DataSourceImpl],
        opened_data_sources: list[DataSourceImpl],
    ) -> Optional[DataSourceImpl]:
        if data_source_name is None:
            return None
        data_source_impl: Optional[DataSourceImpl] = data_source_impl_by_name.get(data_source_name)
        if isinstance(data_source_impl, DataSourceImpl):
            if not data_source_impl.has_open_connection():
                data_source_impl.open_connection()
                opened_data_sources.append(data_source_impl)
            return data_source_impl
        else:
            logger.error(f"Data source '{data_source_name}' not found")

    @classmethod
    def _execute_on_agent(
        cls,
        specs: list[CheckCollectionSpec],
        variables: dict[str, str],
        soda_cloud_impl: Optional[SodaCloud],
        soda_cloud_use_agent_blocking_timeout_in_minutes: int,
        soda_cloud_publish_results: bool,
        soda_cloud_verbose: bool,
    ) -> list[CheckCollectionResult]:
        "Verifies a heterogeneous list of check collections on the Soda Cloud agent, dispatching per-spec via the registry."
        from soda_core.check_collections.check_collection import CheckCollection

        check_collection_results: list = []

        for idx, spec in enumerate(specs):
            # Per-spec isolation: keep the result list positional with the input ``specs``
            # so callers can match results to inputs by index even when one spec fails.
            # Symmetric with ``_execute_locally``: all ``Exception`` subtypes
            # (including ``SodaCoreException``, ``KeyError`` on unknown kind, and
            # ``NotImplementedError`` from a descriptor with ``on_agent_verifier=None``)
            # are isolated into an ERROR-status placeholder. Facade-level legacy
            # preservation (single-contract ``pytest.raises(YamlParserException)``
            # callers) lives in ``ContractVerificationSession.execute``.
            descriptor = None
            try:
                descriptor = CheckCollection.get(spec.kind)
                display_name = descriptor.impl_class._DISPLAY_NAME
                if descriptor.on_agent_verifier is None:
                    raise NotImplementedError(f"{display_name} does not support agent execution")
                check_collection_yaml: CheckCollectionYaml = descriptor.yaml_class.parse(
                    check_collection_yaml_source=spec.yaml_source, provided_variable_values=variables
                )
                check_collection_result = descriptor.on_agent_verifier(
                    soda_cloud_impl=soda_cloud_impl,
                    check_collection_yaml=check_collection_yaml,
                    variables=variables,
                    blocking_timeout_in_minutes=soda_cloud_use_agent_blocking_timeout_in_minutes,
                    publish_results=soda_cloud_publish_results,
                    verbose=soda_cloud_verbose,
                )
                check_collection_results.append(check_collection_result)
            except Exception as exc:
                logger.error(
                    msg=(
                        f"Could not verify spec[{idx}] "
                        f"({spec.kind}, {spec.yaml_source}): "
                        f"{type(exc).__name__}: {exc}"
                    ),
                    exc_info=True,
                )
                check_collection_results.append(_build_error_result(spec=spec, descriptor=descriptor, exception=exc))
        return check_collection_results


def _build_error_result(
    spec: CheckCollectionSpec,
    descriptor: Optional["CheckCollection"],
    exception: BaseException,
) -> CheckCollectionResult:
    """Build an ERROR-status ``CheckCollectionResult`` placeholder for a failed spec.

    Per-spec isolation contract: when a spec fails before producing a real
    result, the session impl appends this placeholder so the result list stays
    positional with the input ``specs``. The placeholder carries
    ``status=ContractVerificationStatus.ERROR``, no check results, and a single
    ERROR-level log record whose message names the spec's kind plus the
    exception's class and message. The originating exception is stored on a
    public ``originating_exception`` attribute so the contract-typed facade
    (``ContractVerificationSession.execute``) can re-raise it for legacy
    single-input callers that ``pytest.raises(SodaCoreException)``.

    Subtype consumers (e.g. a future ``DataStandardsVerificationResult``
    adapter) iterating ``check_collection_results`` get a uniform shape with
    no per-slot ``None`` guard.

    ``descriptor`` is ``None`` when the spec failed before the registry lookup
    succeeded (unknown ``kind`` → ``KeyError`` from ``CheckCollection.get``).
    In that case we synthesize a base-class ``CheckCollectionResult`` carrying
    an empty ``wire_source`` so the placeholder still has positional integrity
    even though no successful upload to Cloud will follow (the registration
    was broken in the first place).
    """
    now = datetime.now(tz=timezone.utc)
    if descriptor is not None:
        # Direct read — ``CheckCollection.register`` validates ``_WIRE_SOURCE`` is
        # a non-empty str at registration time so a missing attribute here is a
        # real invariant breach worth failing loud on.
        wire_source = descriptor.impl_class._WIRE_SOURCE
        display_name = descriptor.impl_class._DISPLAY_NAME
        result_cls = getattr(descriptor.impl_class, "_RESULT_CLASS", CheckCollectionResult)
    else:
        # No descriptor available — spec.kind never resolved. Empty wire_source
        # is documented above; the placeholder exists for positional integrity
        # rather than for successful Cloud ingestion.
        wire_source = ""
        display_name = f"unknown kind {spec.kind!r}"
        result_cls = CheckCollectionResult
    contract = Contract(
        data_source_name=None,
        dataset_prefix=[],
        dataset_name=spec.collection_name or "",
        soda_qualified_dataset_name=spec.collection_name or "",
        source=YamlFileContentInfo(
            source_content_str=None,
            local_file_path=getattr(spec.yaml_source, "file_path", None),
        ),
        wire_source=wire_source,
    )
    # Synthesize a single ERROR-level LogRecord so ``get_errors()`` /
    # ``get_logs()`` return non-empty for this placeholder.
    log_record = logging.LogRecord(
        name="soda_core.check_collections.impl.check_collection_verification_impl",
        level=logging.ERROR,
        pathname="",
        lineno=0,
        msg=f"Could not verify {display_name} (spec.kind={spec.kind!r}): {type(exception).__name__}: {exception}",
        args=(),
        exc_info=None,
    )
    return result_cls.error_placeholder(
        contract=contract,
        log_record=log_record,
        originating_exception=exception,
        started_timestamp=now,
        ended_timestamp=now,
    )


class CheckCollectionImplExtension(Protocol):
    def __init__(self, check_collection_impl: CheckCollectionImpl):
        self.check_collection_impl: CheckCollectionImpl = check_collection_impl

    def parse_checks(self, check_collection_impl: CheckCollectionImpl) -> list[CheckImpl]:
        return []

    def build_queries(self, check_collection_impl: CheckCollectionImpl) -> list[Query]:
        return []


class CheckCollectionImpl(Generic[YamlT, ResultT]):
    """Implements the check-collection runtime.

    Subtypes subscribe ``Generic[YamlT, ResultT]`` to wire their result type
    (``ContractImpl(CheckCollectionImpl[ContractYaml, ContractVerificationResult])``)
    and declare three identity ClassVars: ``_KIND`` (on the paired YAML
    subclass), ``_DISPLAY_NAME`` (user-facing word), and ``_WIRE_SOURCE``
    (per-check Cloud ``source``; no base default — subtypes MUST set it).
    """

    check_collection_impl_extensions: dict[str, type[CheckCollectionImplExtension]] = {}

    _RESULT_CLASS: ClassVar[type[CheckCollectionResult]]
    _DISPLAY_NAME: ClassVar[str] = "check collection"
    # No base default: a missing _WIRE_SOURCE raises AttributeError at first
    # read rather than silently uploading under the abstract base's name.
    _WIRE_SOURCE: ClassVar[str]
    # Scan-definition-type literal that means "test verification on agent" for
    # this subtype. None on the base → is_test_verification_on_agent returns
    # False (no test-mode opinion). Subtypes that support test-mode runs on
    # the Soda agent set their own literal (e.g. "contractTest").
    _TEST_SCAN_DEFINITION_TYPE: ClassVar[Optional[str]] = None

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        # Derive _RESULT_CLASS from Generic[YamlT, ResultT] subscription.
        # YamlT is phantom on this class; ResultT is the runtime hook.
        for base in getattr(cls, "__orig_bases__", ()):
            origin = get_origin(base)
            if origin is None or not isinstance(origin, type):
                continue
            if not issubclass(origin, CheckCollectionImpl):
                continue
            args = get_args(base)
            if len(args) != 2:
                continue
            _yaml_type, result_type = args
            if isinstance(result_type, type):
                cls._RESULT_CLASS = result_type
            break

        # Concrete subtypes (those that wired _RESULT_CLASS this iteration)
        # must declare _WIRE_SOURCE. _KIND / _DISPLAY_NAME have safe base
        # defaults; _WIRE_SOURCE does not, and a missing value would silently
        # upload checks under the abstract base's name.
        #
        # Walk MRO via ``any`` short-circuit rather than flattening every
        # ancestor's ``__dict__`` into a set comprehension — cheaper and more
        # readable: we only need to know whether some ancestor (excluding the
        # base CheckCollectionImpl) declares the attribute.
        if "_RESULT_CLASS" in cls.__dict__:
            has_wire_source_in_ancestors = any(
                "_WIRE_SOURCE" in c.__dict__ for c in cls.__mro__ if c is not CheckCollectionImpl
            )
            if not has_wire_source_in_ancestors:
                raise TypeError(f"{cls.__name__}: concrete CheckCollectionImpl subtype must declare _WIRE_SOURCE")

    @classmethod
    def register_extension(cls, name: str, extension_cls: type[CheckCollectionImplExtension]) -> None:
        cls.check_collection_impl_extensions[name] = extension_cls

    def __init__(
        self,
        logs: Logs,
        check_collection_yaml: CheckCollectionYaml,
        only_validate_without_execute: bool,
        data_source_impl: Optional[DataSourceImpl],
        all_data_source_impls: dict[str, DataSourceImpl],
        data_timestamp: datetime,
        execution_timestamp: datetime,
        soda_cloud: Optional[SodaCloud],
        publish_results: bool,
        check_selectors: list[CheckSelector] = [],
        dwh_data_source_file_path: Optional[str] = None,
        collection_name: Optional[str] = None,
    ):
        self.logs: Logs = logs
        self.check_collection_yaml: CheckCollectionYaml = check_collection_yaml
        self.only_validate_without_execute: bool = only_validate_without_execute
        self.data_source_impl: DataSourceImpl = data_source_impl
        self.all_data_source_impls: dict[str, DataSourceImpl] = all_data_source_impls
        self.soda_cloud: Optional[SodaCloud] = soda_cloud
        self.publish_results: bool = publish_results
        self.soda_config = EnvConfigHelper()

        # Subtype-supplied identifier used for path prefixing and identity
        # disambiguation. None for contracts.
        self.collection_name: Optional[str] = collection_name

        self.filter: Optional[str] = self.check_collection_yaml.filter
        self.check_selectors: list[CheckSelector] = check_selectors

        self.started_timestamp: datetime = datetime.now(tz=timezone.utc)

        self.execution_timestamp: datetime = execution_timestamp
        self.data_timestamp: datetime = data_timestamp

        self.dataset_name: Optional[str] = None

        self.check_attributes: dict[str, Any] = check_collection_yaml.check_attributes

        self.dataset_identifier = DatasetIdentifier.parse(check_collection_yaml.dataset)
        self.dataset_prefix: list[str] = self.dataset_identifier.prefixes
        self.dataset_name = self.dataset_identifier.dataset_name

        self.metrics_resolver: MetricsResolver = MetricsResolver()

        self.column_impls: list[ColumnImpl] = []
        self.check_impls: list[CheckImpl] = []

        # TODO replace usage of self.soda_qualified_dataset_name with self.dataset_identifier
        self.soda_qualified_dataset_name = check_collection_yaml.dataset
        # TODO replace usage of self.sql_qualified_dataset_name with self.dataset_identifier
        self.sql_qualified_dataset_name: Optional[str] = None

        self.datasource_warehouse: Optional[str] = None
        self.compute_warehouse: Optional[str] = None

        if data_source_impl:
            # TODO replace usage of self.sql_qualified_dataset_name with self.dataset_identifier
            self.sql_qualified_dataset_name = data_source_impl.sql_dialect.qualify_dataset_name(
                dataset_prefix=self.dataset_prefix, dataset_name=self.dataset_name
            )

            if data_source_impl.data_source_connection:
                if hasattr(data_source_impl.data_source_connection.connection_properties, "warehouse"):
                    self.datasource_warehouse = data_source_impl.data_source_connection.connection_properties.warehouse

                if self.datasource_warehouse is None:
                    self.datasource_warehouse = data_source_impl.get_current_warehouse()

        from soda_core.contracts.impl.check_types.row_count_check import (
            RowCountMetricImpl,
        )

        self.row_count_metric_impl: MetricImpl = self.metrics_resolver.resolve_metric(
            RowCountMetricImpl(check_collection_impl=self)
        )
        self.dataset_rows_tested: Optional[int] = None

        # Dataset defining CTE - used as basis for all queries in this check collection
        self.cte = CTE("_soda_filtered_dataset").AS(
            [
                SELECT(STAR()),
                FROM(self.dataset_identifier.dataset_name, self.dataset_identifier.prefixes),
                WHERE.optional(SqlExpressionStr.optional(self.filter)),
            ]
        )
        # Optional sampler configuration. Is there a better place or way to store this?
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
                f"with sampler config: type:'{self.dataset_configuration.test_row_sampler_configuration.test_row_sampler.type}', limit:'{self.dataset_configuration.test_row_sampler_configuration.test_row_sampler.limit}'"
            )

            # This modifies the CTE to include sampling by accessing the first element of the cte_query list, may be flaky. Consider adding a better way to modify queries, or change AST to a 3rd party library which may support it already.
            self.cte.cte_query[1] = self.cte.cte_query[1].SAMPLE(
                self.sampler_type,
                self.sampler_limit,
            )

        self.extensions: list[CheckCollectionImplExtension] = []
        for extension_cls in CheckCollectionImpl.check_collection_impl_extensions.values():
            try:
                extension = extension_cls(self)
                self.extensions.append(extension)
            except Exception as e:
                logger.error(
                    f"Error extending check-collection implementation with extension {extension_cls.__name__}: {e}",
                )

        self.column_impls: list[ColumnImpl] = self._parse_columns(check_collection_yaml=check_collection_yaml)
        self.check_impls: list[CheckImpl] = self._parse_checks(check_collection_yaml)

        dataset_check_impls: list[CheckImpl] = list(self.check_impls)
        column_check_impls: list[CheckImpl] = []
        for column_impl in self.column_impls:
            column_check_impls.extend(column_impl.check_impls)
        # For consistency and predictability, we want the checks eval and results in the same order as in the check-collection YAML
        self.all_check_impls: list[CheckImpl] = (
            dataset_check_impls + column_check_impls
            if self._dataset_checks_came_before_columns_in_yaml()
            else column_check_impls + dataset_check_impls
        )

        self._verify_duplicate_identities(self.all_check_impls)
        self.metrics: list[MetricImpl] = self.metrics_resolver.get_resolved_metrics()

        self.queries: list[Query] = []
        if data_source_impl:
            self.queries = self._build_queries()

        self.dwh_data_source_file_path: Optional[str] = dwh_data_source_file_path

    @property
    def is_test_verification_on_agent(self) -> bool:
        """True iff this run is a test-mode verification on the Soda agent.

        Returns ``False`` when the subtype hasn't declared
        ``_TEST_SCAN_DEFINITION_TYPE`` (i.e. has no test-mode opinion).
        """
        test_type = type(self)._TEST_SCAN_DEFINITION_TYPE
        if test_type is None:
            return False
        return self.soda_config.is_running_on_agent and self.soda_config.soda_scan_definition_type == test_type

    @property
    def is_sampling_enabled(self) -> bool:
        return self.sampler_type is not None and self.sampler_limit is not None

    @property
    def should_apply_sampling(self) -> bool:
        return self.is_test_verification_on_agent and self.is_sampling_enabled

    def _dataset_checks_came_before_columns_in_yaml(self) -> Optional[bool]:
        check_collection_keys: list[str] = self.check_collection_yaml.check_collection_yaml_object.keys()
        if "checks" in check_collection_keys and "columns" in check_collection_keys:
            return check_collection_keys.index("checks") < check_collection_keys.index("columns")
        return None

    def _get_data_timestamp(
        self, resolved_variable_values: dict[str, str], soda_variable_values: dict[str, str], default: datetime
    ) -> Optional[datetime]:
        # Skipped for 'NOW' :)
        return None

    def _parse_checks(self, check_collection_yaml: CheckCollectionYaml) -> list[CheckImpl]:
        check_impls: list[CheckImpl] = []
        if check_collection_yaml.checks:
            for check_yaml in check_collection_yaml.checks:
                if check_yaml:
                    check = CheckImpl.parse_check(check_collection_impl=self, check_yaml=check_yaml)
                    check_impls.append(check)

        for extension in self.extensions:
            try:
                check_impls.extend(extension.parse_checks(check_collection_impl=self))
            except Exception as e:
                logger.error(f"Error parsing checks with extension {extension.__class__.__name__}: {e}")

        return check_impls

    def _build_queries(self) -> list[Query]:
        queries: list[Query] = []
        aggregation_metrics: list[AggregationMetricImpl] = []
        for check in self.all_check_impls:
            queries.extend(check.queries)

        for metric in self.metrics:
            # Only build aggregation queries for metrics of known origin. Extensions might build their own queries.
            # Known origin means that the metric does not have any specific datasource/dataset associated or the ones that are linked are the same as the check collection's datasource and dataset.
            if isinstance(metric, AggregationMetricImpl):
                if (metric.data_source_impl is None and metric.dataset_identifier is None) or (
                    metric.data_source_impl == self.data_source_impl
                    and metric.dataset_identifier == self.dataset_identifier
                ):
                    aggregation_metrics.append(metric)

        from soda_core.contracts.impl.check_types.schema_check import SchemaQuery

        schema_queries: list[SchemaQuery] = []
        other_queries: list[SchemaQuery] = []
        for query in queries:
            if isinstance(query, SchemaQuery):
                schema_queries.append(query)
            else:
                other_queries.append(query)

        aggregation_queries: list[AggregationQuery] = []
        for aggregation_metric in aggregation_metrics:
            if len(aggregation_queries) == 0 or not aggregation_queries[-1].can_accept(aggregation_metric):
                aggregation_queries.append(
                    AggregationQuery(
                        cte=self.cte,
                        dataset_prefix=self.dataset_prefix,
                        dataset_name=self.dataset_name,
                        data_source_impl=self.data_source_impl,
                        logs=self.logs,
                    )
                )
            last_aggregation_query: AggregationQuery = aggregation_queries[-1]
            last_aggregation_query.append_aggregation_metric(aggregation_metric)

        all_queries: list[Query] = schema_queries + aggregation_queries + other_queries

        for extension in self.extensions:
            try:
                extension_queries: list[Query] = extension.build_queries(check_collection_impl=self)
                all_queries.extend(extension_queries)
            except Exception as e:
                logger.error(f"Error building queries with extension {extension.__class__.__name__}: {e}")

        return all_queries

    def _parse_columns(self, check_collection_yaml: CheckCollectionYaml) -> list[ColumnImpl]:
        columns: list[ColumnImpl] = []
        if check_collection_yaml.columns:
            for column_yaml in check_collection_yaml.columns:
                if column_yaml:
                    column = ColumnImpl(check_collection_impl=self, column_yaml=column_yaml)
                    columns.append(column)
        return columns

    def verify(self) -> ResultT:
        result_cls: type = type(self)._RESULT_CLASS

        if self.data_source_impl and self.soda_config.is_running_on_agent:
            self.data_source_impl.switch_warehouse(self.compute_warehouse, check_collection_impl=self)
        data_source: Optional[DataSource] = None
        check_results: list[CheckResult] = []
        measurements: list[Measurement] = []
        contract_verification_status: ContractVerificationStatus = ContractVerificationStatus.UNKNOWN
        dataset_rows_tested: Optional[int] = None

        verb: str = "Validating" if self.only_validate_without_execute else "Verifying"
        logger.info(
            f"{verb} {type(self)._DISPLAY_NAME} {Emoticons.SCROLL} "
            f"{self.check_collection_yaml.check_collection_yaml_source.file_path} {Emoticons.FINGERS_CROSSED}"
        )

        if self.data_source_impl:
            data_source = self.data_source_impl.build_data_source()

        if self.logs.has_errors:
            contract_verification_status = ContractVerificationStatus.ERROR

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
            derived_metric_impls: list[DerivedMetricImpl] = [
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
                        check_result: CheckResult = check_impl.evaluate(measurement_values=measurement_values)
                    check_results.append(check_result)

            contract_verification_status = _get_contract_verification_status(self.logs.has_errors, check_results)

            log_lines = self.build_log_summary(
                soda_qualified_dataset_name=self.soda_qualified_dataset_name, check_results=check_results
            )
            for line in log_lines:
                logger.info(line)

        log_records: Optional[list[LogRecord]] = self.logs.pop_log_records()

        soda_cloud_file_id: Optional[str] = None
        sending_results_to_soda_cloud_failed: bool = False
        check_collection_yaml_source_str_original = (
            self.check_collection_yaml.check_collection_yaml_source.yaml_str_original
        )
        soda_cloud_response_json: Optional[dict] = None

        if self.soda_cloud and self.publish_results:
            soda_cloud_file_id = self.soda_cloud._upload_contract_yaml_file(check_collection_yaml_source_str_original)

        post_processing_stages: list[PostProcessingStage] = []
        for (
            check_collection_verification_handler
        ) in CheckCollectionVerificationHandlerRegistry.post_processing_stages.values():
            post_processing_stages += check_collection_verification_handler.provides_post_processing_stages()

        check_collection_result = result_cls(
            contract=Contract(
                data_source_name=self.data_source_impl.name if self.data_source_impl else None,
                dataset_prefix=self.dataset_prefix,
                dataset_name=self.dataset_name,
                soda_qualified_dataset_name=self.soda_qualified_dataset_name,
                source=YamlFileContentInfo(
                    source_content_str=check_collection_yaml_source_str_original,
                    local_file_path=self.check_collection_yaml.check_collection_yaml_source.file_path,
                    soda_cloud_file_id=soda_cloud_file_id,
                ),
                dataset_id=None,  # Set this to None for now (default). Will be filled later when we get the dataset id from Soda Cloud
                # Per-check Cloud upload ``source`` literal is dispatched off the
                # subtype's ClassVar. Future subtypes (e.g. DataStandardImpl with
                # _WIRE_SOURCE="data-standard") flow through here automatically;
                # the upload site reads ``contract.wire_source`` rather than
                # hardcoding a literal.
                wire_source=type(self)._WIRE_SOURCE,
            ),
            data_source=data_source,
            data_timestamp=self.data_timestamp,
            started_timestamp=self.started_timestamp,
            ended_timestamp=datetime.now(tz=timezone.utc),
            measurements=measurements,
            check_results=check_results,
            sending_results_to_soda_cloud_failed=sending_results_to_soda_cloud_failed,
            status=contract_verification_status,
            log_records=log_records,
            post_processing_stages=post_processing_stages,
        )

        scan_id: Optional[str] = None
        if soda_cloud_file_id:
            if data_source is None:
                logger.error(
                    f"Not sending results to Soda Cloud {Emoticons.CROSS_MARK} "
                    f"Data source not found. Check that the data source name in the {type(self)._DISPLAY_NAME}'s "
                    f"'dataset' field matches the name in your data source configuration."
                )
                sending_results_to_soda_cloud_failed = True
                check_collection_result.sending_results_to_soda_cloud_failed = True
            else:
                # send_contract_result will use contract.source.soda_cloud_file_id
                soda_cloud_response_json = self.soda_cloud.send_contract_result(check_collection_result)
                scan_id = soda_cloud_response_json.get("scanId") if soda_cloud_response_json else None
                if not scan_id:
                    check_collection_result.sending_results_to_soda_cloud_failed = True
                else:
                    check_collection_result.scan_id = scan_id
                    # Put the dataset id in the contract object
                    check_collection_result.contract.dataset_id = self.__get_dataset_id(
                        soda_cloud_response_json, self.soda_qualified_dataset_name
                    )
        else:
            logger.debug(f"Not sending results to Soda Cloud {Emoticons.CROSS_MARK}")

        for (
            check_collection_verification_handler
        ) in CheckCollectionVerificationHandlerRegistry.check_collection_verification_handlers:
            try:
                check_collection_verification_handler.handle(
                    check_collection_impl=self,
                    data_source_impl=self.data_source_impl,
                    check_collection_verification_result=check_collection_result,
                    soda_cloud=self.soda_cloud,
                    soda_cloud_send_results_response_json=soda_cloud_response_json,
                    dwh_data_source_file_path=self.dwh_data_source_file_path,
                )
            except Exception as e:
                logger.error(f"Error in verification handler: {e}", exc_info=True)
                self._handle_post_processing_failure(
                    scan_id=scan_id, exc=e, check_collection_verification_handler=check_collection_verification_handler
                )

        return check_collection_result

    def __get_dataset_id(self, soda_cloud_response_json: dict, qualified_dataset_name: str) -> Optional[str]:
        # Find the dataset id for the given qualified dataset name
        for check in soda_cloud_response_json.get("checks", []):
            for datasets in check.get("datasets", []):
                dataset_dqn: Optional[str] = datasets.get("dqn")
                if dataset_dqn and dataset_dqn == qualified_dataset_name:
                    return datasets.get("id")
        return None

    def build_log_summary(self, soda_qualified_dataset_name: str, check_results: list[CheckResult]) -> list[str]:
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

        summary_lines.append(f"\n### {type(self)._DISPLAY_NAME.capitalize()} results for {soda_qualified_dataset_name}")
        summary_lines.append(self.build_summary_table(check_results))

        overview_table = tabulate(table_lines, tablefmt="github", stralign="left")
        summary_lines.append(f"# Summary:\n{overview_table}\n")

        return [line for joined_line in summary_lines for line in joined_line.split("\n")]

    def build_summary_table(self, check_results: list[CheckResult]) -> str:
        overview_table_data = [check_result.log_table_row() for check_result in check_results]

        # Sort by column name, check name and check outcome
        overview_table_data.sort(key=lambda row: (row["Column"], row["Check"], row["Outcome"]))

        # Re-iterate rows data and remove column name if it is the same as the previous row
        previous_column_name: Optional[str] = None
        for row in overview_table_data:
            if previous_column_name == row["Column"]:
                row["Column"] = ""  # Clear column name if it is the same as the previous row
            else:
                previous_column_name = row["Column"]

        return tabulate(overview_table_data, headers="keys", tablefmt="grid")

    @classmethod
    def _verify_duplicate_identities(cls, all_check_impls: list[CheckImpl]) -> None:
        checks_by_identity: dict[str, CheckImpl] = {}
        for check_impl in all_check_impls:
            existing_check_impl: Optional[CheckImpl] = checks_by_identity.get(check_impl.identity)
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

    @classmethod
    def compute_data_quality_score(cls, total_failed_rows_count: int, total_rows_count: int) -> float:
        return 100 - (total_failed_rows_count * 100 / total_rows_count)

    def _handle_post_processing_failure(
        self,
        scan_id: Optional[str],
        exc: Exception,
        check_collection_verification_handler: CheckCollectionVerificationHandler,
    ):
        if scan_id is None:
            logger.warning("Not sending post-processing stage updates to Soda Cloud - no scan ID")
            return
        if self.soda_cloud is None:
            logger.warning("Not sending post-processing stage updates to Soda Cloud - no Soda Cloud client")
            return
        for post_processing_stage in check_collection_verification_handler.provides_post_processing_stages():
            self.soda_cloud.post_processing_update(
                stage=post_processing_stage.name,
                scan_id=scan_id,
                state=PostProcessingStageState.FAILED,
                error=get_exception_stacktrace(exc),
            )


def _get_contract_verification_status(has_errors: bool, check_results: list[CheckResult]) -> ContractVerificationStatus:
    if has_errors:
        return ContractVerificationStatus.ERROR

    if any(check_result.outcome == CheckOutcome.FAILED for check_result in check_results):
        return ContractVerificationStatus.FAILED

    if any(check_result.outcome == CheckOutcome.WARN for check_result in check_results):
        return ContractVerificationStatus.WARNED

    if all(check_result.outcome == CheckOutcome.PASSED for check_result in check_results):
        return ContractVerificationStatus.PASSED

    return ContractVerificationStatus.UNKNOWN


class MeasurementValues:
    def __init__(self, measurements: list[Measurement]):
        self.metric_values_by_id: dict[str, Any] = {
            measurement.metric_id: measurement.value for measurement in measurements
        }
        self.metric_ids_being_derived: set[str] = set()

    def get_value(self, metric_impl: MetricImpl) -> Any:
        return self.metric_values_by_id.get(metric_impl.id)

    def all_measured(self, *metric_impls: MetricImpl) -> bool:
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
    def __init__(self, check_collection_impl: CheckCollectionImpl, column_yaml: ColumnYaml):
        self.column_yaml: ColumnYaml = column_yaml
        self.missing_and_validity: MissingAndValidity = MissingAndValidity(missing_and_validity_yaml=column_yaml)
        self.check_impls: list[CheckImpl] = []
        if column_yaml.check_yamls:
            for check_yaml in column_yaml.check_yamls:
                if check_yaml:
                    check = CheckImpl.parse_check(
                        check_collection_impl=check_collection_impl,
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
    def __apply_default(cls, self_value, default_value) -> Any:
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
        check_collection_impl: CheckCollectionImpl,
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
        check_collection_impl: CheckCollectionImpl,
        check_yaml: CheckYaml,
        column_impl: Optional[ColumnImpl] = None,
    ) -> Optional[CheckImpl]:
        if isinstance(check_yaml.type_name, str):
            check_parser: Optional[CheckParser] = cls.check_parsers.get(check_yaml.type_name)
            if check_parser:
                check_impl = check_parser.parse_check(
                    check_collection_impl=check_collection_impl,
                    column_impl=column_impl,
                    check_yaml=check_yaml,
                )

                if not check_impl.skip:
                    check_impl.setup_metrics(
                        check_collection_impl=check_collection_impl,
                        column_impl=column_impl,
                        check_yaml=check_yaml,
                    )

                return check_impl
            else:
                logger.error(f"Unknown check type '{check_yaml.type_name}'")

    def __init__(
        self,
        check_collection_impl: CheckCollectionImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: CheckYaml,
        extra_identity_properties: Optional[dict[str, object]] = None,
    ):
        self.logs: Logs = check_collection_impl.logs

        self.check_collection_impl: CheckCollectionImpl = check_collection_impl
        self.check_yaml: CheckYaml = check_yaml
        self.name: str = self._get_name_with_default(check_yaml)
        self.column_impl: Optional[ColumnImpl] = column_impl
        self.type: str = check_yaml.type_name
        self.identity: str = self._build_identity(
            check_collection_impl=check_collection_impl,
            column_impl=column_impl,
            check_type=check_yaml.type_name,
            qualifier=check_yaml.qualifier,
            extra_identity_properties=self._merge_identity_properties(extra_identity_properties),
        )

        self.threshold: Optional[ThresholdImpl] = None
        self.metrics: list[MetricImpl] = []
        self.queries: list[Query] = []

        # Merge attributes before filtering (selectors may query them)
        self.attributes: dict[str, Any] = {**check_collection_impl.check_attributes, **check_yaml.attributes}

        # Apply check selectors (subsumes old check_paths logic)
        self.skip: bool = not CheckSelector.all_match(check_collection_impl.check_selectors, self)

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

    def _extra_identity_properties(self) -> dict[str, object]:
        """Subtype hook for extra identity-hash inputs. Base returns ``{}``.

        Distinct from the ``extra_identity_properties`` kwarg on
        :meth:`__init__`: the kwarg carries per-call additions (e.g. reference-
        data hashing per check); this method carries per-subtype additions.
        :meth:`_merge_identity_properties` combines both — kwarg entries win
        on key collision.
        """
        return {}

    def _merge_identity_properties(
        self,
        explicit: Optional[dict[str, object]],
    ) -> Optional[dict[str, object]]:
        """Merge subtype hook contributions with caller-supplied properties.

        Explicit entries win on key collision. Returns ``None`` when both are
        empty so the call into ``_build_identity`` stays shape-identical.
        """
        hook = self._extra_identity_properties()
        if not hook and not explicit:
            return None
        merged: dict[str, object] = {**hook}
        if explicit:
            merged.update(explicit)
        return merged

    @property
    def path(self) -> str:
        """Storage / display / Cloud-upload path. Today equals :attr:`raw_path`.

        Kept as ``@property`` (rather than an ``__init__``-stored attr) so
        subtype-specific check classes (e.g. reconciliation) can override
        with a wrapped computation. Future prefixing logic composes through
        the property body, not via attribute assignment.
        """
        return self._compute_raw_path()

    @property
    def raw_path(self) -> str:
        """The path the user wrote in YAML. Read by selector matching."""
        return self._compute_raw_path()

    def _compute_raw_path(self) -> str:
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

    def _get_name_with_default(self, check_yaml: CheckYaml) -> str:
        if isinstance(check_yaml.name, str):
            return check_yaml.name
        default_check_name: Optional[str] = self.__DEFAULT_CHECK_NAMES_BY_TYPE.get(check_yaml.type_name)
        if isinstance(default_check_name, str):
            return default_check_name
        return check_yaml.type_name

    def _resolve_metric(self, metric_impl: MetricImpl) -> MetricImpl:
        resolved_metric_impl: MetricImpl = self.check_collection_impl.metrics_resolver.resolve_metric(metric_impl)
        self.metrics.append(resolved_metric_impl)
        return resolved_metric_impl

    @abstractmethod
    def setup_metrics(
        self,
        check_collection_impl: CheckCollectionImpl,
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
        check_collection_impl: CheckCollectionImpl,
        column_impl: Optional[ColumnImpl],
        check_type: str,
        qualifier: Optional[str],
        extra_identity_properties: Optional[dict[str, object]] = None,
    ) -> str:
        identity_hash_builder: ConsistentHashBuilder = ConsistentHashBuilder(8)
        if check_collection_impl.data_source_impl:
            identity_hash_builder.add_property("dso", check_collection_impl.data_source_impl.name)
        identity_hash_builder.add_property("pr", check_collection_impl.dataset_prefix)
        identity_hash_builder.add_property("ds", check_collection_impl.dataset_name)
        identity_hash_builder.add_property("c", column_impl.column_yaml.name if column_impl else None)
        identity_hash_builder.add_property("t", check_type)
        identity_hash_builder.add_property("q", qualifier)
        if extra_identity_properties:
            for key in sorted(extra_identity_properties):
                identity_hash_builder.add_property(key, extra_identity_properties[key])

        return identity_hash_builder.get_hash()

    def build_identity_path(self) -> str:
        parts: list[Optional[str]] = [
            self.check_collection_impl.check_collection_yaml.check_collection_yaml_source.file_path,
            self.column_impl.column_yaml.name if self.column_impl else None,
            self.type,
            self.check_yaml.qualifier if self.check_yaml else None,
        ]
        parts = [p for p in parts if p is not None]
        return "/".join(parts)

    def _build_definition(self) -> str:
        contract_dict: dict = {}
        if self.check_collection_impl.check_collection_yaml.filter:
            contract_dict["filter"] = self.check_collection_impl.check_collection_yaml.filter

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
        check_collection_impl: CheckCollectionImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: MissingAncValidityCheckYaml,
        extra_identity_properties: Optional[dict[str, object]] = None,
    ):
        super().__init__(
            check_collection_impl, column_impl, check_yaml, extra_identity_properties=extra_identity_properties
        )
        self.missing_and_validity: MissingAndValidity = MissingAndValidity(missing_and_validity_yaml=check_yaml)
        self.missing_and_validity.apply_column_defaults(column_impl)


class MetricImpl:
    def __init__(
        self,
        check_collection_impl: CheckCollectionImpl,
        metric_type: str,
        column_impl: Optional[ColumnImpl] = None,
        check_filter: Optional[str] = None,
        missing_and_validity: Optional[MissingAndValidity] = None,
        # Associate metric with a non-check-collection data source if needed. Build queries accordingly.
        data_source_impl: Optional[DataSourceImpl] = None,
        # Associate metric with a non-check-collection dataset if needed. Build queries accordingly.
        dataset_identifier: Optional[DatasetIdentifier] = None,
        # Support user-provided column expression for type casting and structured data support.
        column_expression: Optional[SqlExpressionStr | COLUMN] = None,
    ):
        self.check_collection_impl: CheckCollectionImpl = check_collection_impl
        self.column_impl: Optional[ColumnImpl] = column_impl
        self.type: str = metric_type
        self.check_filter: Optional[str] = check_filter
        self.missing_and_validity: Optional[MissingAndValidity] = missing_and_validity
        self.dataset_identifier = dataset_identifier or check_collection_impl.dataset_identifier

        self.data_source_impl: Optional[DataSourceImpl] = None
        if self.check_collection_impl.data_source_impl:
            self.data_source_impl = self.check_collection_impl.data_source_impl
        if data_source_impl:
            self.data_source_impl = data_source_impl

        self.column_expression: Optional[SqlExpressionStr | COLUMN] = column_expression

        self.id: str = self._build_id()

    def _build_id(self) -> str:
        hash_builder: ConsistentHashBuilder = ConsistentHashBuilder(hash_string_length=8)
        id_properties: dict[str, Any] = self._get_id_properties()
        for k, v in id_properties.items():
            hash_builder.add_property(k, v)
        return hash_builder.get_hash()

    def _get_id_properties(self) -> dict[str, Any]:
        id_properties: dict[str, Any] = {"type": self.type}

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

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return False
        return self.id == other.id

    @abstractmethod
    def sql_condition_expression(self) -> Optional[SqlExpression]:
        pass


class AggregationMetricImpl(MetricImpl):
    def __init__(
        self,
        check_collection_impl: CheckCollectionImpl,
        metric_type: str,
        column_impl: Optional[ColumnImpl] = None,
        check_filter: Optional[str] = None,
        missing_and_validity: Optional[MissingAndValidity] = None,
        data_source_impl: Optional[DataSourceImpl] = None,
        dataset_identifier: Optional[DatasetIdentifier] = None,
        column_expression: Optional[SqlExpressionStr | COLUMN] = None,
    ):
        super().__init__(
            check_collection_impl=check_collection_impl,
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

    def convert_db_value(self, value: Any) -> Any:
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

    def _get_id_properties(self) -> dict[str, Any]:
        id_properties: dict[str, Any] = super()._get_id_properties()
        for index, metric_dependency in enumerate(self.get_metric_dependencies()):
            id_properties[str(index)] = metric_dependency.id
        return id_properties


class DerivedPercentageMetricImpl(DerivedMetricImpl):
    def __init__(self, metric_type: str, fraction_metric_impl: MetricImpl, total_metric_impl: MetricImpl):
        self.fraction_metric_impl: MetricImpl = fraction_metric_impl
        self.total_metric_impl: MetricImpl = total_metric_impl
        # Mind the ordering as the self._build_id() must come last
        super().__init__(
            check_collection_impl=fraction_metric_impl.check_collection_impl,
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

    def __init__(
        self,
        check_collection_impl: CheckCollectionImpl,
        column_impl: ColumnImpl,
        check_impl: MissingAndValidityCheckImpl,
    ):
        super().__init__(
            check_collection_impl=check_collection_impl,
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
        # get_max_sql_statement_length() at execute time on large check collections.
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
