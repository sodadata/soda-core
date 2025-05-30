import platform
import types
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.semconv.resource import ResourceAttributes
from soda_core.__version__ import SODA_CORE_VERSION
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.contracts.contract_verification import ContractVerificationSessionResult
from soda_core.telemetry.memory_span_exporter import MemorySpanExporter
from soda_core.telemetry.soda_exporter import (
    SodaConsoleSpanExporter,
    SodaOTLPSpanExporter,
)


class SodaTelemetry:
    """Soda Open Telemetry tracing.

    Minimal, anonymous usage telemetry for Soda Core.
    """

    ENDPOINT = "https://collect.soda.io/v1/traces"
    __instance = None

    @staticmethod
    def get_instance(test_mode: bool = False):
        if test_mode:
            SodaTelemetry.__instance = None

        if SodaTelemetry.__instance is None:
            SodaTelemetry(test_mode=test_mode)
        return SodaTelemetry.__instance

    def __init__(self, test_mode: bool):
        if SodaTelemetry.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            SodaTelemetry.__instance = self

        self.soda_config = EnvConfigHelper()

        self.__send = self.soda_config.soda_core_telemetry_enabled

        if self.__send:
            self.__provider = TracerProvider(
                resource=Resource.create(
                    {
                        "os.architecture": platform.architecture(),
                        "python.version": platform.python_version(),
                        "python.implementation": platform.python_implementation(),
                        ResourceAttributes.OS_TYPE: platform.system(),
                        ResourceAttributes.OS_VERSION: platform.version(),
                        "platform": platform.platform(),
                        ResourceAttributes.SERVICE_VERSION: SODA_CORE_VERSION,
                        ResourceAttributes.SERVICE_NAME: "soda",
                        ResourceAttributes.SERVICE_NAMESPACE: "soda-core",
                    }
                )
            )

            if test_mode:
                self.__setup_for_test()
            else:
                self.__setup()

    def __setup(self):
        """Set up Open Telemetry processors and exporters for normal use."""
        local_debug_mode = self.soda_config.soda_core_telemetry_local_debug_mode

        if local_debug_mode:
            self.__provider.add_span_processor(BatchSpanProcessor(SodaConsoleSpanExporter()))

        if not local_debug_mode:
            otlp_exporter = SodaOTLPSpanExporter(endpoint=self.ENDPOINT)
            otlp_processor = BatchSpanProcessor(otlp_exporter)
            self.__provider.add_span_processor(otlp_processor)

        trace.set_tracer_provider(self.__provider)

    def __setup_for_test(self):
        self.__provider.add_span_processor(SimpleSpanProcessor(MemorySpanExporter.get_instance()))

        trace.set_tracer_provider(self.__provider)

    def set_attribute(self, key: str, value: str) -> None:
        """Set attribute value in the current span."""
        if self.__send:
            current_span = trace.get_current_span()
            current_span.set_attribute(key, value)

    def set_attributes(self, values: dict[str, str]) -> None:
        """Set attributes the current span."""
        if self.__send:
            current_span = trace.get_current_span()
            current_span.set_attributes(values)

    @staticmethod
    def get_datasource_hash(data_source: DataSourceImpl):
        return data_source.generate_safe_safe()

    def ingest_cli_arguments(self, args: dict[str, Any]) -> None:
        for key, value in args.items():
            self.set_attribute(f"cli__{key}", self.__serialize_arg_value(value))

    def ingest_contract_verification_session_result(
        self, contract_verification_session_result: ContractVerificationSessionResult
    ) -> None:
        self.set_attributes(
            {
                "result__checks_count": contract_verification_session_result.get_number_of_checks(),
                "result__checks_failed_count": contract_verification_session_result.get_number_of_checks_failed(),
                "result__checks_passed_count": contract_verification_session_result.get_number_of_checks_passed(),
            }
        )

    def __serialize_arg_value(self, value):
        if isinstance(value, types.FunctionType):
            return f"{value.__module__}.{value.__qualname__}"
        elif callable(value):
            return f"{type(value).__name__}::{getattr(value, '__name__', repr(value))}"
        elif value is None:
            return ""
        else:
            return value
