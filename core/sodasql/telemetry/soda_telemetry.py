import hashlib
import json
import logging
import platform
from typing import Dict


from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from requests.sessions import session

from sodasql.__version__ import SODA_SQL_VERSION
from sodasql.common.config_helper import ConfigHelper
from sodasql.scan.dialect import Dialect

logger = logging.getLogger(__name__)
soda_config = ConfigHelper.get_instance()

class SodaTelemetry:
    ENDPOINT = 'https://collect.dev.sodadata.io/v1/traces'
    __skip = False
    soda_config = ConfigHelper.get_instance()

    def __init__(self,  skip: bool = False):
        self.__skip = skip
        if self.__skip:
            logger.info("Skipping usage telemetry.")
        else:
            logger.info("Sending usage telemetry.")
            self.__setup()

    def __setup(self):
        session

        provider = TracerProvider(
            resource=Resource.create(
                {
                    'os.architecture': platform.architecture(),
                    'python.version': platform.python_version(),
                    'python.implementation': platform.python_implementation(),
                    ResourceAttributes.OS_TYPE: platform.system(),
                    ResourceAttributes.OS_VERSION: platform.version(),
                    'platform': platform.platform(),
                    ResourceAttributes.SERVICE_VERSION: SODA_SQL_VERSION,
                    ResourceAttributes.SERVICE_NAME: 'soda',
                    ResourceAttributes.SERVICE_NAMESPACE: 'soda-sql',
                }
            )
        )
        console_span_processor = BatchSpanProcessor(ConsoleSpanExporter())
        provider.add_span_processor(console_span_processor)
        # otlp_exporter = OTLPSpanExporter(endpoint=ENDPOINT)
        # otlp_processor = BatchSpanProcessor(otlp_exporter)
        # provider.add_span_processor(otlp_processor)
        trace.set_tracer_provider(provider)

    def set_attribute(self, key: str, value: str) -> None:
        if not self.__skip:
            current_span = trace.get_current_span()
            current_span.set_attribute(key, value)

    def obtain_datasource_hash(self, connection: Dialect):
        return connection.generate_hash_safe()

    @property
    def user_cookie_id(self) -> str:
        return self.soda_config.get_value('user_cookie_id')

# Global
soda_telemetry = SodaTelemetry(skip=soda_config.skip_telemetry)