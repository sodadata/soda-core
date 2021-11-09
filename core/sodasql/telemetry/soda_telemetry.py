import platform

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter
)

from sodasql.__version__ import SODA_SQL_VERSION
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

ENDPOINT = 'https://collect.dev.sodadata.io/v1/traces'


class SodaTelemetry:

    def __init__(self):
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
                    ResourceAttributes.SERVICE_NAME: 'soda-sql',
                }
            )
        )
        console_span_processor = BatchSpanProcessor(ConsoleSpanExporter())
        provider.add_span_processor(console_span_processor)
        # otlp_exporter = OTLPSpanExporter(endpoint=ENDPOINT)
        # otlp_processor = BatchSpanProcessor(otlp_exporter)
        # provider.add_span_processor(otlp_processor)
        trace.set_tracer_provider(provider)

# Global
soda_telmetry = SodaTelemetry()
