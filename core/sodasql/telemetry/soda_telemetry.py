from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

ENDPOINT = 'https://collect.dev.sodadata.io/v1/traces'


class SodaTelemetry:

    def __init__(self):
        provider = TracerProvider()
        console_span_processor = BatchSpanProcessor(ConsoleSpanExporter())
        provider.add_span_processor(console_span_processor)
        # otlp_exporter = OTLPSpanExporter(endpoint=ENDPOINT)
        # otlp_processor = BatchSpanProcessor(otlp_exporter)
        # provider.add_span_processor(otlp_processor)
        trace.set_tracer_provider(provider)
