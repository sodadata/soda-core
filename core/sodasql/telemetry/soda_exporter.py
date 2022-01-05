import logging
from typing import Dict, List, Sequence

from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SpanExportResult,
    SpanExporter,
)
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter


logger = logging.getLogger(__name__)


def get_soda_spans(spans: Sequence[ReadableSpan]) -> Sequence[ReadableSpan]:
    result = []
    for span in spans:
        if span.name.startswith("soda"):
            result.append(span)
        else:
            logger.debug(f"Open Telemetry: Skipping non-soda span '{span.name}'.")

    return result


class SodaConsoleSpanExporter(ConsoleSpanExporter):
    """Soda version of console exporter.

    Does not export any non-soda spans for security and privacy reasons."""
    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        return super().export(get_soda_spans(spans))


class SodaOTLPSpanExporter(OTLPSpanExporter):
    """Soda version of OTLP exporter.

    Does not export any non-soda spans for security and privacy reasons."""
    def __init__(*args, **kwargs):
        super().__init__(*args, **kwargs)

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        return super().export(get_soda_spans(spans))
