import json
from collections.abc import Sequence

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult


class MemorySpanExporter(SpanExporter):
    """Implementation of :class:`SpanExporter` that saves spans in memory.

    This class can be used for diagnostic purposes, multi-threaded scenarios etc.
    """

    __instance = None
    __spans = []

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
            cls.__instance._initialize()
        return cls.__instance

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        for span in spans:
            self.__spans.append(span)
        return SpanExportResult.SUCCESS

    def reset(self):
        self.__spans = []

    @property
    def spans(self) -> list[ReadableSpan]:
        return self.__spans

    @property
    def span_dicts(self) -> list[dict]:
        return [json.loads(span.to_json()) for span in self.spans]
