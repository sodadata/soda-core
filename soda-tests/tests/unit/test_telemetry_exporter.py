from unittest.mock import patch

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from soda_core.telemetry.soda_exporter import SodaOTLPSpanExporter
from soda_core.telemetry.soda_telemetry import SodaTelemetry


def test_soda_otlp_exporter_forwards_arguments_by_name():
    with patch.object(OTLPSpanExporter, "__init__", return_value=None) as otlp_init:
        SodaOTLPSpanExporter(
            endpoint="https://collector.example/v1/traces",
            certificate_file="ca.pem",
            headers={"h": "v"},
            timeout=SodaTelemetry.EXPORT_TIMEOUT_SECONDS,
        )

    otlp_init.assert_called_once_with(
        endpoint="https://collector.example/v1/traces",
        certificate_file="ca.pem",
        headers={"h": "v"},
        timeout=SodaTelemetry.EXPORT_TIMEOUT_SECONDS,
        compression=None,
    )


def test_real_otlp_exporter_gets_the_timeout():
    exporter = SodaOTLPSpanExporter(endpoint="https://collector.example/v1/traces", timeout=2)

    assert exporter._timeout == 2
    assert exporter._client_cert is None
