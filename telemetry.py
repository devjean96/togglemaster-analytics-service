import atexit
import os
import threading

from opentelemetry import metrics, trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

_initialization_lock = threading.Lock()
_initialized = False


def _resource_attributes(default_service_name):
    attributes = {"service.name": os.getenv("OTEL_SERVICE_NAME", default_service_name)}
    raw_attributes = os.getenv("OTEL_RESOURCE_ATTRIBUTES", "")
    for item in raw_attributes.split(","):
        key, separator, value = item.partition("=")
        if separator and key.strip() and value.strip():
            attributes[key.strip()] = value.strip()
    return attributes


def init_telemetry(flask_app, default_service_name):
    """Inicializa OpenTelemetry antes dos clientes Boto3 e da thread SQS."""
    global _initialized

    if os.getenv("OTEL_SDK_DISABLED", "false").lower() == "true":
        return

    with _initialization_lock:
        if _initialized:
            return

        resource = Resource.create(_resource_attributes(default_service_name))

        tracer_provider = TracerProvider(resource=resource)
        tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(tracer_provider)

        metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter())
        meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        metrics.set_meter_provider(meter_provider)

        set_global_textmap(
            CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator()])
        )
        FlaskInstrumentor().instrument_app(flask_app, excluded_urls="health")
        BotocoreInstrumentor().instrument()

        def shutdown():
            meter_provider.shutdown()
            tracer_provider.shutdown()

        atexit.register(shutdown)
        _initialized = True
