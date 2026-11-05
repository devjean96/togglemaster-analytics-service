from opentelemetry import propagate
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from telemetry import _resource_attributes


def test_resource_attributes_include_gitops_metadata(monkeypatch):
    monkeypatch.setenv("OTEL_SERVICE_NAME", "analytics-service")
    monkeypatch.setenv(
        "OTEL_RESOURCE_ATTRIBUTES",
        "service.namespace=togglemaster,deployment.environment.name=homolog,service.version=sha-789abc",
    )

    assert _resource_attributes("fallback") == {
        "service.name": "analytics-service",
        "service.namespace": "togglemaster",
        "deployment.environment.name": "homolog",
        "service.version": "sha-789abc",
    }


def test_sqs_consumer_continues_producer_trace(
    app_module, aws_clients, message, monkeypatch
):
    monkeypatch.setenv("OTEL_SDK_DISABLED", "false")
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(app_module, "tracer", provider.get_tracer("test/analytics"))
    propagate.set_global_textmap(TraceContextTextMapPropagator())

    message["MessageAttributes"] = {
        "traceparent": {
            "DataType": "String",
            "StringValue": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        }
    }

    app_module.process_message(message)

    consumer = next(
        span for span in exporter.get_finished_spans() if span.name == "sqs.process_evaluation"
    )
    assert format(consumer.context.trace_id, "032x") == "4bf92f3577b34da6a3ce929d0e0e4736"
