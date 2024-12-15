from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, BatchSpanProcessor

def test_otel():
    # Set up the tracer
    provider = TracerProvider()
    processor = BatchSpanProcessor(ConsoleSpanExporter())
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)

    # Create a tracer
    tracer = trace.get_tracer(__name__)

    # Create a span
    with tracer.start_span("test_span") as span:
        span.set_attribute("test_attribute", "test_value")
        print("OpenTelemetry is working!")

if __name__ == "__main__":
    test_otel()