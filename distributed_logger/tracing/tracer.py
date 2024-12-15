from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from datetime import datetime
from typing import Dict, Optional

class ServiceTracer:
    """
    Handles distributed tracing across services
    """
    def __init__(self, service_name: str):
        """
        Initialize tracer for a service
        
        Args:
            service_name: Name of the service using this tracer
        """
        # Create and set the tracer provider
        provider = TracerProvider()
        
        # Add console exporter to see traces in console/logs
        processor = BatchSpanProcessor(ConsoleSpanExporter())
        provider.add_span_processor(processor)
        
        # Set as global tracer provider
        trace.set_tracer_provider(provider)
        
        # Get a tracer for this service
        self.tracer = trace.get_tracer(service_name)
        
        # For passing trace context between services
        self.propagator = TraceContextTextMapPropagator()
        
        print(f"Initialized tracer for service: {service_name}")

    def start_span(self, name: str, context: Optional[Dict] = None) -> trace.Span:
        """
        Start a new trace span
        
        Args:
            name: Name/description of the operation
            context: Optional context from parent span
        """
        # If we have context from another service, use it
        if context:
            ctx = self.propagator.extract(carrier=context)
            span = self.tracer.start_span(name, context=ctx)
        else:
            span = self.tracer.start_span(name)
            
        print(f"Started span: {name}")
        return span

    def inject_headers(self, headers: Dict) -> Dict:
        """
        Add trace context to request headers
        
        Args:
            headers: Dict of HTTP headers
        """
        self.propagator.inject(carrier=headers)
        return headers

    def get_span_data(self, span: trace.Span) -> Dict:
        """
        Get tracing data from a span for logging
        
        Args:
            span: The active span
        """
        context = span.get_span_context()
        return {
            "trace_id": format(context.trace_id, "032x"),
            "span_id": format(context.span_id, "016x"),
            "timestamp": datetime.now().isoformat()
        }