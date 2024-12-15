# distributed_logger/tracing/instrumentation.py
from flask import Flask, request
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor

def instrument_service(app: Flask) -> Flask:
    """
    Add OpenTelemetry instrumentation to a Flask service
    
    Args:
        app: Flask application to instrument
    """
    # Instrument Flask app - automatically traces all requests
    FlaskInstrumentor().instrument_app(app)
    
    # Instrument requests library - traces all HTTP requests
    RequestsInstrumentor().instrument()
    
    print("Service instrumented with OpenTelemetry")
    return app