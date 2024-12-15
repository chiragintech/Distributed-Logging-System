# distributed_logger/base_logger.py

import uuid
import time
import threading
from datetime import datetime
from typing import Dict, List, Optional, Any, ContextManager
from .fluentd_logger import FluentdLogger
import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import Status, StatusCode, Span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseLogger:
    def __init__(self, service_name: str, dependencies: List[str] = None):
        """Initialize enhanced logger with better tracing support"""
        self.service_name = service_name
        self.node_id = str(uuid.uuid4())
        self.dependencies = dependencies or []
        self.heartbeat_interval = 10 # seconds
        
        # Initialize OpenTelemetry
        self._setup_telemetry()
        
        # Initialize Fluentd logger
        self.fluentd = FluentdLogger()
        
        # Register service
        self._register_service()

        # Start heartbeat thread
        self._start_heartbeat()
        
        
        logger.info(f"Initialized BaseLogger for service: {service_name}")

    def _setup_telemetry(self):
        """Set up OpenTelemetry tracing"""
        provider = TracerProvider()
        processor = BatchSpanProcessor(ConsoleSpanExporter())
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        
        self.tracer = trace.get_tracer(__name__)
        self.propagator = TraceContextTextMapPropagator()

    @contextmanager
    def start_span(self, name: str, attributes: Dict = None) -> ContextManager[Span]:
        """Start a new trace span with context manager support"""
        attributes = attributes or {}
        attributes.update({
            "service.name": self.service_name,
            "node.id": self.node_id
        })
        
        with self.tracer.start_as_current_span(name, attributes=attributes) as span:
            yield span

    def inject_context(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Inject trace context into headers"""
        self.propagator.inject(headers)
        return headers

    def _register_service(self, status: str = "UP"):
        """Register service with system"""
        with self.start_span("service_registration") as span:
            registration_data = {
                "node_id": self.node_id,
                "message_type": "REGISTRATION",
                "service_name": self.service_name,
                "dependencies": self.dependencies,
                "status": status,  # Now uses the status parameter
                "timestamp": datetime.utcnow().isoformat()
            }
            span.set_attribute("registration.dependencies", str(self.dependencies))
            success = self.fluentd.emit_service_log(
                self.service_name, 
                "registration", 
                registration_data
            )
            if success:
                logger.info(f"Service {self.service_name} {status} registration sent")
            else:
                logger.error(f"Failed to send {status} registration for service {self.service_name}")

    def _send_heartbeat(self):
        """Send heartbeat with standardized format"""
        with self.start_span("heartbeat") as span:
            heartbeat_data = {
                "node_id": self.node_id,
                "service_name": self.service_name,
                "message_type": "HEARTBEAT",
                "status": "UP",
                "timestamp": datetime.utcnow().isoformat()
            }
            span.set_attribute("heartbeat.status", "UP")
            self.fluentd.emit_heartbeat(self.service_name, heartbeat_data)

    def _heartbeat_loop(self):
        """Generate heartbeat signals"""
        while True:
            try:
                self._send_heartbeat()
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                time.sleep(1)

    def _start_heartbeat(self):
        """Start heartbeat mechanism"""
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, 
            daemon=True,
            name=f"{self.service_name}-heartbeat"
        )
        heartbeat_thread.start()
        logger.info(f"Started heartbeat thread for {self.service_name}")

    def info(self, message: str, trace_id: Optional[str] = None, **kwargs):
        """Generate INFO level log with standardized format and tracing"""
        with self.start_span("info_log") as span:
            log_data = {
                "log_id": str(uuid.uuid4()),
                "node_id": self.node_id,
                "service_name": self.service_name,
                "log_level": "INFO",
                "message_type": "LOG",
                "message": message,
                "timestamp": datetime.utcnow().isoformat(),
                "trace_id": trace_id or span.get_span_context().trace_id,
                **kwargs
            }
            
            span.set_attribute("log.message", message)
            self.fluentd.emit_service_log(self.service_name, "logs", log_data)
            return log_data

    def warn(self, message: str, response_time_ms: Optional[float] = None, 
             threshold_limit_ms: Optional[float] = None, trace_id: Optional[str] = None, **kwargs):
        """Generate WARN level log with performance metrics and tracing"""
        with self.start_span("warning_log") as span:
            log_data = {
                "log_id": str(uuid.uuid4()),
                "node_id": self.node_id,
                "service_name": self.service_name,
                "log_level": "WARN",
                "message_type": "LOG",
                "message": message,
                "timestamp": datetime.utcnow().isoformat(),
                "trace_id": trace_id or span.get_span_context().trace_id,
                "response_time_ms": response_time_ms,
                "threshold_limit_ms": threshold_limit_ms,
                **kwargs
            }
            
            span.set_attribute("log.message", message)
            if response_time_ms:
                span.set_attribute("performance.response_time_ms", response_time_ms)
            if threshold_limit_ms:
                span.set_attribute("performance.threshold_ms", threshold_limit_ms)
            
            self.fluentd.emit_service_log(self.service_name, "logs", log_data)
            return log_data

    def error(self, message: str, error_code: Optional[str] = None, 
              error_message: Optional[str] = None, trace_id: Optional[str] = None, 
              dependent_service: Optional[str] = None, **kwargs):
        """Generate ERROR level log with error details and tracing"""
        with self.start_span("error_log") as span:
            span.set_status(Status(StatusCode.ERROR))
            
            log_data = {
                "log_id": str(uuid.uuid4()),
                "node_id": self.node_id,
                "service_name": self.service_name,
                "log_level": "ERROR",
                "message_type": "LOG",
                "message": message,
                "timestamp": datetime.utcnow().isoformat(),
                "trace_id": trace_id or span.get_span_context().trace_id,
                "error_details": {
                    "error_code": error_code,
                    "error_message": error_message
                },
                **kwargs
            }
            
            # Add dependency information for cascading errors
            if dependent_service:
                log_data["dependent_service"] = dependent_service
                span.set_attribute("error.dependent_service", dependent_service)
            
            span.set_attribute("error.code", error_code)
            span.set_attribute("error.message", error_message)
            
            self.fluentd.emit_service_log(self.service_name, "logs", log_data)
            return log_data

    def log_service_call(self, target_service: str, success: bool = True, 
                        error_details: Optional[Dict] = None, trace_id: Optional[str] = None,
                        duration_ms: Optional[float] = None):
        """Log service interactions with enhanced tracing"""
        with self.start_span("service_call") as span:
            trace_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "source_service": self.service_name,
                "target_service": target_service,
                "status": "SUCCESS" if success else "ERROR",
                "trace_id": trace_id or span.get_span_context().trace_id,
                "duration_ms": duration_ms
            }

            span.set_attribute("target.service", target_service)
            span.set_attribute("call.success", success)
            
            if duration_ms:
                span.set_attribute("call.duration_ms", duration_ms)

            if not success and error_details:
                trace_data["error_details"] = error_details
                span.set_status(Status(StatusCode.ERROR))
                span.set_attribute("error.code", error_details.get("error_code"))
                span.set_attribute("error.message", error_details.get("error_message"))
            
            self.fluentd.emit_trace(self.service_name, trace_data)
            return trace_data

    def cleanup(self):
        """Cleanup resources"""
        try:
            self.fluentd.close()
            logger.info(f"Cleaned up logger resources for {self.service_name}")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")