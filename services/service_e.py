# services/service_e.py

from flask import Flask, jsonify, request
from base_service import BaseService, create_flask_app
import logging
from datetime import datetime
import time
import random
import uuid
from typing import Dict, Any, Tuple
from opentelemetry.trace import Status, StatusCode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ServiceE(BaseService):
    def __init__(self):
        super().__init__(
            service_name="ServiceE",
            port=5004,
            dependencies=[]
        )
        self.response_threshold_ms = 500

    def process_request(self, trace_id: str = None) -> Tuple[bool, Dict[str, Any]]:
        """Process request with high error rate simulation"""
        trace_id = trace_id or str(uuid.uuid4())
        
        with self.logger.start_span("process_request", {
            "request.trace_id": trace_id
        }) as span:
            try:
                start_time = time.time()
                self.logger.info(
                    "Starting request processing",
                    trace_id=trace_id
                )

                # Main processing simulation
                with self.logger.start_span("main_processing") as proc_span:
                    processing_time = random.uniform(0.5, 2.0)
                    time.sleep(processing_time)
                    
                    total_time_ms = int((time.time() - start_time) * 1000)
                    proc_span.set_attribute("processing.time_ms", total_time_ms)
                    
                    if total_time_ms > self.response_threshold_ms:
                        proc_span.set_attribute("performance.threshold_exceeded", True)
                        self.logger.warn(
                            "Operation took longer than expected",
                            response_time_ms=total_time_ms,
                            threshold_limit_ms=self.response_threshold_ms,
                            trace_id=trace_id
                        )

                # Internal operation simulation
                with self.logger.start_span("internal_operation") as int_span:
                    success, op_details = self._simulate_internal_operation(trace_id, int_span)
                    if not success:
                        return False, {
                            'trace_id': trace_id,
                            'error': op_details['error_code'],
                            'error_message': op_details['error_message'],
                            'processing_time_ms': total_time_ms
                        }

                # High error rate simulation
                if random.random() < 0.85:  # 85% error rate
                    error_scenarios = [
                        ("E_ERR_001", "Database connection timeout"),
                        ("E_ERR_002", "Memory allocation failed"),
                        ("E_ERR_003", "Resource exhausted"),
                        ("E_ERR_004", "Cache corruption detected"),
                        ("E_ERR_005", "System overload"),
                        ("E_ERR_006", "Thread deadlock detected")
                    ]
                    error_code, error_message = random.choice(error_scenarios)
                    
                    span.set_status(Status(StatusCode.ERROR))
                    span.set_attribute("error.code", error_code)
                    
                    self.logger.error(
                        "Operation failed",
                        error_code=error_code,
                        error_message=error_message,
                        trace_id=trace_id
                    )
                    return False, {
                        'trace_id': trace_id,
                        'error': error_code,
                        'error_message': error_message,
                        'processing_time_ms': total_time_ms
                    }

                # Success case
                self.logger.info(
                    f"Request processing completed successfully",
                    trace_id=trace_id,
                    processing_time_ms=total_time_ms
                )
                return True, {
                    'trace_id': trace_id,
                    'message': 'Processing completed successfully',
                    'processing_time_ms': total_time_ms
                }

            except Exception as e:
                span.set_status(Status(StatusCode.ERROR))
                span.record_exception(e)
                
                self.logger.error(
                    "Catastrophic failure in request processing",
                    error_code="E_FATAL",
                    error_message=str(e),
                    trace_id=trace_id
                )
                return False, {
                    'trace_id': trace_id,
                    'error': 'E_FATAL',
                    'error_message': str(e)
                }

    def _simulate_internal_operation(self, trace_id: str, parent_span) -> Tuple[bool, Dict[str, Any]]:
        """Simulate internal operation with high failure rate"""
        try:
            start_time = time.time()
            processing_time = int((time.time() - start_time) * 1000)
            
            # Add processing time to parent span
            parent_span.set_attribute("internal.processing_time_ms", processing_time)
            
            if random.random() < 0.7:  # 70% internal failure rate
                error_details = {
                    "error_code": "E_INTERNAL_ERR",
                    "error_message": "Critical internal system error"
                }
                
                self.logger.error(
                    "Internal operation failed",
                    error_code=error_details["error_code"],
                    error_message=error_details["error_message"],
                    trace_id=trace_id
                )
                
                parent_span.set_status(Status(StatusCode.ERROR))
                parent_span.set_attribute("error.code", error_details["error_code"])
                
                return False, error_details
            
            if processing_time > 200:
                self.logger.warn(
                    "Internal operation was slow",
                    response_time_ms=processing_time,
                    threshold_limit_ms=200,
                    trace_id=trace_id
                )
            
            return True, {
                "processing_time_ms": processing_time
            }

        except Exception as e:
            error_details = {
                "error_code": "E_UNEXPECTED",
                "error_message": str(e)
            }
            
            self.logger.error(
                "Unexpected error in internal operation",
                error_code=error_details["error_code"],
                error_message=error_details["error_message"],
                trace_id=trace_id
            )
            
            parent_span.set_status(Status(StatusCode.ERROR))
            parent_span.record_exception(e)
            
            return False, error_details

def create_app():
    service = ServiceE()
    app = create_flask_app(service)
    
    @app.route('/process', methods=['GET'])
    def process():
        try:
            start_time = time.time()
            trace_id = request.headers.get('X-Trace-ID', str(uuid.uuid4()))
            
            # Service availability check
            if random.random() < 0.7:  # 30% chance of service unavailability
                service.logger.error(
                    "Service unavailable",
                    error_code="E_UNAVAILABLE",
                    error_message="Service temporarily unavailable",
                    trace_id=trace_id
                )
                return jsonify({
                    "status": "error",
                    "service": "ServiceE",
                    "error": "E_UNAVAILABLE",
                    "message": "Service temporarily unavailable",
                    "trace_id": trace_id
                }), 503

            success, result = service.process_request(trace_id)
            processing_time = int((time.time() - start_time) * 1000)
            
            if processing_time > 2000:
                service.logger.warn(
                    "HTTP request took too long",
                    response_time_ms=processing_time,
                    threshold_limit_ms=2000,
                    trace_id=trace_id
                )
            
            response_data = {
                'status': 'success' if success else 'error',
                'service': 'ServiceE',
                'timestamp': datetime.utcnow().isoformat(),
                'trace_id': trace_id,
                **result
            }
            
            response = jsonify(response_data)
            if 'processing_time_ms' in result:
                response.headers['X-Processing-Time'] = str(result['processing_time_ms'])
            
            return response, 200 if success else 500
            
        except Exception as e:
            return jsonify({
                'status': 'error',
                'service': 'ServiceE',
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e)
            }), 500

    @app.route('/health', methods=['GET'])
    def health_check():
        idle_time = datetime.now() - service.last_request_time

        return jsonify({
            "service": "ServiceE",
            "status": "healthy",
            "idle_time_minutes":idle_time.total_seconds()/60,
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0"
        })
            

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host='0.0.0.0', port=5004)