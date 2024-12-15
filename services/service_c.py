# services/service_c.py

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

class ServiceC(BaseService):
    def __init__(self):
        super().__init__(
            service_name="ServiceC",
            port=5002,
            dependencies=[]
        )
        self.performance_threshold_ms = 800
        self.high_load_probability = 0.3
        self.current_load = 0

    def process_request(self, trace_id: str = None) -> Tuple[bool, Dict[str, Any]]:
        """Process request with load simulation and enhanced tracing"""
        trace_id = trace_id or str(uuid.uuid4())
        
        with self.logger.start_span("process_request", {
            "request.trace_id": trace_id,
            "service.load": self.current_load
        }) as span:
            try:
                start_time = time.time()
                
                self.logger.info(
                    "Starting request processing",
                    trace_id=trace_id
                )

                # Update simulated load with tracing
                with self.logger.start_span("update_load") as load_span:
                    self._update_load()
                    load_span.set_attribute("service.current_load", self.current_load)

                # Process request with tracing
                with self.logger.start_span("request_processing") as proc_span:
                    processing_time_ms = self._simulate_processing() * 1000
                    proc_span.set_attribute("processing.time_ms", processing_time_ms)

                    # Performance monitoring
                    if processing_time_ms > self.performance_threshold_ms:
                        proc_span.set_attribute("performance.threshold_exceeded", True)
                        self.logger.warn(
                            "Request processing exceeded threshold",
                            response_time_ms=processing_time_ms,
                            threshold_limit_ms=self.performance_threshold_ms,
                            trace_id=trace_id,
                            current_load=self.current_load
                        )

                    # High load error simulation
                    if self.current_load > 80 and random.random() < 0.2:
                        proc_span.set_status(Status(StatusCode.ERROR))
                        proc_span.set_attribute("error.type", "OVERLOAD_ERROR")
                        
                        self.logger.error(
                            "Service overloaded",
                            error_code="OVERLOAD_ERROR",
                            error_message="Service experiencing high load",
                            trace_id=trace_id,
                            current_load=self.current_load
                        )
                        return False, {
                            'trace_id': trace_id,
                            'error': 'OVERLOAD_ERROR',
                            'error_message': 'Service is overloaded',
                            'current_load': self.current_load,
                            'processing_time_ms': processing_time_ms
                        }

                # Success case
                result_data = self._generate_response_data()
                span.set_attribute("response.load_status", result_data['load_status'])
                
                self.logger.info(
                    "Request processed successfully",
                    trace_id=trace_id,
                    processing_time_ms=processing_time_ms,
                    current_load=self.current_load
                )

                return True, {
                    'trace_id': trace_id,
                    'message': 'Request processed successfully',
                    'data': result_data,
                    'current_load': self.current_load,
                    'processing_time_ms': processing_time_ms
                }

            except Exception as e:
                span.set_status(Status(StatusCode.ERROR))
                span.record_exception(e)
                
                self.logger.error(
                    "Unexpected error during processing",
                    error_code="INTERNAL_ERROR",
                    error_message=str(e),
                    trace_id=trace_id
                )
                return False, {
                    'trace_id': trace_id,
                    'error': 'INTERNAL_ERROR',
                    'error_message': str(e)
                }

    def _simulate_processing(self) -> float:
        """Simulate processing with load-based latency"""
        base_time = random.uniform(0.1, 0.4)
        load_factor = self.current_load / 100.0
        load_delay = load_factor * random.uniform(0.2, 1.0)
        total_time = base_time + load_delay
        time.sleep(total_time)
        return total_time

    def _update_load(self):
        """Update simulated service load"""
        load_change = random.uniform(-10, 15)
        self.current_load = max(0, min(100, self.current_load + load_change))
        
        if self.current_load > 80:
            self.logger.warn(
                "Service under high load",
                current_load=self.current_load,
                threshold_limit_ms=self.performance_threshold_ms
            )

    def _generate_response_data(self) -> Dict[str, Any]:
        """Generate sample response data"""
        return {
            'load_status': 'high' if self.current_load > 80 else 'normal',
            'processed_items': random.randint(1, 50),
            'timestamp': datetime.utcnow().isoformat()
        }

def create_app():
    service = ServiceC()
    app = create_flask_app(service)

    @app.route('/process', methods=['GET'])
    def process():
        try:
            trace_id = request.headers.get('X-Trace-ID', str(uuid.uuid4()))
            success, result = service.process_request(trace_id)
            
            response_data = {
                'status': 'success' if success else 'error',
                'service': 'ServiceC',
                'timestamp': datetime.utcnow().isoformat(),
                'trace_id': trace_id,
                **result
            }
            
            response = jsonify(response_data)
            response.headers['X-Current-Load'] = str(service.current_load)
            if 'processing_time_ms' in result:
                response.headers['X-Processing-Time'] = str(result['processing_time_ms'])
            
            return response, 200 if success else 503
            
        except Exception as e:
            return jsonify({
                'status': 'error',
                'service': 'ServiceC',
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e)
            }), 500

    @app.route('/health', methods=['GET'])
    def health():
        idle_time = datetime.now() - service.last_request_time
        return jsonify({
            'service': 'ServiceC',
            'status': 'healthy',
            'idle_time_minutes':idle_time.total_seconds()/60,
            'timestamp': datetime.utcnow().isoformat(),
            'version': '1.0.0',
            'metrics': {
                'current_load': service.current_load,
                'performance_threshold_ms': service.performance_threshold_ms,
                'high_load_probability': service.high_load_probability
            }
        })

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host='0.0.0.0', port=5002)