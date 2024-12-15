# services/service_d.py

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

class ServiceD(BaseService):
    def __init__(self):
        super().__init__(
            service_name="ServiceD",
            port=5003,
            dependencies=[]
        )
        self.performance_threshold_ms = 300

    def process_request(self, trace_id: str = None) -> Tuple[bool, Dict[str, Any]]:
        """Process request with enhanced tracing"""
        trace_id = trace_id or str(uuid.uuid4())
        
        with self.logger.start_span("process_request", {
            "request.trace_id": trace_id
        }) as span:
            try:
                start_time = time.time()
                
                # Simulate processing with tracing
                with self.logger.start_span("processing_operation") as proc_span:
                    processing_time = random.uniform(0.1, 0.5)
                    time.sleep(processing_time)
                    
                    processing_time_ms = int(processing_time * 1000)
                    proc_span.set_attribute("processing.time_ms", processing_time_ms)
                    
                    if processing_time_ms > self.performance_threshold_ms:
                        proc_span.set_attribute("performance.threshold_exceeded", True)
                        self.logger.warn(
                            "Processing time exceeded threshold",
                            response_time_ms=processing_time_ms,
                            threshold_limit_ms=self.performance_threshold_ms,
                            trace_id=trace_id
                        )

                    # Simulate success/failure
                    if random.random() < 0.9:
                        self.logger.info(
                            f"Successfully processed request",
                            trace_id=trace_id,
                            processing_time_ms=processing_time_ms
                        )
                        return True, {
                            'trace_id': trace_id,
                            'message': 'Request processed successfully',
                            'processing_time_ms': processing_time_ms
                        }
                    else:
                        error_scenarios = [
                            ("PROC_ERR_1", "Data validation failed"),
                            ("PROC_ERR_2", "Resource unavailable"),
                            ("PROC_ERR_3", "Processing timeout")
                        ]
                        error_code, error_message = random.choice(error_scenarios)
                        
                        proc_span.set_status(Status(StatusCode.ERROR))
                        proc_span.set_attribute("error.code", error_code)
                        
                        self.logger.error(
                            "Processing failed",
                            error_code=error_code,
                            error_message=error_message,
                            trace_id=trace_id
                        )
                        return False, {
                            'trace_id': trace_id,
                            'error': error_code,
                            'error_message': error_message,
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

def create_app():
    service = ServiceD()
    app = create_flask_app(service=service)
    
    @app.route('/process', methods=['GET'])
    def process():
        try:
            trace_id = request.headers.get('X-Trace-ID', str(uuid.uuid4()))
            success, result = service.process_request(trace_id)
            
            response_data = {
                'status': 'success' if success else 'error',
                'service': 'ServiceD',
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
                'service': 'ServiceD',
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e)
            }), 500

    @app.route('/health', methods=['GET'])
    def health():
        idle_time = datetime.now() - service.last_request_time
        return jsonify({
            'service': 'ServiceD',
            'status': 'healthy',
            "idle_time_minutes":idle_time.total_seconds()/60,
            'timestamp': datetime.utcnow().isoformat(),
            'version': '1.0.0',
            'metrics': {
                'performance_threshold_ms': service.performance_threshold_ms
            }
        })

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host='0.0.0.0', port=5003)