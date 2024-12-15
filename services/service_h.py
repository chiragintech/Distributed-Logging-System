# services/service_h.py

from flask import Flask, jsonify, request
from base_service import BaseService, create_flask_app
import requests
import logging
from datetime import datetime
import time
import random
import uuid
from typing import Dict, Any, Tuple
from opentelemetry.trace import Status, StatusCode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ServiceH(BaseService):
    def __init__(self):
        super().__init__(
            service_name="ServiceH",
            port=5007,
            dependencies=["ServiceG"]
        )
        self.response_threshold_ms = 1500

    def process_request(self, trace_id: str = None) -> Tuple[bool, Dict[str, Any]]:
        """Process request with enhanced dependency tracking"""
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

                # Call Service G with tracing
                with self.logger.start_span("call_service_g") as g_span:
                    g_success, g_result = self._call_service_g(trace_id)
                    if not g_success:
                        return False, g_result

                # Main processing
                with self.logger.start_span("main_processing") as proc_span:
                    processing_time_ms = int((time.time() - start_time) * 1000)
                    proc_span.set_attribute("processing.time_ms", processing_time_ms)
                    
                    if processing_time_ms > self.response_threshold_ms:
                        proc_span.set_attribute("performance.threshold_exceeded", True)
                        self.logger.warn(
                            "Operation took longer than expected",
                            response_time_ms=processing_time_ms,
                            threshold_limit_ms=self.response_threshold_ms,
                            trace_id=trace_id
                        )

                    # Simulate failures
                    if random.random() < 0.1:  # Lower failure rate than G
                        error_scenarios = [
                            ("H_ERR_001", "Business logic error"),
                            ("H_ERR_002", "Validation failed"),
                            ("H_ERR_003", "Processing timeout")
                        ]
                        error_code, error_message = random.choice(error_scenarios)
                        
                        proc_span.set_status(Status(StatusCode.ERROR))
                        proc_span.set_attribute("error.code", error_code)
                        
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
                            'processing_time_ms': processing_time_ms
                        }

                # Success case
                self.logger.info(
                    "Successfully completed processing",
                    trace_id=trace_id,
                    processing_time_ms=processing_time_ms
                )
                
                return True, {
                    'trace_id': trace_id,
                    'message': 'Processing completed successfully',
                    'processing_time_ms': processing_time_ms,
                    'service_g_result': g_result
                }

            except Exception as e:
                span.set_status(Status(StatusCode.ERROR))
                span.record_exception(e)
                
                self.logger.error(
                    "Unexpected error in ServiceH",
                    error_code="H_FATAL",
                    error_message=str(e),
                    trace_id=trace_id
                )
                return False, {
                    'trace_id': trace_id,
                    'error': 'H_FATAL',
                    'error_message': str(e)
                }

    def _call_service_g(self, trace_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Call Service G with enhanced error handling and tracing"""
        with self.logger.start_span("call_service_g", {
            "target.service": "ServiceG",
            "request.trace_id": trace_id
        }) as span:
            start_time = time.time()
            
            try:
                # Prepare headers with trace context
                headers = {'X-Trace-ID': trace_id}
                self.logger.inject_context(headers)
                
                response = requests.get(
                    f"{self.get_service_url('ServiceG')}/process",
                    headers=headers,
                    timeout=5
                )
                
                duration_ms = int((time.time() - start_time) * 1000)
                span.set_attribute("request.duration_ms", duration_ms)
                
                try:
                    response_data = response.json()
                except:
                    response_data = {}
                
                if response.ok:
                    self.logger.log_service_call(
                        "ServiceG",
                        success=True,
                        trace_id=trace_id,
                        duration_ms=duration_ms
                    )
                    return True, response_data
                else:
                    error_details = {
                        'error_code': response_data.get('error', 'G_FAIL'),
                        'error_message': response_data.get('error_message', 'ServiceG returned error response')
                    }
                    
                    span.set_status(Status(StatusCode.ERROR))
                    span.set_attribute("error.code", error_details['error_code'])
                    
                    self.logger.log_service_call(
                        "ServiceG",
                        success=False,
                        error_details=error_details,
                        trace_id=trace_id,
                        duration_ms=duration_ms
                    )
                    
                    return False, {
                        'trace_id': trace_id,
                        'error': error_details['error_code'],
                        'error_message': error_details['error_message'],
                        'duration_ms': duration_ms
                    }

            except requests.RequestException as e:
                duration_ms = int((time.time() - start_time) * 1000)
                error_details = {
                    'error_code': 'G_CONN_ERR',
                    'error_message': f"Failed to connect to ServiceG: {str(e)}"
                }
                
                span.set_status(Status(StatusCode.ERROR))
                span.record_exception(e)
                
                self.logger.log_service_call(
                    "ServiceG",
                    success=False,
                    error_details=error_details,
                    trace_id=trace_id,
                    duration_ms=duration_ms
                )
                
                return False, {
                    'trace_id': trace_id,
                    'error': error_details['error_code'],
                    'error_message': error_details['error_message'],
                    'duration_ms': duration_ms
                }

def create_app():
    service = ServiceH()
    app = create_flask_app(service)

    @app.route('/process', methods=['GET'])
    def process():
        try:
            trace_id = request.headers.get('X-Trace-ID', str(uuid.uuid4()))
            success, result = service.process_request(trace_id)
            
            response_data = {
                'status': 'success' if success else 'error',
                'service': 'ServiceH',
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
                'service': 'ServiceH',
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e)
            }), 500

    @app.route('/health', methods=['GET'])
    def health():
        idle_time = datetime.now() - service.last_request_time
        return jsonify({
            'service': 'ServiceH',
            'status': 'healthy',
            "idle_time_minutes":idle_time.total_seconds()/60,
            'timestamp': datetime.utcnow().isoformat(),
            'version': '1.0.0',
            'metrics': {
                'response_threshold_ms': service.response_threshold_ms
            },
            'dependencies': ['ServiceG']
        })

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host='0.0.0.0', port=5007)