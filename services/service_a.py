# services/service_a.py

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

class ServiceA(BaseService):
    def __init__(self):
        super().__init__(
            service_name="ServiceA",
            port=5000,
            dependencies=["ServiceB", "ServiceC"]
        )
        self.response_threshold_ms = 1000

    def process_request(self, trace_id: str = None) -> Tuple[bool, Dict[str, Any]]:
        """Process request and coordinate with dependent services"""
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

                # Call Service B
                b_success, b_result = self._call_service_b(trace_id)

                # Call Service C
                c_success, c_result = self._call_service_c(trace_id)

                # Check total processing time
                processing_time_ms = (time.time() - start_time) * 1000
                span.set_attribute("processing.time_ms", processing_time_ms)
                
                if processing_time_ms > self.response_threshold_ms:
                    self.logger.warn(
                        "Request processing exceeded threshold",
                        response_time_ms=processing_time_ms,
                        threshold_limit_ms=self.response_threshold_ms,
                        trace_id=trace_id
                    )

                return True, {
                    'trace_id': trace_id,
                    'service_b_result': b_result,
                    'service_c_result': c_result,
                    'processing_time_ms': processing_time_ms
                }

            except Exception as e:
                span.set_status(Status(StatusCode.ERROR))
                span.record_exception(e)
                
                self.logger.error(
                    "Internal processing error",
                    error_code="INTERNAL_ERROR",
                    error_message=str(e),
                    trace_id=trace_id
                )
                return False, {
                    'trace_id': trace_id,
                    'error': 'INTERNAL_ERROR',
                    'message': str(e)
                }

    def _call_service_b(self, trace_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Call Service B with enhanced error handling and tracing"""
        with self.logger.start_span("call_service_b", {
            "target.service": "ServiceB",
            "request.trace_id": trace_id
        }) as span:
            start_time = time.time()
            
            try:
                # Prepare headers with trace context
                headers = {'X-Trace-ID': trace_id}
                self.logger.inject_context(headers)
                
                response = requests.get(
                    f"{self.get_service_url('ServiceB')}/process",
                    headers=headers,
                    timeout=5
                )
                
                duration_ms = (time.time() - start_time) * 1000
                span.set_attribute("request.duration_ms", duration_ms)
                
                try:
                    response_data = response.json()
                except:
                    response_data = {}
                
                if response.ok:
                    self.logger.log_service_call(
                        "ServiceB",
                        success=True,
                        trace_id=trace_id,
                        duration_ms=duration_ms
                    )
                    return True, response_data
                else:
                    error_details = {
                        'error_code': response_data.get('error', 'SERVICE_B_ERROR'),
                        'error_message': response_data.get('message', 'Service B request failed')
                    }
                    
                    span.set_status(Status(StatusCode.ERROR))
                    span.set_attribute("error.code", error_details['error_code'])
                    
                    self.logger.log_service_call(
                        "ServiceB",
                        success=False,
                        error_details=error_details,
                        trace_id=trace_id,
                        duration_ms=duration_ms
                    )
                    
                    return False, {
                        'error_code': error_details['error_code'],
                        'error_message': error_details['error_message'],
                        'status_code': response.status_code,
                        'duration_ms': duration_ms
                    }

            except requests.RequestException as e:
                duration_ms = (time.time() - start_time) * 1000
                error_details = {
                    'error_code': 'SERVICE_B_CONNECTION_ERROR',
                    'error_message': str(e)
                }
                
                span.set_status(Status(StatusCode.ERROR))
                span.record_exception(e)
                
                self.logger.log_service_call(
                    "ServiceB",
                    success=False,
                    error_details=error_details,
                    trace_id=trace_id,
                    duration_ms=duration_ms
                )
                
                return False, {
                    'error_code': error_details['error_code'],
                    'error_message': error_details['error_message'],
                    'duration_ms': duration_ms
                }

    def _call_service_c(self, trace_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Call Service C with enhanced error handling and tracing"""
        # Implementation similar to _call_service_b but for Service C
        with self.logger.start_span("call_service_c", {
            "target.service": "ServiceC",
            "request.trace_id": trace_id
        }) as span:
            start_time = time.time()
            
            try:
                headers = {'X-Trace-ID': trace_id}
                self.logger.inject_context(headers)
                
                response = requests.get(
                    f"{self.get_service_url('ServiceC')}/process",
                    headers=headers,
                    timeout=5
                )
                
                duration_ms = (time.time() - start_time) * 1000
                span.set_attribute("request.duration_ms", duration_ms)
                
                try:
                    response_data = response.json()
                except:
                    response_data = {}
                
                if response.ok:
                    self.logger.log_service_call(
                        "ServiceC",
                        success=True,
                        trace_id=trace_id,
                        duration_ms=duration_ms
                    )
                    return True, response_data
                else:
                    error_details = {
                        'error_code': response_data.get('error', 'SERVICE_C_ERROR'),
                        'error_message': response_data.get('message', 'Service C request failed')
                    }
                    
                    span.set_status(Status(StatusCode.ERROR))
                    span.set_attribute("error.code", error_details['error_code'])
                    
                    self.logger.log_service_call(
                        "ServiceC",
                        success=False,
                        error_details=error_details,
                        trace_id=trace_id,
                        duration_ms=duration_ms
                    )
                    
                    return False, {
                        'error_code': error_details['error_code'],
                        'error_message': error_details['error_message'],
                        'status_code': response.status_code,
                        'duration_ms': duration_ms
                    }

            except requests.RequestException as e:
                duration_ms = (time.time() - start_time) * 1000
                error_details = {
                    'error_code': 'SERVICE_C_CONNECTION_ERROR',
                    'error_message': str(e)
                }
                
                span.set_status(Status(StatusCode.ERROR))
                span.record_exception(e)
                
                self.logger.log_service_call(
                    "ServiceC",
                    success=False,
                    error_details=error_details,
                    trace_id=trace_id,
                    duration_ms=duration_ms
                )
                
                return False, {
                    'error_code': error_details['error_code'],
                    'error_message': error_details['error_message'],
                    'duration_ms': duration_ms
                }

def create_app():
    service = ServiceA()
    app = create_flask_app(service=service)

    @app.route('/process', methods=['GET'])
    def process():
        try:
            trace_id = request.headers.get('X-Trace-ID', str(uuid.uuid4()))
            success, result = service.process_request(trace_id)
            
            response_data = {
                'status': 'success' if success else 'error',
                'service': 'ServiceA',
                'timestamp': datetime.utcnow().isoformat(),
                'trace_id': trace_id,
                **result
            }
            
            return jsonify(response_data), 200 if success else 500
            
        except Exception as e:
            return jsonify({
                'status': 'error',
                'service': 'ServiceA',
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e)
            }), 500

    @app.route('/health', methods=['GET'])
    def health():
        idle_time = datetime.now() - service.last_request_time
        return jsonify({
            'service': 'ServiceA',
            'status': 'healthy',
            'idle_time_minutes':idle_time.total_seconds()/60,
            'timestamp': datetime.utcnow().isoformat(),
            'version': '1.0.0',
            'dependencies': ['ServiceB', 'ServiceC']
        })

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host='0.0.0.0', port=5000)