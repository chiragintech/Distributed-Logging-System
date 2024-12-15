# services/service_b.py

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

class ServiceB(BaseService):
    def __init__(self):
        super().__init__(
            service_name="ServiceB",
            port=5001,
            dependencies=[]
        )
        self.db_operation_threshold_ms = 800
        self.failure_rate = 0.2
        self.high_latency_rate = 0.3

    def process_request(self, trace_id: str = None) -> Tuple[bool, Dict[str, Any]]:
        """Process request with simulated database operations and enhanced tracing"""
        trace_id = trace_id or str(uuid.uuid4())
        
        with self.logger.start_span("process_request", {
            "request.trace_id": trace_id,
            "request.type": "database_operation"
        }) as span:
            try:
                start_time = time.time()
                
                self.logger.info(
                    "Starting database operation",
                    trace_id=trace_id
                )

                # Simulate DB operation with tracing
                with self.logger.start_span("database_operation") as db_span:
                    processing_time_ms = self._simulate_db_operation() * 1000
                    db_span.set_attribute("db.operation.time_ms", processing_time_ms)

                    # Check for slow processing
                    if processing_time_ms > self.db_operation_threshold_ms:
                        db_span.set_attribute("performance.threshold_exceeded", True)
                        self.logger.warn(
                            "Database operation taking longer than expected",
                            response_time_ms=processing_time_ms,
                            threshold_limit_ms=self.db_operation_threshold_ms,
                            trace_id=trace_id
                        )

                    # Simulate failures
                    if random.random() < self.failure_rate:
                        error_type = random.choice([
                            ("DB_CONN_ERROR", "Database connection failed"),
                            ("DB_TIMEOUT", "Database operation timed out"),
                            ("DB_CONSTRAINT", "Database constraint violation")
                        ])
                        
                        db_span.set_status(Status(StatusCode.ERROR))
                        db_span.set_attribute("error.code", error_type[0])
                        db_span.set_attribute("error.message", error_type[1])
                        
                        self.logger.error(
                            "Database operation failed",
                            error_code=error_type[0],
                            error_message=error_type[1],
                            trace_id=trace_id
                        )
                        
                        return False, {
                            'trace_id': trace_id,
                            'error': error_type[0],
                            'error_message': error_type[1],
                            'processing_time_ms': processing_time_ms
                        }

                # Success case
                result_data = self._generate_sample_data()
                span.set_attribute("db.records_processed", result_data['record_count'])
                
                self.logger.info(
                    "Database operation completed successfully",
                    trace_id=trace_id,
                    processing_time_ms=processing_time_ms
                )
                
                return True, {
                    'trace_id': trace_id,
                    'message': 'Database operation successful',
                    'data': result_data,
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

    def _simulate_db_operation(self) -> float:
        """Simulate database operation with latency"""
        base_time = random.uniform(0.1, 0.4)
        if random.random() < self.high_latency_rate:
            base_time += random.uniform(0.5, 1.0)
        time.sleep(base_time)
        return base_time

    def _generate_sample_data(self) -> Dict[str, Any]:
        """Generate sample response data"""
        return {
            'record_count': random.randint(1, 100),
            'status': 'processed',
            'timestamp': datetime.utcnow().isoformat()
        }

def create_app():
    service = ServiceB()
    app = create_flask_app(service)

    @app.route('/process', methods=['GET'])
    def process():
        try:
            trace_id = request.headers.get('X-Trace-ID', str(uuid.uuid4()))
            success, result = service.process_request(trace_id)
            
            response_data = {
                'status': 'success' if success else 'error',
                'service': 'ServiceB',
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
                'service': 'ServiceB',
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e)
            }), 500

    @app.route('/health', methods=['GET'])
    def health():
        idle_time = datetime.now() - service.last_request_time
        return jsonify({
            'service': 'ServiceB',
            'status': 'healthy',
            'idle_time_minutes':idle_time.total_seconds()/60,
            'timestamp': datetime.utcnow().isoformat(),
            'version': '1.0.0',
            'metrics': {
                'failure_rate': service.failure_rate,
                'high_latency_rate': service.high_latency_rate,
                'operation_threshold_ms': service.db_operation_threshold_ms
            }
        })

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host='0.0.0.0', port=5001)