from kafka import KafkaProducer
import json
import time
from datetime import datetime
import uuid
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertTester:
    
    def _init_(self, kafka_broker='localhost:9092'):
        """Set up our Kafka connection"""
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )
        
    def test_error_logs(self):
        """Generate some error logs to test error alerts"""
        logger.info("Sending test error logs...")
        
        # Create an error log
        error_log = {
            "timestamp": datetime.utcnow().isoformat(),
            "service_name": "TestService",
            "node_id": "test-node-1",
            "log_level": "ERROR",
            "message": "Test error message",
            "error_details": {
                "error_code": "TEST_ERR",
                "error_message": "This is a test error"
            }
        }
        
        # Send a few error logs
        for i in range(3):
            error_log["message"] = f"Test error message #{i+1}"
            self.producer.send('service_logs', error_log)
            time.sleep(1)  # Wait a bit between logs

    def test_warning_logs(self):
        """Generate some warning logs to test warning alerts"""
        logger.info("Sending test warning logs...")
        
        warning_log = {
            "timestamp": datetime.utcnow().isoformat(),
            "service_name": "TestService",
            "node_id": "test-node-1",
            "log_level": "WARN",
            "message": "Test warning message",
            "response_time_ms": 1500,
            "threshold_limit_ms": 1000
        }
        
        # Send a couple warning logs
        for i in range(2):
            warning_log["message"] = f"Test warning message #{i+1}"
            self.producer.send('service_logs', warning_log)
            time.sleep(1)

    def test_heartbeat(self):
        """Test heartbeat monitoring"""
        logger.info("Testing heartbeat monitoring...")
        
        # Send a normal heartbeat
        heartbeat = {
            "timestamp": datetime.utcnow().isoformat(),
            "service_name": "TestService",
            "node_id": "test-node-1",
            "message_type": "HEARTBEAT",
            "status": "UP"
        }
        self.producer.send('service_logs', heartbeat)
        

        logger.info("Waiting to test missing heartbeat...")
        time.sleep(20)
        
        # Send another heartbeat
        heartbeat["timestamp"] = datetime.utcnow().isoformat()
        self.producer.send('service_logs', heartbeat)

    def run_tests(self):
        """Run all our test scenarios"""
        try:
            logger.info("Starting basic alert system tests...")
            
            # Run our test cases
            self.test_error_logs()
            time.sleep(2)
            
            self.test_warning_logs()
            time.sleep(2)
            
            self.test_heartbeat()
            
            # Make sure everything got sent
            self.producer.flush()
            logger.info("Finished running tests!")
            
        except Exception as e:
            logger.error(f"Oops! Something went wrong: {e}")
        finally:
            self.producer.close()

if __name__ == "_main_":
    # Create and run our tester
    logger.info("Starting alert system tests...")
    tester = BasicAlertTester()
    tester.run_tests()
