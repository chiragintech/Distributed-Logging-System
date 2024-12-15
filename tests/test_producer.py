# test_producer.py
from fluent import sender
import time
import random
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_test_logs():
    # Configure Fluentd sender
    fluent_logger = sender.FluentSender(
        'service',
        host='localhost',
        port=24224,
        verbose=True  # Enable verbose logging
    )
    
    # Test different log levels
    log_types = [
        ('INFO', 'Normal operation message'),
        ('WARN', 'High latency detected'),
        ('ERROR', 'Connection failed'),
    ]
    
    try:
        for i in range(5):  # Send 5 sets of logs
            print(f"\nSending batch {i+1} of logs...")
            
            for level, message in log_types:
                log_data = {
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'service_name': 'TestService',
                    'log_level': level,
                    'message': message,
                    'message_type': 'LOG',
                    'trace_id': f'test-trace-{random.randint(1000, 9999)}',
                }
                
                if level == 'WARN':
                    log_data.update({
                        'response_time_ms': random.randint(1000, 2000),
                        'threshold_limit_ms': 1000
                    })
                elif level == 'ERROR':
                    log_data.update({
                        'error_details': {
                            'error_code': 'TEST_ERR',
                            'error_message': 'Test error'
                        }
                    })
                
                # Log the data being sent
                print(f"Sending {level} log: {json.dumps(log_data, indent=2)}")
                
                # Send to Fluentd
                tag = f"service.logs"  # Use correct tag format
                success = fluent_logger.emit(tag, log_data)
                
                if success:
                    print(f"Successfully sent {level} log")
                else:
                    print(f"Failed to send {level} log: {fluent_logger.last_error}")
                
                time.sleep(1)  # Wait between logs
            
            print("\nWaiting 5 seconds before next batch...")
            time.sleep(5)
    
    finally:
        print("Closing Fluentd connection...")
        fluent_logger.close()

if __name__ == "__main__":
    print("Starting test log producer...")
    send_test_logs()