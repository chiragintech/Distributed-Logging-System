# tests/verify_pipeline.py

from elasticsearch import Elasticsearch
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PipelineVerifier:
    def __init__(self):
        self.es = Elasticsearch(['http://localhost:9200'])
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )

    def check_elasticsearch_indices(self):
        """Check if required indices exist"""
        logger.info("Checking Elasticsearch indices...")
        indices = self.es.indices.get_alias().keys()
        logger.info(f"Found indices: {list(indices)}")
        
        # Create indices if they don't exist
        required_indices = ['service-logs', 'trace-logs', 'alerts']
        for index in required_indices:
            if index not in indices:
                self.es.indices.create(index=index)
                logger.info(f"Created missing index: {index}")

    def send_test_log(self):
        """Send a test log and verify it reaches Elasticsearch"""
        logger.info("Sending test log to Kafka...")
        
        # Send test log
        test_log = {
            "timestamp": datetime.utcnow().isoformat(),
            "service_name": "TestService",
            "log_level": "ERROR",
            "message": "Test error message",
            "trace_id": "test-trace-id",
            "error_details": {
                "error_code": "TEST_ERR",
                "error_message": "Test error"
            }
        }
        
        self.producer.send('service_logs', test_log)
        self.producer.flush()
        logger.info("Test log sent to Kafka")
        
        # Wait for processing
        time.sleep(5)
        
        # Verify in Elasticsearch
        self.es.indices.refresh(index='service-logs')
        
        result = self.es.search(
            index="service-logs",
            query={
                "match": {
                    "trace_id": "test-trace-id"
                }
            }
        )
        
        hits = result['hits']['total']['value']
        logger.info(f"Found {hits} matching logs in Elasticsearch")
        
        if hits > 0:
            logger.info("Pipeline is working!")
            logger.info("Log content:")
            logger.info(json.dumps(result['hits']['hits'][0]['_source'], indent=2))
        else:
            logger.error("Pipeline verification failed - log not found in Elasticsearch")

    def run_verification(self):
        """Run complete pipeline verification"""
        try:
            self.check_elasticsearch_indices()
            self.send_test_log()
        finally:
            self.producer.close()

if __name__ == "__main__":
    verifier = PipelineVerifier()
    verifier.run_verification()