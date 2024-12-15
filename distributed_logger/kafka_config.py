# distributed_logger/kafka_config.py

"""Configuration for Kafka and Fluentd connections"""

# Fluentd configuration
FLUENTD_HOST = '192.168.244.129'  # Your VM1 IP
FLUENTD_PORT = 24224

# Kafka configuration
KAFKA_BROKER = '192.168.244.130:9092'  # Your VM2 IP
KAFKA_TOPICS = {
    'SERVICE_LOGS': 'service_logs',    # For regular service logs (INFO, WARN, ERROR)
    'TRACE_LOGS': 'trace_logs',        # For service call traces and dependencies
    'HEARTBEAT': 'heartbeat_logs'      # For service heartbeats
}

# Tag prefixes for Fluentd routing
TAG_PREFIXES = {
    'SERVICE': 'service',
    'TRACE': 'trace',
    'HEARTBEAT': 'heartbeat'
}
# Consumer group IDs
CONSUMER_GROUPS = {
    'ALERT': 'alert_consumer',
    'ELASTICSEARCH': 'elasticsearch_consumer'
}

# Monitoring thresholds
THRESHOLDS = {
    'ERROR_RATE': 5,  # errors per minute
    'RESPONSE_TIME': 1000,  # milliseconds
    'HEARTBEAT_TIMEOUT': 30  # seconds
}