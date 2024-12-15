# consumers/alert_system.py

from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, Set, List
import logging
from colorama import init, Fore, Style
import threading
import time

# Initialize colorama for colored terminal output
init()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertSystem:
    def __init__(self, kafka_broker: str):
        """Initialize the alert system"""
        self.kafka_broker = kafka_broker
        
        # Initialize Kafka consumers
        self.consumers = {
            'service_logs': KafkaConsumer(
                'service_logs',
                bootstrap_servers=[kafka_broker],
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            ),
            'trace_logs': KafkaConsumer(
                'trace_logs',
                bootstrap_servers=[kafka_broker],
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            ),
            'heartbeat_logs': KafkaConsumer(
                'heartbeat_logs',
                bootstrap_servers=[kafka_broker],
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
        }
        
        # Track service dependencies
        self.dependencies: Dict[str, Set[str]] = defaultdict(set)
        
        # Track service status
        self.service_status: Dict[str, Dict] = defaultdict(dict)
        
        # Track last heartbeat times
        self.last_heartbeat = {}
        
        # Track cascading errors
        self.error_chains ={}

        # Track registered services
        self.registered_services = {}
        
        # Start heartbeat monitor
        self._start_heartbeat_monitor()

        # thresholds
        self.heartbeat_warning_threshold = timedelta(seconds = 20)
        self.heartbeat_error_threshold = timedelta(seconds=30)
    

    def _handle_registration_message(self,log_data:dict):
        """
        Handles the registration messages from microservices
        """
        if log_data.get('message_type') != 'REGISTRATION':
            return

        node_id = log_data.get('node_id')
        service_name = log_data.get('service_name')
        status = log_data.get('status','UP') # default it to UP

        if node_id  and service_name:
            if status == 'UP':
                self.registered_services[node_id] = {
                    'service_name':service_name,
                    'status':'UP',
                    'registered_at':datetime.now(),
                    'last_heartbeat':datetime.now()
                }
                self.last_heartbeat[service_name] = datetime.now()
                self._print_alert(
                    "INFO",
                    service_name,
                    f"Service Registered Successully",
                    f"Node_ID:{node_id}",
                )
            elif status == "DOWN":
                if node_id in self.registered_services:
                    service_name = self.registered_services[node_id]['service_name']
                    del self.registered_services[node_id]
                    if service_name in self.last_heartbeat:
                        del self.last_heartbeat[service_name]
                    


    def _print_alert(self, level: str, service: str, message: str, details: str = None):
        """Print formatted alert to terminal"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        color = Fore.YELLOW if level == "WARN" else Fore.RED if level == "ERROR" else Fore.WHITE
        
        print(f"\n{color}[{timestamp}] {level} - {service}{Style.RESET_ALL}")
        print(f"└─ {message}")
        if details:
            print(f"   └─ {details}")

    def _handle_heartbeat(self, heartbeat_data: dict):
        """Handle heartbeat messages"""
        try:
            node_id = heartbeat_data.get('node_id')
            service_name = heartbeat_data.get('service_name')
            
            if node_id and node_id in self.registered_services:
                current_time = datetime.now()
                self.last_heartbeat[service_name] = current_time
                self.registered_services[node_id]['last_heartbeat'] = current_time
                logger.debug(f"Updated heartbeat for {service_name} at {current_time}")
        except Exception as e:
            logger.error(f"Error handling heartbeat: {e}")


    def _monitor_heartbeats(self):
        """Monitor heartbeats only for registered services"""
        logger.info("Starting heartbeat monitor")
        while True:
            try:
                current_time = datetime.now()
                
                for node_id, service_info in self.registered_services.items():
                    service_name = service_info['service_name']
                    
                    if service_name in self.last_heartbeat:
                        time_diff = current_time - self.last_heartbeat[service_name]
                        
                        if time_diff > self.heartbeat_error_threshold:
                            self._print_alert(
                                "ERROR",
                                service_name,
                                "Service may be down - Multiple heartbeats missed",
                                f"Last heartbeat: {self.last_heartbeat[service_name].strftime('%Y-%m-%d %H:%M:%S')}"
                            )
                        elif time_diff > self.heartbeat_warning_threshold:
                            self._print_alert(
                                "WARN",
                                service_name,
                                "Service heartbeat delayed",
                                f"Last heartbeat: {self.last_heartbeat[service_name].strftime('%Y-%m-%d %H:%M:%S')}"
                            )
                        else:
                            logger.debug(f"Service {service_name} heartbeat OK - Last: {time_diff.total_seconds():.1f}s ago")
            
            except Exception as e:
                logger.error(f"Error in heartbeat monitor: {e}")
            
            time.sleep(5)  # Check every 5 seconds

    def _start_heartbeat_monitor(self):
        """Start heartbeat monitoring thread"""
        monitor_thread = threading.Thread(
            target=self._monitor_heartbeats,
            daemon=True
        )
        monitor_thread.start()

    def _update_dependencies(self, trace_data: dict):
        """Update service dependency graph"""
        source = trace_data.get('source_service')
        target = trace_data.get('target_service')
        
        if source and target:
            self.dependencies[source].add(target)

    def _track_cascading_error(self, trace_id: str, service: str, error_details: dict):
        """Track error propagation through services"""
        if trace_id not in self.error_chains:
            self.error_chains[trace_id] = []
        
        self.error_chains[trace_id].append({
            'service': service,
            'error': error_details
        })
        
        # If we have multiple errors in the chain, analyze them
        if len(self.error_chains[trace_id]) > 1:
            self._analyze_error_chain(trace_id)

    def _analyze_error_chain(self, trace_id: str):
        """Analyze error chain to identify root cause"""
        chain = self.error_chains[trace_id]
        
        # Find the deepest service in the dependency graph
        deepest_service = None
        max_depth = -1
        
        for error in chain:
            service = error['service']
            depth = 0
            
            # Calculate service depth in dependency graph
            for other_service, deps in self.dependencies.items():
                if service in deps:
                    depth += 1
            
            if depth > max_depth:
                max_depth = depth
                deepest_service = service
        
        if deepest_service:
            # Get the error details for the root cause
            root_error = next(
                (error['error'] for error in chain if error['service'] == deepest_service),
                None
            )
            
            if root_error:
                affected_services = [error['service'] for error in chain]
                self._print_alert(
                    "ERROR",
                    deepest_service,
                    f"Root cause of cascading failure affecting {', '.join(affected_services)}",
                    f"Error: {root_error.get('error_code', 'Unknown')} - {root_error.get('error_message', 'No message')}"
                )

    def process_messages(self):
        """Process messages from all topics"""
        try:
            logger.info("Starting alert system...")
            
            while True:
                # Process messages from each consumer with non-blocking poll
                for topic, consumer in self.consumers.items():
                    # Poll with a short timeout
                    messages = consumer.poll(timeout_ms=100)
                    
                    for tp, records in messages.items():
                        for record in records:
                            message_data = record.value
                            
                            # Handle messages based on topic
                            if topic == 'heartbeat_logs':
                                self._handle_heartbeat(message_data)
                                
                            elif topic == 'service_logs':
                                if message_data.get('message_type') == "REGISTRATION":
                                    self._handle_registration_message(message_data)
                                elif message_data.get('log_level') in ['ERROR', 'WARN']:
                                    level = message_data['log_level']
                                    service = message_data['service_name']
                                    msg = message_data['message']
                                    
                                    details = None
                                    if 'error_details' in message_data:
                                        details = (f"Error: {message_data['error_details'].get('error_code', 'Unknown')} - "
                                                f"{message_data['error_details'].get('error_message', 'No message')}")
                                        
                                        # trace_id = message_data.get('trace_id')
                                        # if trace_id:
                                        #     self._track_cascading_error(trace_id, service, message_data['error_details'])
                                    
                                    self._print_alert(level, service, msg, details)
                                    
                            elif topic == 'trace_logs':
                                self._update_dependencies(message_data)
                                if message_data.get('status') == 'ERROR':
                                    self._print_alert(
                                        "ERROR",
                                        f"{message_data['source_service']} -> {message_data['target_service']}",
                                        "Service call failed",
                                        f"Duration: {message_data.get('duration_ms', 0)}ms"
                                    )
                
                # Small sleep to prevent CPU overuse
                time.sleep(0.1)
                    
        except KeyboardInterrupt:
            logger.info("Shutting down alert system...")
        finally:
            self.close()
    def close(self):
        """Clean up resources"""
        for consumer in self.consumers.values():
            consumer.close()
        logger.info("Closed all connections")

if __name__ == "__main__":
    # Configuration
    KAFKA_BROKER = "localhost:9092"
    
    # Create and run alert system
    alert_system = AlertSystem(KAFKA_BROKER)
    alert_system.process_messages()