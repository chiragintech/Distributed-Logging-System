# services/base_service.py

import os
import sys
from datetime import datetime , timedelta
from typing import Dict, List, Optional
import logging
from distributed_logger import BaseLogger
import threading
import time
from flask import Flask
from requests import request


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseService:
    def __init__(self, service_name: str, port: int, dependencies: List[str] = None,
                 max_idle_minutes:int  = 5):
        """
        Initialize service with BaseLogger integration
        """
        self.service_name = service_name
        self.port = port
        self.dependencies = dependencies or []
        self.idle_time = timedelta(minutes=max_idle_minutes)
        self.last_request_time = datetime.now()
        self.is_shutting_down = False

        
        # Initialize the BaseLogger instead of direct Fluentd connection
        self.logger = BaseLogger(
            service_name=service_name,
            dependencies=dependencies
        )

        self._start_idle_monitor()
        
        logger.info(f"Initialized {service_name} with {max_idle_minutes} minute idle timeout")
    def _start_idle_monitor(self):
        """
        start monitoring idle time
        """
        self.idle_monitor = threading.Thread(
            target=self._monitor_idle_time,
            daemon= True,
            name= f"{self.service_name}-idle-monitor"
        )
        self.idle_monitor.start()

    def _monitor_idle_time(self):
        """
        monitor idle time and initiate shutdown if exceeded
        """
        check_interval = 10 #seconds
        while not self.is_shutting_down:
            time.sleep(check_interval)

            idle_time = datetime.now() - self.last_request_time
            print(f"idle time is{idle_time}")
            if idle_time >= self.idle_time:
                logger.info(f"Shutting service {self.service_name} due to inactivity for {self.idle_time} minutes")
                self.initiate_shutdown()
                break
    def update_last_request_time(self):
        """
        Update the last request timestamp
        """
        self.last_request_time = datetime.now()
    
    def initiate_shutdown(self):
        """
        Gracefull shutdown
        """
        if not self.is_shutting_down:
            self.is_shutting_down = True
            try:
                self.logger._register_service(status = 'DOWN')
                self.logger.cleanup()
                self._request_flask_shutdown()
            except Exception as e:
                logger.error(f"Error during shutdown")
    def _request_flask_shutdown(self):
        """Request Flask server to shutdown"""
        import requests
        try:
            # Send shutdown request to our own endpoint
            requests.get(f"http://localhost:{self.port}/shutdown")
        except Exception as e:
            logger.error(f"Error requesting shutdown: {e}")



    def get_service_url(self, service_name: str) -> str:
        """Get URL for a service"""
        service_ports = {
            'ServiceA': 5000,
            'ServiceB': 5001,
            'ServiceC': 5002,
            'ServiceD': 5003,
            'ServiceE': 5004,
            'ServiceF': 5005,
            'ServiceG': 5006,
            'ServiceH': 5007
        }
        return f"http://localhost:{service_ports[service_name]}"

    def cleanup(self):
        """Cleanup service resources"""
        try:
            self.logger.info(f"Service {self.service_name} shutting down")
            self.logger.cleanup()
        except Exception as e:
            logger.error(f"Error during service cleanup: {e}")
def create_flask_app(service: BaseService) -> Flask:
    """Create Flask app with idle monitoring"""
    app = Flask(service.service_name)
    
    # Add before_request handler to update last request time
    @app.before_request
    def update_request_time():
        service.update_last_request_time()
    
    # Add shutdown endpoint
    @app.route('/shutdown')
    def shutdown():
        logger.info(f"Shutdown request received for {service.service_name}")
        
        func = request.environ.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('Not running with werkzeug server')
            
        func()
        return 'Server shutting down...'
    
    return app