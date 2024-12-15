# distributed_logger/fluentd_logger.py

from fluent import sender
from typing import Dict, Any
from datetime import datetime
import json
import logging
from .kafka_config import FLUENTD_HOST, FLUENTD_PORT, TAG_PREFIXES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FluentdLogger:
    """
    Handles communication with Fluentd for log forwarding
    """
    def __init__(self):
        # Initialize separate senders for different log types
        self.senders = {
            'service': sender.FluentSender(
                TAG_PREFIXES['SERVICE'],
                host=FLUENTD_HOST,
                port=FLUENTD_PORT
            ),
            'trace': sender.FluentSender(
                TAG_PREFIXES['TRACE'],
                host=FLUENTD_HOST,
                port=FLUENTD_PORT
            ),
            'heartbeat': sender.FluentSender(
                TAG_PREFIXES['HEARTBEAT'],
                host=FLUENTD_HOST,
                port=FLUENTD_PORT
            )
        }
        
        logger.info("Initialized FluentdLogger with senders for service, trace, and heartbeat logs")

    def emit_service_log(self, service_name: str, log_type: str, data: Dict[str, Any]) -> bool:
        """Emit a service log message"""
        tag = f"{service_name}.{log_type}"
        return self._emit('service', tag, data)

    def emit_trace(self, service_name: str, data: Dict[str, Any]) -> bool:
        """Emit a trace message"""
        tag = f"{service_name}.trace"
        return self._emit('trace', tag, data)

    def emit_heartbeat(self, service_name: str, data: Dict[str, Any]) -> bool:
        """Emit a heartbeat message"""
        tag = f"{service_name}.heartbeat"
        return self._emit('heartbeat', tag, data)

    def _emit(self, sender_type: str, tag: str, data: Dict[str, Any]) -> bool:
        """Internal method to emit messages"""
        if 'timestamp' not in data:
            data['timestamp'] = datetime.utcnow().isoformat()

        try:
            success = self.senders[sender_type].emit(tag, data)
            
            # Debug output
            logger.debug(f"Sending {sender_type} message: {tag}")
            logger.debug(json.dumps(data, indent=2))
            
            if not success:
                logger.error(f"Failed to send to Fluentd: {self.senders[sender_type].last_error}")
                logger.error(f"Failed message: {json.dumps(data, indent=2)}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            logger.error(f"Failed message: {json.dumps(data, indent=2)}")
            return False

    def close(self):
        """Close all Fluentd connections"""
        for sender_type, s in self.senders.items():
            try:
                s.close()
                logger.info(f"Closed {sender_type} sender")
            except Exception as e:
                logger.error(f"Error closing {sender_type} sender: {e}")