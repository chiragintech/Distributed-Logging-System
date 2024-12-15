from .base_logger import BaseLogger
from .dependency_tracker import DependencyTracker

__version__ = '0.2.0'

class DistributedLogger(BaseLogger):
    """
    Main logger class that combines logging and dependency tracking.
    This is the primary class that services will use.
    """
    def __init__(self, service_name: str, dependencies: list[str] = None):
        super().__init__(service_name, dependencies)
        
        # Log service startup
        self.info(f"Service {service_name} starting up")
        if dependencies:
            self.info(f"Registered dependencies: {', '.join(dependencies)}")

__all__ = ['DistributedLogger']