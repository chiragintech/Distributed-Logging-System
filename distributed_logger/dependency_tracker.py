from typing import Dict, List, Set, Optional
import networkx as nx
from datetime import datetime

class DependencyTracker:
    """
    Tracks service dependencies and interactions.
    Uses a directed graph to maintain dependency relationships.
    """
    
    def __init__(self, service_name: str, dependencies: List[str] = None):
        """
        Initialize the dependency tracker.
        
        Args:
            service_name (str): Name of the service
            dependencies (List[str], optional): List of services this service depends on
        """
        self.service_name = service_name
        self.dependency_graph = nx.DiGraph()
        self.dependency_graph.add_node(service_name)
        
        # Add known dependencies
        if dependencies:
            for dep in dependencies:
                self.add_dependency(dep)
                
    
    def add_dependency(self, dependent_service: str):
        """
        Add a new dependency for the service.
        
        Args:
            dependent_service (str): Name of the service being depended on
        """
        self.dependency_graph.add_edge(self.service_name, dependent_service)
    

    
    def get_dependencies(self) -> Dict[str, Set[str]]:
        """
        Get all dependencies for the service.
        
        Returns:
            Dict with direct and indirect dependencies
        """
        return {
            "direct": set(self.dependency_graph.successors(self.service_name)),
            "indirect": set(nx.descendants(self.dependency_graph, self.service_name)) - 
                       set(self.dependency_graph.successors(self.service_name)),
            "dependents": set(self.dependency_graph.predecessors(self.service_name))
        }
    
    def get_dependency_graph(self) -> nx.DiGraph:
        """Get the complete dependency graph"""
        return self.dependency_graph