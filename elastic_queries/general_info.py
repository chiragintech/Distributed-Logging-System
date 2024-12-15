from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import json
from typing import Dict, List, Any

class LogQueryTool:
    def __init__(self, elasticsearch_host: str = "http://localhost:9200"):
        """Initialize connection to Elasticsearch"""
        self.es = Elasticsearch([elasticsearch_host])
    
    def get_recent_errors(self, minutes: int = 30) -> List[Dict[str, Any]]:
        """Get error logs from the last specified minutes"""
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"log_level": "ERROR"}},
                        {"range": {
                            "timestamp": {
                                "gte": f"now-{minutes}m"
                            }
                        }}
                    ]
                }
            },
            "sort": [{"timestamp": "desc"}]
        }
        
        response = self.es.search(index="service_logs", body=query)
        return [hit["_source"] for hit in response["hits"]["hits"]]
    
    def get_service_status(self) -> Dict[str, str]:
        """Get latest status of all services based on heartbeats"""
        query = {
            "size": 0,
            "aggs": {
                "services": {
                    "terms": {"field": "service_name"},
                    "aggs": {
                        "latest_heartbeat": {
                            "top_hits": {
                                "size": 1,
                                "sort": [{"timestamp": "desc"}]
                            }
                        }
                    }
                }
            }
        }
        
        response = self.es.search(index="heartbeat_logs", body=query)
        
        service_status = {}
        for bucket in response["aggregations"]["services"]["buckets"]:
            service_name = bucket["key"]
            latest = bucket["latest_heartbeat"]["hits"]["hits"][0]["_source"]
            timestamp = datetime.fromisoformat(latest["timestamp"].replace('Z', '+00:00'))
            
            # Consider service down if no heartbeat in last 45 seconds
            if datetime.utcnow() - timestamp > timedelta(seconds=45):
                status = "DOWN"
            else:
                status = latest["status"]
            
            service_status[service_name] = status
            
        return service_status
    
    def get_slow_responses(self, threshold_ms: int = 1000) -> List[Dict[str, Any]]:
        """Get logs where response time exceeded threshold"""
        query = {
            "query": {
                "range": {
                    "response_time_ms": {
                        "gt": threshold_ms
                    }
                }
            },
            "sort": [{"response_time_ms": "desc"}]
        }
        
        response = self.es.search(index="service_logs", body=query)
        return [hit["_source"] for hit in response["hits"]["hits"]]
    
    def get_service_logs(self, service_name: str, hours: int = 1) -> List[Dict[str, Any]]:
        """Get all logs for a specific service in the last specified hours"""
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"service_name": service_name}},
                        {"range": {
                            "timestamp": {
                                "gte": f"now-{hours}h"
                            }
                        }}
                    ]
                }
            },
            "sort": [{"timestamp": "desc"}]
        }
        
        response = self.es.search(index="service_logs", body=query)
        return [hit["_source"] for hit in response["hits"]["hits"]]

def print_results(title: str, results: List[Dict[str, Any]]):
    """Helper function to print results in a readable format"""
    print(f"\n=== {title} ===")
    for result in results:
        print(json.dumps(result, indent=2))
        print("-" * 50)

if __name__ == "__main__":
    # Initialize query tool
    query_tool = LogQueryTool()
    
    # Get and print recent errors
    recent_errors = query_tool.get_recent_errors(minutes=30)
    print_results("Recent Errors (Last 30 minutes)", recent_errors)
    
    # Get and print service status
    status = query_tool.get_service_status()
    print("\n=== Service Status ===")
    for service, state in status.items():
        print(f"{service}: {state}")
    
    # Get and print slow responses
    slow_responses = query_tool.get_slow_responses(threshold_ms=1000)
    print_results("Slow Responses (>1000ms)", slow_responses)
    
    # Get logs for a specific service
    service_logs = query_tool.get_service_logs("ServiceA", hours=1)
    print_results("ServiceA Logs (Last 1 hour)", service_logs)