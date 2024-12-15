from elasticsearch import Elasticsearch
import time

def test_elasticsearch():
    """Test basic Elasticsearch functionality with minimal memory usage"""
    try:
        # Connect to Elasticsearch
        es = Elasticsearch(['http://localhost:9200'])
        
        # Test 1: Check cluster health
        health = es.cluster.health()
        print(f"\nCluster health: {health['status']}")
        
        # Test 2: Create small test document
        test_doc = {
            "timestamp": time.time(),
            "message": "test",
            "level": "INFO"
        }
        
        response = es.index(index="test-logs", document=test_doc)
        print(f"Document indexed: {response['result']}")
        
        # Test 3: Simple search
        es.indices.refresh(index="test-logs")
        result = es.search(index="test-logs", query={"match": {"message": "test"}})
        print(f"Search successful, found {result['hits']['total']['value']} documents")
        
        return True
    except Exception as e:
        print(f"Error testing Elasticsearch: {e}")
        return False

if __name__ == "__main__":
    test_elasticsearch()