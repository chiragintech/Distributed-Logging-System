# test_client.py

import requests
import time
import random
import logging
from datetime import datetime
from typing import Dict, List, Tuple
import uuid
from colorama import init, Fore, Back, Style

# Initialize colorama for colored output
init()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ServiceTester:
    def __init__(self):
        self.base_urls = {
            'ServiceA': 'http://localhost:5000',
            'ServiceB': 'http://localhost:5001',
            'ServiceC': 'http://localhost:5002',
            'ServiceD': 'http://localhost:5003',
            'ServiceE': 'http://localhost:5004',
            'ServiceF': 'http://localhost:5005',
            'ServiceG': 'http://localhost:5006',
            'ServiceH': 'http://localhost:5007'
        }
        
        # Service dependencies
        self.dependencies = {
            'ServiceA': ['ServiceB', 'ServiceC'],
            'ServiceG': ['ServiceF'],
            'ServiceH': ['ServiceG']
        }
        
        # Services that may report as degraded but are still operational
        self.unstable_services = {
            'ServiceE': {
                'expected_issues': ['High error rate', 'System instability', 'Resource exhaustion'],
                'acceptable_status_codes': {200, 503}  # Status codes we consider "normal" for this service
            }
        }

    def check_services(self) -> List[Tuple[str, bool, str, Dict]]:
        """Check all services and return their status with details"""
        results = []
        
        for service_name, url in self.base_urls.items():
            try:
                response = requests.get(
                    f"{url}/health",
                    timeout=3,
                    headers={'Accept': 'application/json'}
                )
                
                health_data = {}
                try:
                    health_data = response.json()
                except:
                    health_data = {'status': 'unknown'}

                # Handle different service states
                if service_name in self.unstable_services:
                    # Special handling for services that may report as degraded
                    if response.status_code in self.unstable_services[service_name]['acceptable_status_codes']:
                        status = health_data.get('status', 'unknown')
                        if status == 'degraded':
                            results.append((
                                service_name, 
                                True, 
                                "Running (Expected Degraded State)", 
                                health_data
                            ))
                        else:
                            results.append((
                                service_name,
                                True,
                                "Running",
                                health_data
                            ))
                    else:
                        results.append((
                            service_name,
                            False,
                            f"Unexpected status: {response.status_code}",
                            health_data
                        ))
                else:
                    # Normal service handling
                    if response.ok:
                        status = health_data.get('status', 'unknown')
                        if status == 'degraded':
                            results.append((
                                service_name,
                                True,
                                f"Running (Degraded) - {', '.join(health_data.get('issues', []))}",
                                health_data
                            ))
                        else:
                            results.append((
                                service_name,
                                True,
                                "Running",
                                health_data
                            ))
                    else:
                        results.append((
                            service_name,
                            False,
                            f"Unhealthy (Status: {response.status_code})",
                            health_data
                        ))
                    
            except requests.ConnectionError:
                results.append((service_name, False, "Not running", {}))
            except requests.Timeout:
                results.append((service_name, False, "Timeout", {}))
            except Exception as e:
                results.append((service_name, False, f"Error: {str(e)}", {}))
        
        return results

    def print_service_status(self, status_results: List[Tuple[str, bool, str, Dict]]):
        """Print service status with detailed information"""
        print("\nService Status:")
        print("-" * 60)
        
        all_running = True
        services_with_issues = []
        
        for service, is_running, details, health_data in status_results:
            # Determine status color and icon
            if not is_running:
                status_color = Fore.RED
                icon = "✗"
                all_running = False
            elif "Degraded" in details:
                status_color = Fore.YELLOW
                icon = "!"
            else:
                status_color = Fore.GREEN
                icon = "✓"
            
            # Print basic status
            print(f"{status_color}{icon} {service:<10}{Style.RESET_ALL}: {details}")
            
            # Print additional health information if available
            if health_data and service in self.unstable_services:
                print(f"   └─ {Fore.CYAN}Expected behavior for this service{Style.RESET_ALL}")
                if 'issues' in health_data:
                    print(f"   └─ Known issues: {', '.join(health_data['issues'])}")
            
            # Track services needing attention
            if not is_running:
                services_with_issues.append(service)
        
        return all_running, services_with_issues

    def test_service(self, service_name: str) -> Dict:
        """Test a single service with tracing"""
        try:
            trace_id = str(uuid.uuid4())
            start_time = time.time()
            
            response = requests.get(
                f"{self.base_urls[service_name]}/process",
                timeout=5,
                headers={
                    'Accept': 'application/json',
                    'X-Trace-ID': trace_id
                }
            )
            
            duration = int((time.time() - start_time) * 1000)
            
            # Special handling for unstable services
            if service_name in self.unstable_services and response.status_code in self.unstable_services[service_name]['acceptable_status_codes']:
                logger.info(
                    f"{Fore.YELLOW}{service_name} Response{Style.RESET_ALL} "
                    f"(trace_id: {trace_id}, duration: {duration}ms): Expected behavior"
                )
                return response.json() if response.ok else None
            
            if response.ok:
                result = response.json()
                logger.info(
                    f"{Fore.GREEN}{service_name} Response{Style.RESET_ALL} "
                    f"(trace_id: {trace_id}, duration: {duration}ms): {result}"
                )
                return result
            else:
                logger.warning(
                    f"{Fore.YELLOW}{service_name} Error{Style.RESET_ALL} "
                    f"(trace_id: {trace_id}, status: {response.status_code}): {response.text}"
                )
                return None
                
        except requests.RequestException as e:
            logger.error(f"{Fore.RED}Error calling {service_name}: {str(e)}{Style.RESET_ALL}")
            return None
        except Exception as e:
            logger.error(f"{Fore.RED}Unexpected error testing {service_name}: {str(e)}{Style.RESET_ALL}")
            return None

    def run_load_test(self, duration_seconds: int = 300):
        """Run a load test with improved monitoring"""
        logger.info(f"Starting {duration_seconds} second load test")
        start_time = time.time()
        stats = {
            'total_requests': 0,
            'errors': 0,
            'service_stats': {name: {'requests': 0, 'errors': 0} for name in self.base_urls.keys()}
        }
        
        try:
            while time.time() - start_time < duration_seconds:
                service_name = random.choice(list(self.base_urls.keys()))
                
                try:
                    stats['total_requests'] += 1
                    stats['service_stats'][service_name]['requests'] += 1
                    
                    result = self.test_service(service_name)
                    if not result:
                        stats['errors'] += 1
                        stats['service_stats'][service_name]['errors'] += 1
                    
                    # Random delay between requests
                    time.sleep(random.uniform(0.5, 2.0))
                    
                except Exception as e:
                    stats['errors'] += 1
                    stats['service_stats'][service_name]['errors'] += 1
                    logger.error(f"Error in load test: {e}")
                    time.sleep(1)
                
                # Print statistics every 10 seconds
                elapsed = time.time() - start_time
                if int(elapsed) % 10 == 0:
                    self._print_stats(stats, elapsed)
                    
        except KeyboardInterrupt:
            logger.info("Load test interrupted by user")
        finally:
            # Print final statistics
            elapsed = time.time() - start_time
            self._print_stats(stats, elapsed, final=True)

    def _print_stats(self, stats: Dict, elapsed: float, final: bool = False):
        """Print detailed test statistics"""
        if final:
            print("\n" + "="*60)
            print(f"{Fore.CYAN}Final Test Statistics:{Style.RESET_ALL}")
        else:
            print("\n" + "-"*60)
            print(f"{Fore.CYAN}Current Statistics:{Style.RESET_ALL}")
        
        total_requests = stats['total_requests']
        total_errors = stats['errors']
        success_rate = ((total_requests - total_errors) / total_requests * 100) if total_requests > 0 else 0
        requests_per_second = total_requests / elapsed if elapsed > 0 else 0
        
        print(f"\nOverall Statistics:")
        print(f"Total Requests: {total_requests}")
        print(f"Total Errors: {total_errors}")
        print(f"Success Rate: {success_rate:.2f}%")
        print(f"Requests/second: {requests_per_second:.2f}")
        print(f"Elapsed Time: {int(elapsed)} seconds")
        
        print(f"\nPer-Service Statistics:")
        for service, service_stats in stats['service_stats'].items():
            service_success_rate = ((service_stats['requests'] - service_stats['errors']) / service_stats['requests'] * 100) if service_stats['requests'] > 0 else 0
            
            # Color code based on success rate
            if service_success_rate >= 90:
                color = Fore.GREEN
            elif service_success_rate >= 70:
                color = Fore.YELLOW
            else:
                color = Fore.RED
                
            print(f"{color}{service:<10}{Style.RESET_ALL}: "
                  f"Requests: {service_stats['requests']}, "
                  f"Errors: {service_stats['errors']}, "
                  f"Success Rate: {service_success_rate:.2f}%")

def main():
    tester = ServiceTester()
    logger.info(f"{Fore.CYAN}Starting service tests...{Style.RESET_ALL}")
    
    # Check services and print status
    print("\nChecking service status...")
    service_status = tester.check_services()
    all_running, problem_services = tester.print_service_status(service_status)
    
    if not all_running:
        print(f"\n{Fore.RED}Error: The following services need attention:{Style.RESET_ALL}")
        for service in problem_services:
            print(f"- {service}")
        print("\nPlease start the required services using:")
        for service in problem_services:
            print(f"python run_service.py {service.lower()[-1]}")
        return
    
    # Start load test
    print(f"\n{Fore.GREEN}All services are operational!{Style.RESET_ALL}")
    input("\nPress Enter to start the load test (Ctrl+C to stop)...")
    
    try:
        tester.run_load_test(150)
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Test interrupted by user{Style.RESET_ALL}")
    
if __name__ == "__main__":
    main()