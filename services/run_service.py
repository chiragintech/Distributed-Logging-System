import sys
from flask import Flask

def run_service(service_name: str):
    if service_name == "a":
        from service_a import create_app
    elif service_name == "b":
        from service_b import create_app
    elif service_name == "c":
        from service_c import create_app
    elif service_name == "d":
        from service_d import create_app
    elif service_name == "e":
        from service_e import create_app
    elif service_name == "f":
        from service_f import create_app
    elif service_name == "g":
        from service_g import create_app
    elif service_name == "h":
        from service_h import create_app
    else:
        print(f"Unknown service: {service_name}")
        return
        
    app = create_app()
    port = {
        "a": 5000, 
        "b": 5001, 
        "c": 5002, 
        "d": 5003, 
        "e": 5004,
        "f": 5005,
        "g": 5006,
        "h": 5007
    }[service_name]
    app.run(host="localhost", port=port)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_service.py [a|b|c|d|e|f|g|h]")
        sys.exit(1)
    
    run_service(sys.argv[1])