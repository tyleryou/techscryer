"""
This script was designed as a universal environmental variable loader to keep Python dev environmental variables 
[os.getenv('variable_name')] consistent with Airflow prod variables [Variable.get('variable_name')].  
"""

import os
from typing import Optional, Any

try:
    from airflow.models import Variable
    RUNNING_IN_AIRFLOW = True
except ImportError:
    from dotenv import load_dotenv
    load_dotenv()
    RUNNING_IN_AIRFLOW = False

def get_env(key: str) -> Any:
    """
    Unified environment variable loader that works both:
    - Locally (using .env/os.getenv)
    - In Airflow (using Variable.get)
    
    Args:
        key: The environment variable name to retrieve
        
    Returns:
        The value of the environment variable
        
    Raises:
        KeyError: If the variable is not found in either source
    """
    # First try direct environment variables (works everywhere)
    
    # Fallback to Airflow variables if running in Airflow
    if RUNNING_IN_AIRFLOW:
        try:
            return Variable.get(key)
        except:
            raise Exception('Invalid Airflow environmental variable key entry.')
    else:
        try:
            return os.getenv(key)  
        except:
            raise Exception('Invalid local environmental variable key entry.')
        
get_env('OTEL_BEARER_TOKEN')
        