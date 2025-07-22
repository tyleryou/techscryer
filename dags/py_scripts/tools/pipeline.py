import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import requests
from tools.observe import Logger, Tracer, Meter
from tools import env_loader as env
from typing import Any, Literal
from pymongo import MongoClient
import time  # Added import

class Pipeline:
    def __init__(self, base_url: str, service_name: str):
        self.base_url = base_url
        # self.observer = Observe(service_name)  # Single observer instance
        self.logger = Logger(service_name).get_logger()
        self.tracer = Tracer(service_name).get_tracer()
        self.meter = Meter(service_name)
        self.logger.info(f'Pipeline: {service_name} - initialized with base URL: {base_url}')

    def extract(self, endpoint: str, client_id: str, token: str, body_query: str) -> dict[str, Any] | None:
        with self.tracer.start_as_current_span('extract'):
            start_time = time.time()
            full_url = self.base_url + endpoint
            self.logger.info(f'Sending POST request to: [{full_url}]')

            try:
                r = requests.post(
                    url=full_url,
                    headers={
                        'Client-ID': client_id,
                        'Authorization': f'Bearer {token}',
                    },
                    data=body_query
                )
                
                latency = time.time() - start_time
                self.meter.request_duration.record(latency, {'endpoint': endpoint})
                
                r.raise_for_status()
                
                if r.json() is not None and r.json() != []:
                    self.logger.info(f'Successfully extracted data from: [{endpoint}]')
                    self.meter.requests_total.add(1, {'method': 'POST', 'endpoint': endpoint, 'reason': 'success'})
                    return r.json()
                elif r.json() == []:
                    self.logger.info(f'No new records from endpoint: [{endpoint}]')
                    self.meter.requests_total.add(1, {'method': 'POST', 'endpoint': endpoint, 'reason': 'no_new_records'})
                    return None
                else:
                    self.logger.error(f'No data from endpoint: [{endpoint}]')
                    self.meter.extract_errors.add(1, {'method': 'POST', 'endpoint': endpoint, 'error_type': 'empty_response'})
                    raise Exception(f'No data from endpoint: [{endpoint}]')

            except requests.exceptions.RequestException as e:
                latency = time.time() - start_time
                self.meter.extract_errors.add(1, {
                    'endpoint': endpoint,
                    'error_type': type(e).__name__,
                    'status_code': getattr(e.response, 'status_code', 'unknown')
                })
                # self.observer.record_request(
                #     method='POST', 
                #     status_code=getattr(e.response, 'status_code', 500), 
                #     duration=latency
                # )
                self.logger.error(f'Failed request to [{endpoint}]: {e}')
                return None

    def load(self, database_platform: Literal['postgresql', 'mongodb'],
             database_name: str, data_store: str,
             data: dict[str, Any] | list[dict[str, Any]]) -> bool:
        
        with self.tracer.start_as_current_span('load'):
            start_time = time.time()
            doc_count = len(data) if isinstance(data, list) else 1
            
            try:
                if database_platform == 'mongodb':
                    self.logger.info(f'Connecting to MongoDB')
                    self.meter.increment_connections()
                    
                    client = MongoClient(
                        host=env.get_env('MONGO_HOST'),
                        port=int(env.get_env('MONGO_PORT')),
                        username=env.get_env('MONGO_USER'),
                        password=env.get_env('MONGO_PW'),
                        tls=True,
                        tlsCAFile=env.get_env('TLS_MONGO_CERT_PATH'),
                        authSource='admin'
                    )
                    
                    db = client[database_name]
                    collection = db[data_store]
                    
                    if not data:
                        self.meter.load_errors.add(1, {
                            'database': database_name,
                            'collection': data_store,
                            'error_type': 'empty_data'
                        })
                        self.logger.error(f'Attempted to insert empty data into {database_name}.{data_store}')
                        return False
                    
                    self.meter.documents_processed.add(doc_count, {
                        'database': database_name,
                        'collection': data_store
                    })
                    
                    if isinstance(data, list):
                        collection.insert_many(data)
                    else:
                        collection.insert_one(data)
                        
                    self.logger.info(f'Successfully uploaded {doc_count} documents')
                    return True
                    
                elif database_platform == 'postgresql':
                    # PostgreSQL implementation would go here
                    return True
                    
                else:
                    self.meter.load_errors.add(1, {
                        'reason': 'invalid_platform',
                        'platform': database_platform
                    })
                    self.logger.error('Invalid database platform')
                    return False
                    
            except Exception as e:
                self.meter.load_errors.add(1, {
                    'database': database_name,
                    'collection': data_store,
                    'error_type': type(e).__name__
                })
                self.logger.error(f'Failed to insert data: {e}')
                return False
                
            finally:
                if database_platform == 'mongodb':
                    self.meter.decrement_connections()
                # Use pre-created metric instead of creating new one
                self.meter.load_duration.record(time.time() - start_time, {
                    'database': database_name,
                    'collection': data_store,
                    'platform': database_platform
                })