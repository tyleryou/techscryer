import logging
import requests
from typing import Any, Literal
from pymongo import MongoClient

# ------------------------------
# Logging Setup
# ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ------------------------------
# Pipeline Classgi
# ------------------------------
class Pipeline:
    def __init__(self, base_url: str):
        self.base_url = base_url
        logger.info(f'Pipeline initialized with base URL: {base_url}')

    # Expecting a JSON object return or None if request fails.
    def extract(
            self,
            endpoint: str,
            client_id: str,
            token: str,
            body_query: str,
            # token_type: str
    ) -> dict[str, Any] | None:
        full_url = self.base_url+endpoint

        headers = {
            'Client-ID': client_id,
            'Authorization': f'Bearer {token}',
        }

        logger.info(f'Sending POST request to {full_url}')

        try:
            r = requests.post(
                url=full_url,
                headers=headers,
                data=body_query
            )
            # Raise if unsuccessful post.
            r.raise_for_status()
            # Check for empty response
            if r.json() is not None:
                logger.info(f'Successfully extracted data from {endpoint}')
                return r.json()
            else:
                logger.error(f'No data received from endpoint: {endpoint}')
                raise Exception(f'No data from endpoint: {endpoint}')

        except requests.exceptions.RequestException as e:
            logger.error(f'Failed on request to endpoint: {endpoint} --- {e}')

# To do
    def load(
            self,
            database_platform: Literal['postgresql', 'mongodb'],
            database_name: str,
            table_name: str,
            host: str,
            port: int,
            user: str,
            password: str,
            data: [str, Any]
    ) -> bool:
        if database_platform == 'postgres':
            # do some work
            return True
        elif database_platform == 'mongodb':
            logger.info(f'Loading data into {database_platform}: {database_name}.{table_name}')
            client = MongoClient(f'mongodb://{user}:{password}@{host}:{port}/')
            db = client[database_name]
            collection = db[table_name]
            try:
                collection.insert_many(data)
                logger.info(f'Successfully uploaded data into {database_platform}: {database_name}.{table_name}')
                return True
            except Exception as e:
                logger.error(f'Failed to insert data into {database_platform}: {database_name}.{table_name} --- {e}')
                raise Exception(f'Failed to insert into MongoDB with error: {e}')
        else:
            raise Exception('Correct database not entered.')