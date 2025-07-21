import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from tools import env_loader as env
from pymongo import MongoClient
from urllib.parse import quote_plus
from typing import Literal

class Query:
    def __init__(
        self,
        database_platform: Literal['postgresql', 'mongodb'],
        database_name: str,
        data_store: str
    ):
        self.database_platform = database_platform
        self.database_name = database_name
        self.data_store = data_store
        self.db = None
        self.collection = None
        self.query()

    def query(self):
        if self.database_platform == 'mongodb':
            client = MongoClient(
                host=env.get_env('MONGO_HOST'),
                port=int(env.get_env('MONGO_PORT')),
                username=env.get_env('MONGO_USER'),
                password=env.get_env('MONGO_PW'),
                tls=True,
                tlsCAFile=env.get_env('TLS_MONGO_CERT_PATH'),
                authSource=env.get_env('MONGO_AUTH_SOURCE')
            )
            self.db = client[self.database_name]
            self.collection = self.db[self.data_store]
            return self.collection
