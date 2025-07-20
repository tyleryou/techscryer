from pymongo import MongoClient
from urllib.parse import quote_plus
from dotenv import load_dotenv
from typing import Literal
import os

load_dotenv()

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
                host=os.getenv('MONGO_HOST'),
                port=int(os.getenv('MONGO_PORT')),
                username=os.getenv('MONGO_USER'),
                password=os.getenv('MONGO_PW'),
                tls=True,
                tlsCAFile=os.getenv('TLS_MONGO_CERT_PATH'),
                authSource=os.getenv('MONGO_AUTH_SOURCE')
            )
            self.db = client[self.database_name]
            self.collection = self.db[self.data_store]
            return self.collection
